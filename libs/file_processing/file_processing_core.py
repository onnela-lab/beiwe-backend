from collections import defaultdict
from datetime import datetime
from multiprocessing.pool import ThreadPool
from typing import DefaultDict

from botocore.exceptions import ReadTimeoutError
from cronutils.error_handler import ErrorHandler
from django.core.exceptions import ValidationError

from config.constants import (ACCELEROMETER, ANDROID_LOG_FILE, CALL_LOG, CHUNK_TIMESLICE_QUANTUM,
    CHUNKS_FOLDER, IDENTIFIERS, SURVEY_DATA_FILES, SURVEY_TIMINGS, WIFI)
from config.settings import CONCURRENT_NETWORK_OPS, FILE_PROCESS_PAGE_SIZE
from database.data_access_models import ChunkRegistry, FileToProcess
from database.study_models import Study
from database.survey_models import Survey
from database.system_models import FileProcessLock
from database.user_models import Participant
from libs.file_processing.batched_network_operations import batch_upload
from libs.file_processing.data_fixes import (fix_app_log_file, fix_call_log_csv, fix_identifier_csv,
    fix_survey_timings, fix_wifi_csv)
from libs.file_processing.data_qty_stats import calculate_data_quantity_stats
from libs.file_processing.exceptions import (BadTimecodeError, ChunkFailedToExist,
    HeaderMismatchException, ProcessingOverlapError)
from libs.file_processing.file_for_processing import FileForProcessing
from libs.file_processing.utility_functions_csvs import (clean_java_timecode, construct_csv_string,
    csv_to_list, unix_time_to_string)
from libs.file_processing.utility_functions_simple import (binify_from_timecode, compress,
    convert_unix_to_human_readable_timestamps, ensure_sorted_by_timestamp,
    resolve_survey_id_from_file_name)
from libs.s3 import s3_retrieve

"""########################## Hourly Update Tasks ###########################"""

# This is useful for testing and profiling behavior. Replaces the imported threadpool with this
# dummy class and poof! Single-threaded so the "threaded" network operations have real stack traces!
# class ThreadPool():
#     def map(self, *args, **kwargs):
#         # cut off any threadpool kwargs, which is conveniently easy because map does not use kwargs!
#         return map(*args)
#     def terminate(self): pass
#     def close(self): pass
#     def __init__(self, *args,**kwargs): pass


def process_file_chunks():
    """
    This is the function that is called from the command line.  It runs through all new files
    that have been uploaded and 'chunks' them. Handles logic for skipping bad files, raising
    errors appropriately.
    This is primarily called manually during testing and debugging.
    """
    # Initialize the process and ensure there is no other process running at the same time
    error_handler = ErrorHandler()
    if FileProcessLock.islocked():
        raise ProcessingOverlapError("Data processing overlapped with a previous data indexing run.")
    FileProcessLock.lock()

    try:
        number_bad_files = 0

        # Get the list of participants with open files to process
        participants = Participant.objects.filter(files_to_process__isnull=False).distinct()
        print("processing files for the following users: %s" % ",".join(participants.values_list('patient_id', flat=True)))

        for participant in participants:
            while True:
                previous_number_bad_files = number_bad_files
                starting_length = participant.files_to_process.exclude(deleted=True).count()

                print("%s processing %s, %s files remaining" % (datetime.now(), participant.patient_id, starting_length))

                # Process the desired number of files and calculate the number of unprocessed files
                number_bad_files += do_process_user_file_chunks(
                        page_size=FILE_PROCESS_PAGE_SIZE,
                        error_handler=error_handler,
                        position=number_bad_files,
                        participant=participant,
                )

                # If no files were processed, quit processing
                if (participant.files_to_process.exclude(deleted=True).count() == starting_length
                        and previous_number_bad_files == number_bad_files):
                    # Cases:
                    #   every file broke, might as well fail here, and would cause infinite loop otherwise.
                    #   no new files.
                    break
    finally:
        FileProcessLock.unlock()

    error_handler.raise_errors()
    # raise EverythingWentFine(DATA_PROCESSING_NO_ERROR_STRING)


def do_process_user_file_chunks(
        page_size: int, error_handler: ErrorHandler, position: int, participant: Participant
):
    """
    Run through the files to process, pull their data, put it into s3 bins. Run the file through
    the appropriate logic path based on file type.

    If a file is empty put its ftp object to the empty_files_list, we can't delete objects
    in-place while iterating over the db.

    All files except for the audio recording files are in the form of CSVs, most of those files
    can be separated by "time bin" (separated into one-hour chunks) and concatenated and sorted
    trivially. A few files, call log, identifier file, and wifi log, require some triage
    beforehand.  The debug log cannot be correctly sorted by time for all elements, because it
    was not actually expected to be used by researchers, but is apparently quite useful.

    Any errors are themselves concatenated using the passed in error handler.

    In a single call to this function, page_size files will be processed,at the position specified.
    This is expected to exclude files that have previously errored in file processing.
    (some conflicts can be most easily resolved by just delaying a file until the next processing
    period, and it solves )
    """
    # Declare a defaultdict of a tuple of 2 lists
    all_binified_data = defaultdict(lambda: ([], []))
    ftps_to_remove = set()
    # The ThreadPool enables downloading multiple files simultaneously from the network, and continuing
    # to download files as other files are being processed, making the code as a whole run faster.
    pool = ThreadPool(CONCURRENT_NETWORK_OPS)  # Todo: make pool more global to prevent OS memory hogging
    survey_id_dict = {}

    # A Django query with a slice (e.g. .all()[x:y]) makes a LIMIT query, so it
    # only gets from the database those FTPs that are in the slice.
    # print(participant.as_unpacked_native_python())
    print(len(participant.files_to_process.exclude(deleted=True).all()))
    print(page_size)
    print(position)

    # ordering by path results in files grouped by type and chronological order, which is perfect
    # for efficiency.
    # Fixme: does this order_by break the skipping of items that failed to process? solution: exclude ids?
    files_to_process = participant.files_to_process \
        .exclude(deleted=True)  #.order_by("s3_file_path", "created_on")

    # This pool pulls in data for each FileForProcessing on a background thread and instantiates it.
    # Instantiating a FileForProcessing object queries S3 for the File's data. (network request))
    files_for_processing = pool.map(
        FileForProcessing, files_to_process[position: position + page_size], chunksize=1
    )

    for file_for_processing in files_for_processing:
        with error_handler:
            process_one_file(
                file_for_processing, survey_id_dict, all_binified_data, ftps_to_remove
            )
    pool.close()
    pool.terminate()

    # there are several failure modes and success modes, information for what to do with different
    # files percolates back to here.  Delete various database objects accordingly.
    more_ftps_to_remove, number_bad_files, earliest_time_bin, latest_time_bin = upload_binified_data(
        all_binified_data, error_handler, survey_id_dict
    )
    ftps_to_remove.update(more_ftps_to_remove)

    # Update the data quantity stats, if it actually processed any files
    if len(files_to_process) > 0:
        calculate_data_quantity_stats(participant,
                                      earliest_time_bin_number=earliest_time_bin,
                                      latest_time_bin_number=latest_time_bin)

    # Actually delete the processed FTPs from the database
    FileToProcess.objects.filter(pk__in=ftps_to_remove).delete()
    return number_bad_files


def process_one_file(
        file_for_processing: FileForProcessing, survey_id_dict: dict, all_binified_data: DefaultDict,
        ftps_to_remove: set
):
    """ This function is the inner loop of the chunking process. """

    if file_for_processing.exception:
        file_for_processing.raise_data_processing_error()

    # there are two cases: chunkable data that can be stuck into "time bins" for each hour, and
    # files that do not need to be "binified" and pretty much just go into the ChunkRegistry unmodified.
    if file_for_processing.chunkable:
        newly_binified_data, survey_id_hash = process_csv_data(file_for_processing)

        # survey answers store the survey id in the file name (truly ancient design decision).
        if file_for_processing.data_type in SURVEY_DATA_FILES:
            survey_id_dict[survey_id_hash] = resolve_survey_id_from_file_name(
                file_for_processing.file_to_process.s3_file_path)

        if newly_binified_data:
            append_binified_csvs(
                all_binified_data, newly_binified_data, file_for_processing.file_to_process
            )
        else:  # delete empty files from FilesToProcess
            ftps_to_remove.add(file_for_processing.file_to_process.id)
        return

    else:
        # case: unchunkable data file
        timestamp = clean_java_timecode(
            file_for_processing.file_to_process.s3_file_path.rsplit("/", 1)[-1][:-4]
        )
        # Since we aren't binning the data by hour, just create a ChunkRegistry that
        # points to the already existing S3 file.
        try:
            ChunkRegistry.register_unchunked_data(
                file_for_processing.data_type,
                timestamp,
                file_for_processing.file_to_process.s3_file_path,
                file_for_processing.file_to_process.study.pk,
                file_for_processing.file_to_process.participant.pk,
                file_for_processing.file_contents,
            )
            ftps_to_remove.add(file_for_processing.file_to_process.id)
        except ValidationError as ve:
            if len(ve.messages) != 1:
                # case: the error case (below) is very specific, we only want that singular error.
                raise

            # case: an unchunkable file was re-uploaded, causing a duplicate file path collision
            # we detect this specific case and update the registry with the new file size
            # (hopefully it doesn't actually change)
            if 'Chunk registry with this Chunk path already exists.' in ve.messages:
                ChunkRegistry.update_registered_unchunked_data(
                    file_for_processing.data_type,
                    file_for_processing.file_to_process.s3_file_path,
                    file_for_processing.file_contents,
                )
                ftps_to_remove.add(file_for_processing.file_to_process.id)
            else:
                # any other errors, add
                raise


def upload_binified_data(binified_data, error_handler, survey_id_dict):
    """ Takes in binified csv data and handles uploading/downloading+updating
        older data to/from S3 for each chunk.
        Returns a set of concatenations that have succeeded and can be removed.
        Returns the number of failed FTPS so that we don't retry them.
        Returns the earliest and latest time bins handled
        Raises any errors on the passed in ErrorHandler."""
    failed_ftps = set([])
    ftps_to_retire = set([])
    upload_these = []

    # Track the earliest and latest time bins, to return them at the end of the function
    earliest_time_bin = None
    latest_time_bin = None

    for data_bin, (data_rows_list, ftp_list) in binified_data.items():
        with error_handler:
            try:
                study_object_id, user_id, data_type, time_bin, original_header = data_bin
                # Update earliest and latest time bins
                if earliest_time_bin is None or time_bin < earliest_time_bin:
                    earliest_time_bin = time_bin
                if latest_time_bin is None or time_bin > latest_time_bin:
                    latest_time_bin = time_bin
                # data_rows_list may be a generator; here it is evaluated
                rows = data_rows_list
                updated_header = convert_unix_to_human_readable_timestamps(original_header, rows)
                chunk_path = construct_s3_chunk_path(study_object_id, user_id, data_type, time_bin)

                if ChunkRegistry.objects.filter(chunk_path=chunk_path).exists():
                    chunk = ChunkRegistry.objects.get(chunk_path=chunk_path)
                    try:
                        s3_file_data = s3_retrieve(chunk_path, study_object_id, raw_path=True)
                    except ReadTimeoutError as e:
                        # The following check was correct for boto 2, still need to hit with boto3 test.
                        if "The specified key does not exist." == str(e):
                            # This error can only occur if the processing gets actually interrupted and
                            # data files fail to upload after DB entries are created.
                            # Encountered this condition 11pm feb 7 2016, cause unknown, there was
                            # no python stacktrace.  Best guess is mongo blew up.
                            # If this happened, delete the ChunkRegistry and push this file upload to the next cycle
                            chunk.remove()  # this line of code is ancient and almost definitely wrong.
                            raise ChunkFailedToExist("chunk %s does not actually point to a file, deleting DB entry, should run correctly on next index." % chunk_path)
                        raise  # Raise original error if not 404 s3 error

                    old_header, old_rows = csv_to_list(s3_file_data)

                    if old_header != updated_header:
                        # To handle the case where a file was on an hour boundary and placed in
                        # two separate chunks we need to raise an error in order to retire this file. If this
                        # happens AND ONE of the files DOES NOT have a header mismatch this may (
                        # will?) cause data duplication in the chunked file whenever the file
                        # processing occurs run.
                        raise HeaderMismatchException('%s\nvs.\n%s\nin\n%s' %
                                                      (old_header, updated_header, chunk_path) )
                    old_rows = list(old_rows)
                    old_rows.extend(rows)
                    del rows
                    ensure_sorted_by_timestamp(old_rows)
                    new_contents = construct_csv_string(updated_header, old_rows)
                    del old_rows

                    upload_these.append((chunk, chunk_path, compress(new_contents), study_object_id))
                    del new_contents
                else:
                    ensure_sorted_by_timestamp(rows)
                    new_contents = construct_csv_string(updated_header, rows)
                    if data_type in SURVEY_DATA_FILES:
                        # We need to keep a mapping of files to survey ids, that is handled here.
                        survey_id_hash = study_object_id, user_id, data_type, original_header
                        survey_id = Survey.objects.filter(
                            object_id=survey_id_dict[survey_id_hash]).values_list("pk", flat=True).get()
                    else:
                        survey_id = None

                    # this object will eventually get **kwarg'd into ChunkRegistry.register_chunked_data
                    chunk_params = {
                        "study_id": Study.objects.filter(object_id=study_object_id).values_list("pk", flat=True).get(),
                        "participant_id": Participant.objects.filter(patient_id=user_id).values_list("pk", flat=True).get(),
                        "data_type": data_type,
                        "chunk_path": chunk_path,
                        "time_bin": time_bin,
                        "survey_id": survey_id
                    }

                    upload_these.append((chunk_params, chunk_path, compress(new_contents), study_object_id))
            except Exception as e:
                # Here we catch any exceptions that may have arisen, as well as the ones that we raised
                # ourselves (e.g. HeaderMismatchException). Whichever FTP we were processing when the
                # exception was raised gets added to the set of failed FTPs.
                failed_ftps.update(ftp_list)
                print(e)
                print("FAILED TO UPDATE: study_id:%s, user_id:%s, data_type:%s, time_bin:%s, header:%s "
                      % (study_object_id, user_id, data_type, time_bin, updated_header))
                raise

            else:
                # If no exception was raised, the FTP has completed processing. Add it to the set of
                # retireable (i.e. completed) FTPs.
                ftps_to_retire.update(ftp_list)

    pool = ThreadPool(CONCURRENT_NETWORK_OPS)
    errors = pool.map(batch_upload, upload_these, chunksize=1)
    for err_ret in errors:
        if err_ret['exception']:
            print(err_ret['traceback'])
            raise err_ret['exception']

    pool.close()
    pool.terminate()
    # The things in ftps to retire that are not in failed ftps.
    # len(failed_ftps) will become the number of files to skip in the next iteration.
    return ftps_to_retire.difference(failed_ftps), len(failed_ftps), earliest_time_bin, latest_time_bin


"""################################ S3 Stuff ################################"""


def construct_s3_chunk_path(study_id: bytes, user_id: bytes, data_type: bytes, time_bin: int) -> str:
    """ S3 file paths for chunks are of this form:
        CHUNKED_DATA/study_id/user_id/data_type/time_bin.csv """

    study_id = study_id.decode() if isinstance(study_id, bytes) else study_id
    user_id = user_id.decode() if isinstance(user_id, bytes) else user_id
    data_type = data_type.decode() if isinstance(data_type, bytes) else data_type

    return "%s/%s/%s/%s/%s.csv" % (
        CHUNKS_FOLDER,
        study_id,
        user_id,
        data_type,
        unix_time_to_string(time_bin*CHUNK_TIMESLICE_QUANTUM).decode()
    )


"""############################## Standard CSVs #############################"""

def binify_csv_rows(rows_list: list, study_id: str, user_id: str, data_type: str, header: bytes) -> DefaultDict[tuple, list]:
    """ Assumes a clean csv with element 0 in the rows column as a unix(ish) timestamp.
        Sorts data points into the appropriate bin based on the rounded down hour
        value of the entry's unix(ish) timestamp. (based CHUNK_TIMESLICE_QUANTUM)
        Returns a dict of form {(study_id, user_id, data_type, time_bin, header):rows_lists}. """
    ret = defaultdict(list)
    for row in rows_list:
        # discovered August 7 2017, looks like there was an empty line at the end
        # of a file? row was a [''].
        if row and row[0]:
            # this is the first thing that will hit corrupted timecode values errors (origin of which is unknown).
            try:
                timecode = binify_from_timecode(row[0])
            except BadTimecodeError:
                continue
            ret[(study_id, user_id, data_type, timecode, header)].append(row)
    return ret

def append_binified_csvs(old_binified_rows: DefaultDict[tuple, list],
                         new_binified_rows: DefaultDict[tuple, list],
                         file_for_processing:  FileToProcess):
    """ Appends binified rows to an existing binified row data structure.
        Should be in-place. """
    for data_bin, rows in new_binified_rows.items():
        old_binified_rows[data_bin][0].extend(rows)  # Add data rows
        old_binified_rows[data_bin][1].append(file_for_processing.pk)  # Add ftp


# TODO: stick on FileForProcessing
def process_csv_data(file_for_processing: FileForProcessing):
    """ Constructs a binified dict of a given list of a csv rows,
        catches csv files with known problems and runs the correct logic.
        Returns None If the csv has no data in it. """

    if file_for_processing.file_to_process.participant.os_type == Participant.ANDROID_API:
        # Do fixes for Android
        if file_for_processing.data_type == ANDROID_LOG_FILE:
            file_for_processing.file_contents = fix_app_log_file(
                file_for_processing.file_contents, file_for_processing.file_to_process.s3_file_path
            )

        header, csv_rows_list = csv_to_list(file_for_processing.file_contents)
        if file_for_processing.data_type != ACCELEROMETER:
            # If the data is not accelerometer data, convert the generator to a list.
            # For accelerometer data, the data is massive and so we don't want it all
            # in memory at once.
            csv_rows_list = list(csv_rows_list)

        if file_for_processing.data_type == CALL_LOG:
            header = fix_call_log_csv(header, csv_rows_list)
        if file_for_processing.data_type == WIFI:
            header = fix_wifi_csv(header, csv_rows_list, file_for_processing.file_to_process.s3_file_path)
    else:
        # Do fixes for iOS
        header, csv_rows_list = csv_to_list(file_for_processing.file_contents)

        if file_for_processing.data_type != ACCELEROMETER:
            csv_rows_list = list(csv_rows_list)

    # Memory saving measure: this data is now stored in its entirety in csv_rows_list
    file_for_processing.clear_file_content()

    # Do these fixes for data whether from Android or iOS
    if file_for_processing.data_type == IDENTIFIERS:
        header = fix_identifier_csv(header, csv_rows_list, file_for_processing.file_to_process.s3_file_path)
    if file_for_processing.data_type == SURVEY_TIMINGS:
        header = fix_survey_timings(header, csv_rows_list, file_for_processing.file_to_process.s3_file_path)

    header = b",".join([column_name.strip() for column_name in header.split(b",")])
    if csv_rows_list:
        return (
            # return item 1: the data as a defaultdict
            binify_csv_rows(
                csv_rows_list,
                file_for_processing.file_to_process.study.object_id,
                file_for_processing.file_to_process.participant.patient_id,
                file_for_processing.data_type,
                header
            ),
            # return item 2: the tuple that we use as a key for the defaultdict
            (
                file_for_processing.file_to_process.study.object_id,
                file_for_processing.file_to_process.participant.patient_id,
                file_for_processing.data_type,
                header
            )
        )
    else:
        return None, None

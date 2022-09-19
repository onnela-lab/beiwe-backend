from collections import defaultdict
from multiprocessing.pool import ThreadPool
from typing import DefaultDict

from cronutils.error_handler import ErrorHandler
from django.core.exceptions import ValidationError

from config.settings import CONCURRENT_NETWORK_OPS
from constants.data_stream_constants import (ACCELEROMETER, ANDROID_LOG_FILE, CALL_LOG, IDENTIFIERS,
    SURVEY_DATA_FILES, SURVEY_TIMINGS, WIFI)
from constants.user_constants import ANDROID_API
from database.data_access_models import ChunkRegistry, FileToProcess
from database.user_models import Participant
from libs.file_processing.batched_network_operations import batch_upload
from libs.file_processing.csv_merger import CsvMerger
from libs.file_processing.data_fixes import (fix_app_log_file, fix_call_log_csv, fix_identifier_csv,
    fix_survey_timings, fix_wifi_csv)
from libs.file_processing.data_qty_stats import calculate_data_quantity_stats
from libs.file_processing.exceptions import BadTimecodeError
from libs.file_processing.file_for_processing import FileForProcessing
from libs.file_processing.utility_functions_csvs import clean_java_timecode, csv_to_list
from libs.file_processing.utility_functions_simple import (binify_from_timecode,
    resolve_survey_id_from_file_name)


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


def do_process_user_file_chunks(
        page_size: int, error_handler: ErrorHandler, position: int, participant: Participant
):
    """Run through the files to process, pull their data, put it into s3 bins. Run the file through
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
    # The ThreadPool enables downloading multiple files simultaneously from the network, and continuing
    # to download files as other files are being processed, making the code as a whole run faster.
    # In principle we could make a global pool that is free-memory aware.
    pool = ThreadPool(1)
    survey_id_dict = {}
    
    # A Django query with a slice (e.g. .all()[x:y]) makes a LIMIT query, so it
    # only gets from the database those FTPs that are in the slice.
    # print(participant.as_unpacked_native_python())
    print(len(participant.files_to_process.exclude(deleted=False).all()))
    print(page_size)
    print(position)
    
    # TODO: investigate, comment.  ordering by path results in files grouped by type and
    # chronological order, which is perfect for download efficiency... right? would it break anthing?
    files_to_process = participant.files_to_process \
        .exclude(deleted=False)  #.order_by("s3_file_path", "created_on")
    
    # This pool pulls in data for each FileForProcessing on a background thread and instantiates it.
    # Instantiating a FileForProcessing object queries S3 for the File's data. (network request))
    files_for_processing = pool.map(
        FileForProcessing, files_to_process[position: position + page_size], chunksize=1
    )
    
    # Declare a defaultdict of a tuple of 2 lists
    all_binified_data = defaultdict(lambda: ([], []))
    ftps_to_remove = set()
    
    # using a threadpool and a with statement doesn't ensure that close() is called
    try:
        for file_for_processing in files_for_processing:
            with error_handler:
                process_one_file(
                    file_for_processing, survey_id_dict, all_binified_data, ftps_to_remove
                )
    except BaseException:
        raise
    finally:
        pool.close()
        pool.terminate()
    
    # there are several failure modes and success modes, information for what to do with different
    # files percolates back to here.  Delete various database objects accordingly.
    more_ftps_to_remove, number_bad_files, earliest_time_bin, latest_time_bin = upload_binified_data(
        all_binified_data, error_handler, survey_id_dict, participant
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
        process_chunkable_file(file_for_processing, survey_id_dict, all_binified_data, ftps_to_remove)
    else:
        process_unchunkable_file(file_for_processing, ftps_to_remove)


def process_chunkable_file(
    file_for_processing: FileForProcessing, survey_id_dict: dict, all_binified_data: DefaultDict,
    ftps_to_remove: set
):
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


def process_unchunkable_file(file_for_processing: FileForProcessing, ftps_to_remove: set):
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


def upload_binified_data(binified_data, error_handler, survey_id_dict, participant):
    """ Takes in binified csv data and handles uploading/downloading+updating
        older data to/from S3 for each chunk.
        Returns a set of concatenations that have succeeded and can be removed.
        Returns the number of failed FTPS so that we don't retry them.
        Returns the earliest and latest time bins handled
        Raises any errors on the passed in ErrorHandler."""
    # failed_ftps = set([])
    # ftps_to_retire = set([])
    # upload_these = []
    
    # # Track the earliest and latest time bins, to return them at the end of the function
    # earliest_time_bin = None
    # latest_time_bin = None
    uploads = CsvMerger(binified_data, error_handler, survey_id_dict, participant)
    
    pool = ThreadPool(CONCURRENT_NETWORK_OPS)
    errors = pool.map(batch_upload, uploads.upload_these, chunksize=1)
    for err_ret in errors:
        if err_ret['exception']:
            print(err_ret['traceback'])
            raise err_ret['exception']
    
    pool.close()
    pool.terminate()
    # The things in ftps to retire that are not in failed ftps.
    # len(failed_ftps) will become the number of files to skip in the next iteration.
    return uploads.get_retirees()


"""############################## Standard CSVs #############################"""

calss BinifiedData:
    def __init__(self, )

def binify_csv_rows(rows_list: list, data_type: str, header: bytes) -> DefaultDict[tuple, list]:
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
            ret[(data_type, timecode, header)].append(row)
    print("done binifying")
    return ret

def append_binified_csvs(old_binified_rows: DefaultDict[tuple, list],
                         new_binified_rows: DefaultDict[tuple, list],
                         file_for_processing: FileToProcess):
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
    print(file_for_processing.data_type)
    if file_for_processing.file_to_process.os_type == ANDROID_API:
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
    
    print("fixes applied")
    header = b",".join([column_name.strip() for column_name in header.split(b",")])
    if csv_rows_list:
        return (
            # return item 1: the data as a defaultdict
            binify_csv_rows(csv_rows_list, file_for_processing.data_type, header),
            # return item 2: the tuple that we use as a key for the defaultdict
            (file_for_processing.data_type, header)
        )
    else:
        return None, None

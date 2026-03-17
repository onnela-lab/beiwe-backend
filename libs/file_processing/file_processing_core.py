from __future__ import annotations

from collections import Counter, defaultdict
from collections.abc import Generator
from datetime import timedelta
from time import mktime

from cronutils.error_handler import ErrorHandler
from django.core.exceptions import ValidationError
from django.utils import timezone

from config.settings import FILE_PROCESS_PAGE_SIZE
from constants import common_constants
from constants.data_processing_constants import AllBinifiedData, BinifyDict, DEBUG_FILE_PROCESSING
from constants.data_stream_constants import (ACCELEROMETER, DATA_STREAM_TO_S3_FILE_NAME_STRING,
    DEVICEMOTION, GPS, GYRO, MAGNETOMETER)
from database.common_models import Q
from database.data_access_models import ChunkRegistry, FileToProcess
from database.models import Participant, S3File, Study
from libs.file_processing.csv_merger import CsvMerger, FinalOutputContent, Sha1Hash
from libs.file_processing.data_qty_stats import calculate_data_quantity_stats
from libs.file_processing.file_for_processing import FileForProcessing
from libs.file_processing.utility_functions_simple import (BadTimecodeError, binify_from_timecode,
    clean_java_timecode, resolve_survey_id_from_file_name)
from libs.s3 import s3_upload_no_compression
from libs.sentry import SentryUtils
from libs.utils.dev_utils import Timer
from libs.utils.threadpool_utils import drain_in_reverse, s3_op_threaded_iterate


FileToProcessPK = int


# LIST CANNOT INCLUDE THE WIFI DATA STREAM
DUPLICATE_CLEARABLE_TYPES = (
    Q(s3_file_path__contains=f"/{DATA_STREAM_TO_S3_FILE_NAME_STRING[ACCELEROMETER]}/") | 
    Q(s3_file_path__contains=f"/{DATA_STREAM_TO_S3_FILE_NAME_STRING[DEVICEMOTION]}/") |
    Q(s3_file_path__contains=f"/{DATA_STREAM_TO_S3_FILE_NAME_STRING[GPS]}/") |
    Q(s3_file_path__contains=f"/{DATA_STREAM_TO_S3_FILE_NAME_STRING[GYRO]}/") |
    Q(s3_file_path__contains=f"/{DATA_STREAM_TO_S3_FILE_NAME_STRING[MAGNETOMETER]}/")
)


def log(*args, **kwargs):
    """ A simple wrapper around print to make it easier to change logging later. """
    if DEBUG_FILE_PROCESSING:
        print(*args, **kwargs)


def logd(*args, **kwargs):
    print(*args, **kwargs)


def easy_run(participant: Participant):
    """ Just a handy way to just run data processing in the terminal, use with caution, does not
    test for celery activity. """
    logd(f"processing files for {participant.patient_id}")
    processor = FileProcessingTracker(participant)
    processor.remove_already_purged_s3files()
    # processor.clear_duplicate_ftps()  # not currently used on a live instance
    processor.process_user_file_chunks()


"""########################## Hourly Update Tasks ###########################"""


# The memory leak was NOT caused by using a ThreadPool, but single-threading the network operations
# mean that there are no overlapping network operations, so our _peak_ memory usage is lower.

class FileProcessingTracker():
    
    def __init__(
        self, participant: Participant, page_size: int = FILE_PROCESS_PAGE_SIZE,
    ) -> None:
        self.error_handler: ErrorHandler = SentryUtils.report_data_processing(
            tags={'patient_id': participant.patient_id}
        )
        
        # we need both forms
        self.survey_object_id: str | None = None
        self.survey_pk: int | None = None
        
        self.participant = participant
        self.study: Study = Study.obj_get(pk=participant.study_id)
        self.study_object_id: str = self.study.object_id
        self.patient_id: str = participant.patient_id
        
        # we operate on a page of files at a time, this is the size of the page.
        self.page_size = page_size
        
        # It is possible for devices to record data from unreasonable times, like the unix epoch
        # start. This heuristic is a safety measure to clear out bad data.
        common_constants.LATEST_POSSIBLE_DATA_TIMESTAMP = \
            int(mktime((timezone.now() + timedelta(days=90)).timetuple()))
        
        # a defaultdict of a tuple of 2 lists - this stores the data that is being processed.
        self.all_binified_data: AllBinifiedData = defaultdict(lambda: ([], []))
        
        self.buggy_files = set[FileToProcessPK]()  # only used in logging
    
    #
    ## Outer Loop
    #
    
    def filter_survey_ids(self, ftps: list[FileToProcess]) -> dict[str | None, list[FileToProcess]]:
        d: defaultdict[str | None, list[FileToProcess]] = defaultdict(list)
        for ftp in ftps:
            path = ftp.s3_file_path
            if "surveyTimings" in path:
                d[resolve_survey_id_from_file_name(path)].append(ftp)
            else:
                d[None].append(ftp)
        
        return dict(d)
    
    def get_paginated_files_to_process(self) -> Generator[list[FileToProcess], None, None]:
        # we want to be able to delete database objects at any time so we get the whole contents of
        # the query. The memory overhead is not very high, if it ever is change this to a query for
        # pks and then each pagination is a separate query. (only memory overhead matters.)
        
        # Extremely aggressive data recording sessions can cause the memory leak to use of 1`500` MB
        # by the time it hits 1000 files, so we can at least limit that. if 1000 files per run
        # isn't enough to keep up with uploads, that's the study's problem.
        
        # sorting by s3_file_path clumps together the data streams, which is good for efficiency.
        pks = list(self.participant.files_to_process.exclude(deleted=True).order_by("s3_file_path"))
        
        logd("Number Files To Process:", len(pks))
        
        # yield 100 files at a time
        ret = []
        for pk in pks:
            ret.append(pk)
            if len(ret) == self.page_size:
                yield ret
                ret = []
        yield ret
    
    def remove_already_purged_s3files(self):
        # paths may be removed from S3Files / S3 itself (usually because they are duplicates) but
        # remain in FilesToProcess, this will remove these dead files.
        paths = [
            path + ".zst" for path in   # (match the S3File raw path with .zst)
            self.participant.files_to_process.values_list("s3_file_path", flat=True)
        ]
        # transform these into sets and get the missing paths via set difference
        s3_paths = set(S3File.objects.filter(path__in=paths).values_list("path", flat=True))
        paths = set(paths)
        missing = [m[:-4] for m in (paths - s3_paths)]  # get the paths as they exist in FileToProcess (no .zst)
        self.participant.files_to_process.filter(s3_file_path__in=missing).delete()
    
    def clear_duplicate_ftps(self):
        """
        This function is not currently used or tested outside of manual scenarios
        """
        # An extreme memory use case occurs when there are many FTPs with duplicate data. This
        # mostly happens when reprocessing all participant data who have a lot of duplicate uplaods.
        
        paths = [  # get paths, convert to S3File form with .zst at the end
            p + ".zst" for p in self.participant.files_to_process
                .filter(DUPLICATE_CLEARABLE_TYPES).values_list("s3_file_path", flat=True)
        ]
        
        print(f"Checking {len(paths)} files for duplicates...")
        paths_and_sha1s = dict(S3File.objects.filter(path__in=paths).values_list("path", "sha1"))
        hashes_with_multiple_paths = {k for k, v in Counter(paths_and_sha1s.values()).items() if v > 1}
        
        if not hashes_with_multiple_paths:
            return
        
        print(f"Found {len(hashes_with_multiple_paths)} duplicate sha1s, finding their paths...")
        bad_hashes_to_matching_paths = defaultdict(list)
        for path, sha1 in paths_and_sha1s.items():
            if sha1 in hashes_with_multiple_paths:
                bad_hashes_to_matching_paths[sha1].append(path[:-4])  # get the paths as they exist in FileToProcess (no .zst) in one dict
        
        paths_to_remove = []
        for dup_paths in bad_hashes_to_matching_paths.values():
            dup_paths.pop()  # remove one item from the list of duplicates
            paths_to_remove.extend(dup_paths)
        
        print(f"removing {len(paths_to_remove)} duplicate files from FileToProcess...")
        print(self.participant.files_to_process.filter(s3_file_path__in=paths_to_remove).delete())
    
    def process_user_file_chunks(self):
        """ Call this function to process data for a participant. """
        
        start = timezone.now()  # one participant running too long looks like a down processing server
        
        survey_pk_lookup = dict(self.participant.study.surveys.values_list("object_id", "pk"))
        survey_pk_lookup[None] = None  # for non-survey ftps
        
        for page_of_ftps in self.get_paginated_files_to_process():
            
            if (timezone.now() - start) > timedelta(minutes=29, seconds=30):
                # case is 30 seconds under 30 minutes so that a big multihour hog will at least get
                # rescheduled if it is running immediately after queueing.
                logd("processing time exceeded 30 minutes, exiting early to be polite.")
                return
            
            if not page_of_ftps:
                logd("no more files to process for this participant.")
                continue
            logd(f"will process {len(page_of_ftps)} files.")
            
            # we separate out surveyTimings because they need to be processed only with other survey
            # timings files from the same survey
            for survey_id, ftps in self.filter_survey_ids(page_of_ftps).items():
                
                self.survey_object_id = survey_id
                self.survey_pk = survey_pk_lookup[survey_id]
                self.do_process_user_file_chunks(ftps)
            
            self.buggy_files = set()
    
    def generate_FileForProcessing(self, ftp: FileToProcess) -> FileForProcessing:
        # We pass in the study in order to save a database query for the encryption key
        return FileForProcessing(ftp, self.study)
    
    def do_process_user_file_chunks(self, files_to_process: list[FileToProcess]):
        """ Run through the files to process, pull their data, sort data into time bins. Run the
        file through the appropriate logic path based on file type. """
        
        # Threading this increases speed but increases memory usage.
        with Timer() as t:
            files = list(s3_op_threaded_iterate(self.generate_FileForProcessing, files_to_process))
        log(f"downloaded all files in {t.fseconds} for processing.")
        
        for file_for_processing in drain_in_reverse(files):
            with self.error_handler:
                self.process_one_file(file_for_processing)
        del file_for_processing  # type: ignore - it will be bound
        
        # there are several failure modes and success modes, information for what to do with different
        # files percolates back to here.  Delete various database objects accordingly.
        ftps_to_remove, bad_files, earliest_time_bin, latest_time_bin = self.upload_binified_data()
        self.buggy_files.update(bad_files)
        logd(f"Successfully processed {len(ftps_to_remove)} files ({self.participant.patient_id}), "
              f"there have been a total of {len(self.buggy_files)} failed files.")
        
        # Update the data quantity stats (if it actually processed any files)
        if len(files_to_process) > 0:
            with Timer() as t:
                calculate_data_quantity_stats(self.participant, earliest_time_bin, latest_time_bin)
            log(f"FileProcessingCore: calculate_data_quantity_stats took {t.fseconds} seconds")
        
        # Actually delete the processed FTPs from the database now that we are done.
        FileToProcess.objects.filter(pk__in=ftps_to_remove).delete()
    
    def process_one_file(self, file_for_processing: FileForProcessing):
        """ Dispatches a file to the correct processing logic. """
        if file_for_processing.exception:
            file_for_processing.raise_data_processing_error()
        
        # there are two cases: chunkable data that can be stuck into "time bins" for each hour, and
        # files that do not need to be "binified" and pretty much just go into the ChunkRegistry unmodified.
        if file_for_processing.chunkable:
            self.process_chunkable_file(file_for_processing)
        else:
            self.process_unchunkable_file(file_for_processing)
    
    def upload_binified_data(self) -> tuple[set[FileToProcessPK], set[FileToProcessPK], int|None, int|None]:
        """ Takes in binified csv data and handles uploading/downloading+updating
            older data to/from S3 for each chunk.
            
            Returns a set of FTPs that have succeeded and can be removed.
            Returns a list of FTPs that failed.
            Returns the earliest and latest time bins handled. """
        # Track the earliest and latest time bins, to return them at the end of the function
        merged_data = CsvMerger(
            self.all_binified_data, self.error_handler, self.participant, self.survey_object_id, self.survey_pk
        )
        # a failed upload will require the user gets rerun entirely.
        len_merged_data = len(merged_data.upload_these)
        with Timer() as t:
            self.do_uploads(merged_data)
        log(f"FileProcessingCore: do_uploads took {t.fseconds} seconds for {len_merged_data} files")
        return merged_data.get_retirees()
    
    def do_uploads(self, merged_data: CsvMerger):
        # upload handler - used to be multithreaded, not doing that anymore for memory reasons.
        
        while True:
            try:
                # if the upload fails we simply error out and try again later in a separate run.
                # The type definition of items in merged_data must match the do_upload function.
                self.do_upload(*merged_data.upload_these.pop(-1))
            except IndexError:
                break
    
    def do_upload(
        self,
        chunk_kwargs: dict,
        chunk_path: str,
        compressed_contents: FinalOutputContent,
        sha1_hash: Sha1Hash,
        size_uncompressed: int,
        create_new_chunk: bool,
    ):
        """ Even if the upload succeeds and then something goes wrong with the database update,
        that's fine.  If there is an error it is raised and the FTP is not deleted.  The next time
        file processing runs it will duplicate work, but the code deduplicates output lines, so data
        remains intact. We briefly have a period where data size and hashes are off.  Tolerable. """
        
        # self.study saves a db query for the encryption key
        s3_upload_no_compression(
            chunk_path,
            compressed_contents,
            self.study,
            size_uncompressed,
            sha1_hash,
            raw_path=True,
        )
        
        if create_new_chunk:  # validates, creates
            ChunkRegistry.register_chunked_data(**chunk_kwargs)
        else:  # update info about an existing ChunkRegistry
            ChunkRegistry.objects.filter(chunk_path=chunk_path).update(
                last_updated=timezone.now(), **chunk_kwargs
            )
    
    #
    ## Chunkable File Processing
    #
    
    def process_chunkable_file(self, file_for_processing: FileForProcessing):
        """ logic for downloading, fixing, merging, but not uploading data from one file. """
        newly_binified_data = self.process_csv_data(file_for_processing)
        
        if newly_binified_data:
            self.append_binified_csvs(newly_binified_data, file_for_processing.file_to_process)
        else:
            file_for_processing.delete_ftp()
    
    def append_binified_csvs(
        self, new_binified_rows: BinifyDict, file_for_processing: FileToProcess
    ):
        """ Appends new binified rows to existing binified row data structure, in-place. """
        # data_bin: BinifyKey = study_object_id, patient_id, data_type, timecode int, header bytes
        for data_bin, rows in new_binified_rows.items():
            self.all_binified_data[data_bin][0].extend(rows)  # Add data rows
            self.all_binified_data[data_bin][1].append(file_for_processing.pk)  # Add ftp
        return
    
    def process_csv_data(self, file_for_processing: FileForProcessing) -> BinifyDict | None:
        """ Constructs a binified dict of a given list of a csv rows, catches csv files with known
            problems and runs the correct logic. Returns None If the csv has no data in it. """
        # long running function. decomposes the file into a list of rows and a header, applies data
        # stream fixes (barf).
        file_for_processing.prepare_data()
        # get the header and rows from the file, tell it to clear references to the file contents
        csv_rows_list = file_for_processing.file_lines
        header = file_for_processing.header
        
        # shove csv rows into their respective time bins. upon returning from this function there
        # should only be the binified data representation in memory, return None otherwise.
        try:
            if csv_rows_list and header:
                return self.binify_csv_rows(csv_rows_list, file_for_processing.data_type, header)
        finally:
            file_for_processing.clear_file_lines()  # source file contents can now be GC'd
    
    def binify_csv_rows(self, rows_list: list[list[bytes]], data_type: str, header: bytes) -> BinifyDict:
        """ Assumes a clean csv with element 0 in the row's column as a unix(ish) timestamp.
            Sorts data points into the appropriate bin based on the rounded down hour
            value of the entry's unix(ish) timestamp. (based CHUNK_TIMESLICE_QUANTUM)
            Returns a dict of form {(study_id, patient_id, data_type, time_bin, header):rows_lists}. """
        
        ret: BinifyDict = defaultdict(list)
        for row in rows_list:
            # August 7 2017, looks like there was an empty line at the end of a file? row was a ['']
            if row and row[0]:
                # this is the first thing that will hit corrupted timecode values errors (origin of which is unknown).
                try:
                    timecode = binify_from_timecode(row[0])
                except BadTimecodeError:
                    continue
                ret[(self.study_object_id, self.patient_id, data_type, timecode, header)].append(row)
        
        return ret
    
    #
    ## Unchunkable File Processing
    #
    
    def process_unchunkable_file(self, file_for_processing: FileForProcessing):
        """ Processes a file that is not chunkable. Registers it in the ChunkRegistry directly."""
        try:
            # if the timecode is bad, we scrap this file. We just don't care.
            timestamp = clean_java_timecode(
                file_for_processing.file_to_process.s3_file_path.rsplit("/", 1)[-1][:-4]
            )
        except BadTimecodeError:
            file_for_processing.delete_ftp()
            return
        
        file_for_processing.decompress_file_contents()
        file_contents = file_for_processing.file_contents
        file_for_processing.clear_file_content()  # source file contents can now be GC'd
        
        # Since we aren't binning the data by hour, just create a ChunkRegistry that
        # points to the already existing S3 file.
        try:
            ChunkRegistry.register_unchunked_data(
                file_for_processing.data_type,
                timestamp,
                file_for_processing.file_to_process.s3_file_path,
                file_for_processing.study.pk,
                file_for_processing.file_to_process.participant.pk,
                file_contents,
            )
            file_for_processing.delete_ftp()
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
                    file_contents,
                )
                file_for_processing.delete_ftp()
            else:
                raise  # any other errors, add
        
        return

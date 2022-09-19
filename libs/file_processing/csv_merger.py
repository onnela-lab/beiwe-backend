from multiprocessing.pool import ThreadPool
from typing import Dict, List, Set, Tuple

from botocore.exceptions import ReadTimeoutError
from cronutils import ErrorHandler
from config.settings import CONCURRENT_NETWORK_OPS

from constants.common_constants import RUNNING_TEST_OR_IN_A_SHELL
from constants.data_processing_constants import (CHUNK_EXISTS_CASE, CHUNK_TIMESLICE_QUANTUM,
    CHUNKS_FOLDER, REFERENCE_CHUNKREGISTRY_HEADERS)
from constants.data_stream_constants import SURVEY_DATA_FILES
from database.data_access_models import ChunkRegistry
from database.survey_models import Survey
from database.system_models import GenericEvent
from database.user_models import Participant
from libs.file_processing.exceptions import ChunkFailedToExist
from libs.file_processing.utility_functions_csvs import (construct_csv_string, csv_to_list,
    unix_time_to_string)
from libs.file_processing.utility_functions_simple import (compress,
    convert_unix_to_human_readable_timestamps, ensure_sorted_by_timestamp)
from libs.s3 import s3_retrieve


class CsvMerger:
    """ This class is consumes binified data and  """
    
    def __init__(
        self, binified_data: Dict, error_handler: ErrorHandler, survey_id_dict: Dict,
        participant: Participant
    ):
        self.participant = participant
        self.study_object_id = self.participant.study.object_id
        
        self.failed_ftps = set()
        self.ftps_to_retire = set()
        
        self.upload_these: List[Tuple[ChunkRegistry, str, bytes, str]] = []
        # chunk, something?, file contents, study object id
        
        # Track the earliest and latest time bins, to return them at the end of the function
        self.earliest_time_bin: int = None
        self.latest_time_bin: int = None
        
        self.binified_data = binified_data
        self.error_handler = error_handler
        self.survey_id_dict = survey_id_dict
        self.iterate()
        # self.threaded_iterate()  # in development
    
    def get_retirees(self) -> Tuple[Set[int], int, int, int]:
        """ returns the ftp pks that have succeeded, the number of ftps that have failed, 
        and the earliest and the latest time bins """
        return self.ftps_to_retire.difference(self. failed_ftps), \
            len(self.failed_ftps), self.earliest_time_bin, self.latest_time_bin
    
    def iterate(self):
        # this function is the core loop. we iterate over all binified data and merge data into new
        # chunks, then handle ChunkRegistry parameter setup for the next stage of processing.
        ftp_list: List[int]
        for data_bin, (data_rows_list, ftp_list) in self.binified_data.items():
            with self.error_handler:
                self.inner_iterate(data_bin, data_rows_list, ftp_list)
    
    def threaded_iterate(self):
        binified_items = [
            (self, data_bin, data_rows_list, ftp_list)
            for data_bin, (data_rows_list, ftp_list) in self.binified_data.items()
        ]
        # using a threadpool and a with statement doesn't ensure that close() is called
        # pool = ThreadPool(CONCURRENT_NETWORK_OPS)
        pool = ThreadPool(1)
        try:
            pool.imap_unordered(self.threaded_inner_iterate, binified_items).__iter__()
        except BaseException:
            raise
        finally:
            pool.close()
            pool.terminate
    
    def threaded_inner_iterate(self, data_bin, data_rows_list, ftp_list: List[int]):
        with self.error_handler:
            data_bin
            return self.inner_iterate(data_bin, data_rows_list, ftp_list)
    
    def get_basics(self, data_bin, data_rows_list):
        data_stream, time_bin, original_header = data_bin
        # Update earliest and latest time bins
        if self.earliest_time_bin is None or time_bin < self.earliest_time_bin:
            self.earliest_time_bin = time_bin
        if self.latest_time_bin is None or time_bin > self.latest_time_bin:
            self.latest_time_bin = time_bin
        
        # data_rows_list may be a generator; here it is evaluated
        updated_header = convert_unix_to_human_readable_timestamps(
            original_header, data_rows_list
        )
        chunk_path = construct_s3_chunk_path(
            self.study_object_id, self.participant.patient_id, data_stream, time_bin
        )
        return data_stream, chunk_path, updated_header
    
    
    def inner_iterate(self, data_bin, data_rows_list, ftp_list: List[int]):
        # time_bin: int
        # original_header: bytes
        
        try:
            data_stream, chunk_path, updated_header = self.get_basics(self, data_bin, data_rows_list)
            
            # two core cases
            if ChunkRegistry.objects.filter(chunk_path=chunk_path).exists():
                self.chunk_exists_case(
                    chunk_path, self.study_object_id, updated_header, data_rows_list, data_stream
                )
            else:
                self.chunk_not_exists_case(
                    chunk_path, self.study_object_id, updated_header, self.participant.patient_id,
                    data_stream, original_header, time_bin, data_rows_list
                )
        
        except Exception as e:
            # Here we catch any exceptions that may have arisen, as well as the ones that we raised
            # ourselves (e.g. HeaderMismatchException). Whichever FTP we were processing when the
            # exception was raised gets added to the set of failed FTPs.
            self.failed_ftps.update(ftp_list)
            print(e)
            try:
                print(
                    f"FAILED TO UPDATE: study_id:{study_object_id}, patient_id:{patient_id}, "
                    f"data_stream:{data_stream}, time_bin:{time_bin}, header:{updated_header}"
                )
            except UnboundLocalError:
                # print something different if a variable is not defined yet
                for k in ("study_object_id", "patient_id", "data_stream", "time_bin", "updated_header"):
                    if k not in locals():
                        print(f"variable {k} was not defined")
            raise
        
        else:
            # If no exception was raised, the FTP has completed processing. Add it to the set of
            # retireable (i.e. completed) FTPs.
            self.ftps_to_retire.update(ftp_list)
    
    def chunk_not_exists_case(
        self, chunk_path: str, study_object_id: str, updated_header: str, patient_id: str,
        data_stream: str, original_header: bytes, time_bin: int, rows: List[bytes]
    ):
        ensure_sorted_by_timestamp(rows)
        final_header = self.validate_one_header(updated_header, data_stream)
        new_contents = construct_csv_string(final_header, rows)
        if data_stream in SURVEY_DATA_FILES:
            # We need to keep a mapping of files to survey ids, that is handled here.
            survey_id_hash = study_object_id, patient_id, data_stream, original_header
            survey_id = Survey.objects.filter(
                object_id=self.survey_id_dict[survey_id_hash]
            ).values_list("pk", flat=True).get()
        else:
            survey_id = None
        
        # this object will eventually get **kwarg'd into ChunkRegistry.register_chunked_data
        chunk_params = {
            "study_id": self.participant.study_id,
            "participant_id": self.participant.id,
            "data_type": data_stream,
            "chunk_path": chunk_path,
            "time_bin": time_bin,
            "survey_id": survey_id
        }
        
        self.upload_these.append(
            (chunk_params, chunk_path, compress(new_contents), study_object_id)
        )
    
    def chunk_exists_bit(self, chunk_path: str, study_object_id: str):
        try:
            return s3_retrieve(chunk_path, study_object_id, raw_path=True)
        except ReadTimeoutError as e:
            # The following check was correct for boto 2, still need to hit with boto3 test.
            if "The specified key does not exist." == str(e):
                # This error can only occur if the processing gets actually interrupted and
                # data files fail to upload after DB entries are created.
                # Encountered this condition 11pm feb 7 2016, cause unknown, there was
                # no python stacktrace.  Best guess is mongo blew up.
                # If this happened, delete the ChunkRegistry and push this file upload to the next cycle
                ChunkRegistry.objects.filter(chunk_path=chunk_path).delete()
                raise ChunkFailedToExist(
                    "chunk %s does not actually point to a file, deleting DB entry, should run correctly on next index."
                    % chunk_path
                )
            raise  # Raise original error
    
    def chunk_exists_case(
        self, chunk_path: str, study_object_id: str, updated_header: str, rows: List[bytes],
        data_stream: str
    ):
        print("downloading intermediate file", chunk_path)
        s3_file_data = self.chunk_exists_bit(chunk_path, study_object_id)
        print(f"its size was {len(s3_file_data)}")
        old_header, old_rows = csv_to_list(s3_file_data)
        final_header = self.validate_two_headers(old_header, updated_header, data_stream)
        
        old_rows = list(old_rows)
        old_rows.extend(rows)
        ensure_sorted_by_timestamp(old_rows)
        new_contents = construct_csv_string(final_header, old_rows)
        
        self.upload_these.append(
            (CHUNK_EXISTS_CASE, chunk_path, compress(new_contents), study_object_id, )
        )
    
    def validate_one_header(self, header: bytes, data_stream: str) -> bytes:
        real_header: bytes = REFERENCE_CHUNKREGISTRY_HEADERS[data_stream][self.participant.os_type]
        if header == real_header:
            return real_header
        
        message = f'header was \n"{str(header)}"\n expected\n"{str(real_header)}"'
        GenericEvent.easy_create("bad_header_2", message)
        return real_header
    
    def validate_two_headers(self, header_a: bytes, header_b: bytes, data_stream: str) -> bytes:
        # if headers are the same run the single header logic
        if header_a == header_b:
            return self.validate_one_header(header_a, data_stream)
        
        real_header: bytes = REFERENCE_CHUNKREGISTRY_HEADERS[data_stream][self.participant.os_type]
        
        # compare to reference
        # NOTE: this solves for the case where a participant changed their device os.
        if header_a == real_header or header_b == real_header:
            return real_header
        
        message = f'headers were \n"{str(header_a)}"\n and \n"{str(header_b)}"\n expected\n"{str(real_header)}"'
        GenericEvent.easy_create("bad_header_1", message)
        return real_header
    
    def report_error(self, e: Exception):
        # okay we do have to inspect the stack trace a bit more with this but that's ok.
        # for now I want this reported to sentry as an error.
        if RUNNING_TEST_OR_IN_A_SHELL:
            print()
            print(type(e), str(e))
            print()
        else:
            with self.error_handler:
                raise e


# unused, result of brainstorming how to validate a header as viable.
# def try_validate_header(header: bytes, reference_comma_count: int):
#     """ In order to resolve potential header mismatches and preserve valid lines of data """
#     # all headers start with a timestamp
#     # we force "timestamp," as the first element of the header on wifi log, identifiers, and android log
#     looks_ok = header.startswith(b"timestamp,")
#     count = header.count(b",")
#     comma_count_okay = count == reference_comma_count

#     if count == 0:
#         # if it has no commas it is horribly broken
#         return False, False, False

#     possible_timecode, _ = header.split(b",", 1)
#     try:
#         int(possible_timecode)
#         int_first_element = True
#     except ValueError:
#         int_first_element = False
#     return comma_count_okay, looks_ok, int_first_element


def construct_s3_chunk_path(
    study_id: bytes, patient_id: bytes, data_stream: bytes, time_bin: int
) -> str:
    """ S3 file paths for chunks are of this form:
        CHUNKED_DATA/study_id/patient_id/data_stream/time_bin.csv """
    
    study_id = study_id.decode() if isinstance(study_id, bytes) else study_id
    patient_id = patient_id.decode() if isinstance(patient_id, bytes) else patient_id
    data_stream = data_stream.decode() if isinstance(data_stream, bytes) else data_stream
    
    return "%s/%s/%s/%s/%s.csv" % (
        CHUNKS_FOLDER, study_id, patient_id, data_stream,
        unix_time_to_string(time_bin * CHUNK_TIMESLICE_QUANTUM).decode()
    )

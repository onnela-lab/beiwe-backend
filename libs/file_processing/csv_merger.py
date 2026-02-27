from __future__ import annotations

import hashlib

from botocore.exceptions import ReadTimeoutError
from cronutils import ErrorHandler

from constants.common_constants import CHUNKS_FOLDER, RUNNING_TEST_OR_FROM_A_SHELL
from constants.data_processing_constants import (SURVEY_TIMINGS, AllBinifiedData, BinifyKey,
    CHUNK_TIMESLICE_QUANTUM, DEBUG_FILE_PROCESSING, REFERENCE_CHUNKREGISTRY_HEADERS)
from constants.data_stream_constants import SURVEY_DATA_FILES
from database.data_access_models import ChunkRegistry
from database.system_models import GenericEvent
from database.user_models_participant import Participant
from libs.file_processing.utility_functions_csvs import (construct_csv_as_bytes,
    existing_data_csv_splitter, unix_time_to_string)
from libs.file_processing.utility_functions_simple import (
    convert_unix_to_human_readable_timestamps, ensure_sorted_by_timestamp)
from libs.s3 import s3_retrieve
from libs.utils.compression import compress
from libs.utils.dev_utils import Timer
from libs.utils.security_utils import chunk_hash


FileToProcessPK = int
UncompressedSize = int
FinalOutputContent = bytes
ChunkPath = str
ByteCount = int
Sha1Hash = bytes
Uploadable = tuple[dict, ChunkPath, FinalOutputContent, Sha1Hash, ByteCount, bool]
class ChunkFailedToExist(Exception): pass


def log(*args, **kwargs):
    """ A simple wrapper around print to make it easier to change logging later. """
    if DEBUG_FILE_PROCESSING:
        print(*args, **kwargs)


class CsvMerger:
    """ This class is consumes binified data, pulls in already-present data from S3, and merges the
    the data sets into their respective chunks (time bins), updates the relevant ChunkRegistry with
    the new size and checksum, and uploads the new data to S3, overwritinge existing chunk data.
    
    The Survey Timings data stream had a longstanding bug where files from all the different
    surveys would get merged into one file, and then also receive a file path without a survey id.
    The presence of a survey id in the __init__ now changes behavior to handle this, but
    but the many merged files of data consumed by this class MUST STILL BE FILTERED to be sourced
    on a single survey id. """
    
    def __init__(
        self,
        binified_data: AllBinifiedData,
        error_handler: ErrorHandler,
        participant: Participant,
        survey_id: str | None,
    ):
        assert isinstance(participant, Participant)
        assert survey_id is None or isinstance(survey_id, str)
        
        self.participant = participant
        self.survey_id = survey_id
        
        self.failed_ftps = set[FileToProcessPK]()
        self.ftps_to_retire = set[FileToProcessPK]()
        
        # The type definition of items in merged_data must match the do_upload function.
        self.upload_these: list[Uploadable] = []  # chunk, chunk_path, file content
        
        # Track the earliest and latest time bins, to return them at the end of the function
        self.earliest_time_bin: int | None = None
        self.latest_time_bin: int | None = None
        
        self.binified_data: AllBinifiedData = binified_data
        self.error_handler = error_handler
        self.iterate()
    
    def get_retirees(self) -> tuple[set[FileToProcessPK], set[FileToProcessPK], int | None, int | None]:
        """ returns the ftp pks that have succeeded, the of ftps that have failed,
        and the earliest and the latest time bins """
        return self.ftps_to_retire.difference(self.failed_ftps), \
            self.failed_ftps, self.earliest_time_bin, self.latest_time_bin
    
    def iterate(self):
        # this is the core loop. Iterate over all binified data and merge it into chunks, then
        # handle ChunkRegistry parameter setup for the next stage of processing.
        ftp_list: list[int]
        data_bin: BinifyKey
        while True:
            # this construction consumes elements from the dictionary as we iterate over them, it
            # saves memory because we are building up large byte arrays as we go from the data we
            # are pulling out of the dictionary.
            try:
                data_bin, (data_rows_list, ftp_list) = self.binified_data.popitem()
            
            except KeyError:
                break
            with self.error_handler:
                self.inner_iterate(data_bin, data_rows_list, ftp_list)
    
    def inner_iterate(
        self, data_bin: BinifyKey, data_rows_list: list[list[bytes]], ftp_list: list[int]
    ):
        study_object_id, patient_id, data_stream, time_bin, original_header, updated_header = None, None, None, None, None, None  # yuck need to prepopulate these for error cases, just leave it like this its better this way
        
        try:
            #     str            str          str        int         bytes
            study_object_id, patient_id, data_stream, time_bin, original_header = data_bin
            # Update earliest and latest time bins
            if self.earliest_time_bin is None or time_bin < self.earliest_time_bin:
                self.earliest_time_bin = time_bin
            if self.latest_time_bin is None or time_bin > self.latest_time_bin:
                self.latest_time_bin = time_bin
            
            # data_rows_list is a list of lists of bytes, each list of bytes is a row of data
            # these are from new files, they do not have the human readable timestamp column yet.
            updated_header = convert_unix_to_human_readable_timestamps(original_header, data_rows_list)
            chunk_path = construct_s3_chunk_path(
                study_object_id, patient_id, data_stream, time_bin, self.survey_id
            )
            
            # two core cases
            if ChunkRegistry.objects.filter(chunk_path=chunk_path).exists():
                self.chunk_exists_case(
                    chunk_path, study_object_id, updated_header, data_rows_list, data_stream
                )
            else:
                self.chunk_not_exists_case(
                    chunk_path, updated_header, data_stream, time_bin, data_rows_list
                )
        
        except Exception:
            # Here we catch any exceptions that may have arisen, as well as the ones that we raised
            # ourselves (e.g. HeaderMismatchException). Whichever FTP we were processing when the
            # exception was raised gets added to the set of failed FTPs.
            self.failed_ftps.update(ftp_list)
            log(
                f"FAILED TO UPDATE: study_object_id:{study_object_id}, patient_id:{patient_id}, "
                f"data_stream:{data_stream}, time_bin:{time_bin}, header:{updated_header}"
            )
            raise
        else:
            # If no exception was raised, the FTP has completed processing. Add it to the set of
            # retireable (i.e. completed) FTPs.
            self.ftps_to_retire.update(ftp_list)
    
    def chunk_not_exists_case(
        self,
        chunk_path: str,
        updated_header: bytes,
        data_stream: str,
        time_bin: int,
        rows: list[list[bytes]],
    ):
        name = chunk_path[38:]
        final_header = self.validate_one_header(updated_header, data_stream)
        
        ensure_sorted_by_timestamp(rows)  # final data transformation, a sort.
        
        with Timer() as t:
            new_contents = construct_csv_as_bytes(final_header, rows)  # build the csv, get the metadata
        
        rows.clear(); del rows  # memory usage paranoia begins
        
        size_uncompressed = len(new_contents)  # we don't use this anymore in the final return
        md5_hash = chunk_hash(new_contents)
        sha1_hash = hashlib.sha1(new_contents).digest()
        new_contents = compress(new_contents)  # This file hangs around in memory, compress it asap.
        log(f"CsvMerger: constructed {size_uncompressed} bytes (new) for {name} in {t.fseconds} seconds.")
        
        # this object will get **kwargs'd into ChunkRegistry.register_chunked_data
        chunk_params = {
            "study_id": self.participant.study_id,
            "participant_id": self.participant.id,
            "data_type": data_stream,
            "chunk_path": chunk_path,
            "chunk_hash": md5_hash,
            "time_bin": time_bin,
            "survey_id": self.survey_id,
            "file_size": size_uncompressed,  # we don't use this anymore in the final return...
        }
        self.upload_these.append((chunk_params, chunk_path, new_contents, sha1_hash, size_uncompressed, True))
    
    def chunk_exists_case(
        self,
        chunk_path: str,
        study_object_id: str,
        updated_header: bytes,
        new_rows: list[list[bytes]],
        data_stream: str,
    ):
        name = chunk_path[38:]
        
        with Timer() as t_retrieve:
            try:
                old_s3_file_data = [s3_retrieve(chunk_path, study_object_id, raw_path=True)]
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
        
        log(f"CsvMerger: retrieved existing data for {name} in {t_retrieve.fseconds} seconds.")
        
        orig_size = len(old_s3_file_data[0])
        with Timer() as t_unpack:  # get extant data from s3, merge with new binified data
            # use pop to reduce refcount to just that called function's scope (paranoid)
            s3_header, output_rows = existing_data_csv_splitter(old_s3_file_data.pop())
        
        old_s3_file_data.clear(); del old_s3_file_data  # deepest paranoia about memory usage.
        log(f"CsvMerger: merged {orig_size} bytes for {name} in {t_unpack.fseconds} seconds.")
        
        final_header = self.validate_two_headers(s3_header, updated_header, data_stream)
        output_rows.extend(new_rows)
        new_rows.clear(); del new_rows  # memory paranoia...
        
        ensure_sorted_by_timestamp(output_rows)  # merges data together with a sort and deduplicate
        
        # this construction ensures there is no reference to the output of construct_csv_string
        # in memory after this line.  Hopefully the gc is deterministic enough to benefit from that.
        # Construct csv string also deduplicates rows.
        with Timer() as t_construct:
            new_contents = construct_csv_as_bytes(final_header, output_rows)
        
        output_rows.clear(); del output_rows  # Some more paranoia
        log(f"CsvMerger: compressed new data for {name} in {t_construct.fseconds} seconds.")
        
        # get metadata before compressing
        size_uncompressed = len(new_contents)
        chunk_kwargs = {"chunk_hash": chunk_hash(new_contents), "file_size": size_uncompressed}
        
        # reuse variable to zero references to the uncompressed data
        sha1_hash = hashlib.sha1(new_contents).digest()
        new_contents = compress(new_contents)
        self.upload_these.append((chunk_kwargs, chunk_path, new_contents, sha1_hash, size_uncompressed, False))
    
    def validate_one_header(self, header: bytes, data_stream: str) -> bytes:
        # pp(self.participant)
        real_header = REFERENCE_CHUNKREGISTRY_HEADERS[data_stream][self.participant.os_type]
        if header == real_header:
            return real_header
        
        message = f'header was \n"{str(header)}"\n expected\n"{str(real_header)}"'
        GenericEvent.easy_create("bad_header_2", message)
        return real_header
    
    def validate_two_headers(self, header_a: bytes, header_b: bytes, data_stream: str) -> bytes:
        # if headers are the same run the single header logic
        if header_a == header_b:
            return self.validate_one_header(header_a, data_stream)
        
        real_header = REFERENCE_CHUNKREGISTRY_HEADERS[data_stream][self.participant.os_type]
        
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
        if RUNNING_TEST_OR_FROM_A_SHELL:
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
    study_object_id: str, patient_id: str, data_stream: str, time_bin: int, survey_id: str | None
) -> str:
    """ S3 file paths for chunks are of this form:
        CHUNKED_DATA/study_id/patient_id/data_stream/time_bin.csv """
    
    if data_stream in SURVEY_DATA_FILES and survey_id is None:
        raise ValueError("Survey ID must be provided for survey data files.")
    if data_stream not in SURVEY_DATA_FILES and survey_id is not None:
        raise ValueError("Survey ID should not be provided for non-survey data files.")
    
    if survey_id is not None:
        return "%s/%s/%s/%s/%s/%s.csv" % (
            CHUNKS_FOLDER, study_object_id, patient_id, data_stream, survey_id,
            unix_time_to_string(time_bin * CHUNK_TIMESLICE_QUANTUM).decode()
        )
    
    return "%s/%s/%s/%s/%s.csv" % (
        CHUNKS_FOLDER, study_object_id, patient_id, data_stream,
        unix_time_to_string(time_bin * CHUNK_TIMESLICE_QUANTUM).decode()
    )

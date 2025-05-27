import sys
import traceback

from constants.data_stream_constants import (ANDROID_LOG_FILE, CALL_LOG, CHUNKABLE_FILES,
    IDENTIFIERS, SURVEY_TIMINGS, WIFI)
from constants.user_constants import ANDROID_API
from database.data_access_models import FileToProcess
from libs.file_processing.data_fixes import (fix_app_log_file, fix_call_log_csv, fix_identifier_csv,
    fix_survey_timings, fix_wifi_csv)
from libs.file_processing.utility_functions_simple import s3_file_path_to_data_type
from libs.s3 import s3_retrieve


class SomeException(Exception): pass
class SomeException2(Exception): pass


# This file contains the class and necessary functions for the general data container
# class that we use.

HEADER_DEDUPLICATOR = {}

class FileForProcessing():
    def __init__(self, file_to_process: FileToProcess):
        self.file_to_process: FileToProcess = file_to_process
        self.data_type: str = s3_file_path_to_data_type(file_to_process.s3_file_path)
        self.chunkable: bool = self.data_type in CHUNKABLE_FILES
        self.file_contents: bytes | None = None
        
        # populated later
        self.file_lines: list[list[bytes]] | None = None
        self.header: bytes | None = None
        
        # state tracking
        self.exception: Exception | None = None
        self.traceback: str | None = None
        
        # magically populate at instantiation for now due to networking paradigm.
        self.download_file_contents()
    
    def clear_file_content(self):
        if self.file_contents is None:
            raise Exception("misuse, file_contents was already deleted.")
        self.file_contents = None
    
    def clear_file_lines(self):
        if self.file_contents is not None:
            raise Exception("misuse, file_contents was not deleted.")
        if self.file_lines is None:
            raise Exception("misuse, file_lines was already deleted.")
        self.file_lines = None
        self.header = None
    
    def download_file_contents(self) -> None:
        """ Handles network errors and updates state accordingly. """
        # blow up if misused
        if self.file_lines is not None:
            raise Exception("file_contents was already deleted.")
        
        # Try to retrieve the file contents. If any errors are raised, store them to be reraised by
        # the parent function
        try:
            self.file_contents = s3_retrieve(
                self.file_to_process.s3_file_path,
                self.file_to_process.study.object_id,
                raw_path=True
            )
        except Exception as e:
            traceback.print_exc()  # for debugging
            self.traceback = sys.exc_info() # type: ignore[assignment]
            self.exception = e
            raise SomeException(e)
    
    def raw_csv_to_line_list(self):
        """ Grab a list elements from of every line in the csv, strips off trailing whitespace. dumps
        them into a new list (of lists), and returns the header line along with the list of rows. """
        assert self.file_contents is not None, "file_contents was not populated. (1)"
        
        # case: the file coming in is just a single line, e.g. the header.
        # Need to provide the header and an empty iterator.
        if b"\n" not in self.file_contents:
            self.header = self.file_contents
            self.file_lines = []
            self.clear_file_content()
            return
        
        # normal case
        lines = self.file_contents.splitlines()
        self.clear_file_content()
        self.header = lines.pop(0)  # annoyingly slow, but after a lot of tests, this is the best/fastest way.
        self.file_lines = list(line.split(b",") for line in lines)
        
        # this is a dumb hack that turns all identical headers into references to the same, unique,
        # header string.  This is a stupid memory optimization.
        if self.header in HEADER_DEDUPLICATOR:
            self.header = HEADER_DEDUPLICATOR[self.header]
        else:
            HEADER_DEDUPLICATOR[self.header] = self.header
    
    def prepare_data(self):
        """ We need to apply fixes (in the correct order), and get the list of csv lines."""
        assert self.file_contents is not None, "file_contents was not populated (2)."
        assert self.file_lines is not None, "file_lines was not populated."
        
        # the android log file is weird, it is almost not a csv, more of a time enumerated list of
        # events. we need to fix it to be a csv.
        if self.file_to_process.os_type == ANDROID_API and self.data_type == ANDROID_LOG_FILE:
            self.file_contents = fix_app_log_file(
                self.file_contents, self.file_to_process.s3_file_path
            )
        
        # convert the file to a list of lines and columns
        self.raw_csv_to_line_list()
        
        assert self.header is not None, "header was not populated (1)."
        if self.file_to_process.os_type == ANDROID_API:
            # two android fixes require the data immediately, so we convert the generator to a list.
            if self.data_type == CALL_LOG:
                self.header = fix_call_log_csv(self.header, self.file_lines)
            elif self.data_type == WIFI:
                self.header = fix_wifi_csv(self.header, self.file_lines, self.file_to_process.s3_file_path)
        
        # these fixes are for Android and iOS
        if self.data_type == IDENTIFIERS:
            self.header = fix_identifier_csv(self.header, self.file_lines, self.file_to_process.s3_file_path)
        if self.data_type == SURVEY_TIMINGS:
            self.header = fix_survey_timings(self.header, self.file_lines, self.file_to_process.s3_file_path)
        
        # sometimes there is whitespace in the header? clean it.
        self.header = b",".join(tuple(column_name.strip() for column_name in self.header.split(b",")))
    
    def raise_data_processing_error(self):
        """ If we encountered any errors in retrieving the files for processing, they have been
        lumped together into data['exception']. Raise them here to the error handler and move to the
        next file. """
        
        print("\n" + self.file_to_process.s3_file_path)
        print(self.traceback)
        #########################################################
        # YOU ARE SEEING THIS EXCEPTION WITHOUT A STACK TRACE   #
        # BECAUSE IT OCCURRED INSIDE POOL.MAP ON ANOTHER THREAD #
        #########################################################
        if self.exception is None:
            raise Exception("FileForProcessing.exception was not populated.")
        raise self.exception


def is_version_greater_ios(proposed_version: str, reference_version: str) -> bool:
    """ Returns True if the proposed version is greater than the reference version.  Reference 
    MUST be a valid version number. Function should handle weird proposed versions. """
    # Old
    if proposed_version =="" or proposed_version is None:
        return False
    # this would be a bug
    if proposed_version.count(".") == 0 or proposed_version.count(".") > 2:
        raise ValueError(f"Invalid version number: {proposed_version}")
    
    # because we are stupid we need this.
    if proposed_version.count(".") == 1:
        proposed_version = proposed_version + ".0"
    
    # handle equal case
    if proposed_version == reference_version:
        return False
    
    # split the version numbers into major, minor, and patch
    proposed_major, proposed_minor, proposed_patch = proposed_version.split(".")
    reference_major, reference_minor, reference_patch = reference_version.split(".")
    proposed_major = int(proposed_major)
    proposed_minor = int(proposed_minor)
    proposed_patch = int(proposed_patch)
    reference_major = int(reference_major)
    reference_minor = int(reference_minor)
    reference_patch = int(reference_patch)
    # handle the math
    if proposed_major > reference_major:
        return True
    if proposed_minor > reference_minor:
        return True
    if proposed_patch > reference_patch:
        return True
    return False


def is_version_greater_android(proposed_version: str, reference_version: int) -> bool:
    """ Returns True if the proposed version is greater than the reference version.  Reference 
    MUST be a valid version code. Function should handle weird proposed versions. """
    # Old
    if proposed_version == "" or proposed_version is None:
        return False
    
    proposed_version_calculate = int(proposed_version)  # if it errors... we error.
    return proposed_version_calculate > reference_version

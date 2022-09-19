import sys
import traceback

from constants.data_stream_constants import CHUNKABLE_FILES
from database.data_access_models import FileToProcess
from libs.file_processing.utility_functions_simple import s3_file_path_to_data_type
from libs.s3 import s3_retrieve


class SomeException(Exception): pass
class SomeException2(Exception): pass


# This file contains the class and necessary functions for the general data container
# class that we use.


class FileForProcessing():
    def __init__(self, file_to_process: FileToProcess):
        self.file_to_process: FileToProcess = file_to_process
        self.data_type: str = s3_file_path_to_data_type(file_to_process.s3_file_path)
        self.chunkable: bool = self.data_type in CHUNKABLE_FILES
        self.file_contents: bytes = None
        
        # state tracking
        self.exception: Exception or None = None
        self.traceback: str or None = None
        
        # magically populate at instantiation for now due to networking paradigm.
        self.download_file_contents()
    
    def clear_file_content(self):
        self.file_contents: bytes = None
    
    def download_file_contents(self) -> bytes or None:
        """ Handles network errors and updates state accordingly """
        # Try to retrieve the file contents. If any errors are raised, store them to be raised by the
        # parent function
        print("downloading", self.file_to_process.s3_file_path)
        try:
            self.file_contents = s3_retrieve(
                self.file_to_process.s3_file_path,
                self.file_to_process.study.object_id,
                raw_path=True
            )
        except Exception as e:
            traceback.print_exc()
            self.traceback = sys.exc_info()
            self.exception = e
            raise SomeException(e)
        print("done.")
    
    def raise_data_processing_error(self):
        """
        If we encountered any errors in retrieving the files for processing, they have been
        lumped together into data['exception']. Raise them here to the error handler and
        move to the next file.
        """
        print("\n" + self.file_to_process.s3_file_path)
        print(self.traceback)
        ################################################################
        # YOU ARE SEEING THIS EXCEPTION WITHOUT A STACK TRACE
        # BECAUSE IT OCCURRED INSIDE POOL.MAP ON ANOTHER THREAD
        ################################################################
        raise self.exception

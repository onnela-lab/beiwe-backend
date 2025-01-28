import json
from multiprocessing.pool import ThreadPool
from typing import Generator, Iterable, Tuple
from zipfile import ZIP_STORED, ZipFile

from constants.data_stream_constants import (SURVEY_ANSWERS, SURVEY_TIMINGS,
    VOICE_RECORDING)
from database.study_models import Study
from libs.s3 import s3_retrieve
from libs.streaming_io import StreamingBytesIO


class DummyError(Exception): pass


def determine_file_name(chunk):
    """ Generates the correct file name to provide the file with in the zip file.
        (This also includes the folder location files in the zip.) """
    extension = chunk["chunk_path"][-3:]  # get 3 letter file extension from the source...
    chunk_path = chunk["chunk_path"]
    patient_id = chunk["participant__patient_id"]
    time_bin = str(chunk["time_bin"]).replace(":", "_")  # why wouldn't it be a string...?
    data_stream = chunk["data_type"]
    
    if data_stream == SURVEY_ANSWERS:
        # survey answers are not chunkable and we were getting some corrupted file names on
        #corrupted multi-uploads from ios.
        survey_id = chunk_path.rsplit("/", 2)[1]
        return f"{patient_id}/{data_stream}/{survey_id}/{time_bin}.csv"
    
    elif data_stream == SURVEY_TIMINGS:
        # add the survey_id from the database entry.
        survey_id = chunk["survey__object_id"]
        return f"{patient_id}/{data_stream}/{survey_id}/{time_bin}.{extension}"
    
    elif data_stream == VOICE_RECORDING:
        # Audio surveys are also not chunkable and have to work out some extra logic
        # survey_id = chunk_path.rsplit("/", 2)[1]
        survey_id = chunk["survey__object_id"]
        # if for some reason there is no match  just use the original extension.
        if ".mp4" in chunk_path:
            extension = "mp4"
        elif ".wav" in chunk_path:
            extension = "wav"
        
        if chunk_path.count("/") == 4:
            return f"{patient_id}/{data_stream}/{survey_id}/{time_bin}.{extension}"
    
    # all other files have this form:
    return f"{patient_id}/{data_stream}/{time_bin}.{extension})"


def batch_retrieve_s3(chunk: dict) -> Tuple[dict, bytes]:
    """ Data is returned in the form (chunk_object, file_data). """
    return chunk, s3_retrieve(
        chunk["chunk_path"],
        Study.objects.get(id=chunk["study_id"]),
        raw_path=True,
    )


class ZipGenerator:
    """ Pulls in data from S3 in a multithreaded network operation, constructs a zip file of that
    data. This is a generator, advantage is it starts returning data (file by file, but wrapped
    in zip compression) almost immediately.
    NOTE: does not compress! """
    
    def __init__(self, files_list: Iterable[str], construct_registry: bool, threads: int = 3):
        self.construct_registry = construct_registry
        self.files_list = files_list
        self.processed_files: set[str] = set()
        self.duplicate_files: set[str] = set()  # mostly for debugging
        self.file_registry: dict[str, str] = {}
        self.total_bytes = 0
        self.threads = threads
    
    def deduplicate_names(self, file_name: str) -> str:
        """ adds '(1)', '(2)', etc to duplicate file names, checking and updateding self.duplicate_files. """
        i = 1
        while True:
            i += 1
            new_filename = f"{file_name}({i})"
            if new_filename not in self.duplicate_files:
                self.duplicate_files.add(new_filename)
                return new_filename
    
    def __iter__(self) -> Generator[bytes, None, None]:
        pool = ThreadPool(self.threads)
        zip_output = StreamingBytesIO()
        zip_input = ZipFile(zip_output, mode="w", compression=ZIP_STORED, allowZip64=True)
        try:
            # chunks_and_content is a list of tuples, of the chunk and the content of the file.
            # chunksize (which is a keyword argument of imap, not to be confused with Beiwe Chunks)
            # is the size of the batches that are handed to the pool. We always want to add the next
            # file to retrieve to the pool asap, so we want a chunk size of 1.
            # (In the documentation there are comments about the timeout, it is irrelevant under this construction.)
            chunks_and_content = pool.imap_unordered(batch_retrieve_s3, self.files_list, chunksize=1)
            
            for chunk, file_contents in chunks_and_content:
                if self.construct_registry:
                    self.file_registry[chunk['chunk_path']] = chunk["chunk_hash"]
                
                file_name = determine_file_name(chunk)
                if file_name in self.processed_files:
                    self.duplicate_files.add((file_name, chunk['chunk_path'], ))
                    
                    continue
                self.processed_files.add(file_name)
                
                zip_input.writestr(file_name, file_contents)
                
                # These can be large, and we don't want them sticking around in memory as we wait
                # for the yield, and they could be many megabytes and it is about to be duplicated.
                del file_contents, chunk
                
                # write data to your stream
                one_file_in_a_zip = zip_output.getvalue()
                self.total_bytes += len(one_file_in_a_zip)
                yield one_file_in_a_zip
                # print "%s: %sK, %sM" % (random_id, total_bytes / 1024, total_bytes / 1024 / 1024)
                
                del one_file_in_a_zip
                zip_output.empty()
            
            # construct the registry file
            if self.construct_registry:
                zip_input.writestr("registry", json.dumps(self.file_registry))
                yield zip_output.getvalue()
                zip_output.empty()
            
            # close, then yield all remaining data in the zip.
            zip_input.close()
            yield zip_output.getvalue()
        
        except DummyError:
            # The try-except-finally block is here to guarantee the Threadpool is closed and terminated.
            # we don't handle any errors, we just re-raise any error that shows up.
            # (a with statement historically is insufficient. I don't know why.)
            raise
        finally:
            # We rely on the finally block to ensure that the threadpool will be closed and terminated,
            # and also to print an error to the log if we need to.
            pool.close()
            pool.terminate()

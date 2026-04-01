import json
from collections.abc import Generator, Iterable
from contextlib import suppress
from multiprocessing.pool import ThreadPool
from zipfile import ZIP_STORED, ZipFile

from constants.s3_constants import NoSuchKeyException
from database.models import Study
from endpoints.participant_endpoints import SentryUtils
from libs.s3 import s3_retrieve, s3_retrieve_no_decompress
from libs.streaming_io import StreamingBytesIO
from libs.utils.file_name_utils import determine_base_file_name


class DummyError(Exception): pass


class ZipGenerator:
    """ Pulls in data from S3 in a multithreaded network operation, constructs a zip file of that
    data. This is a generator, advantage is it starts returning data (file by file, but wrapped
    in zip compression) almost immediately.  NOTE! The zip itself is just an uncompressed container! """
    
    def __init__(
        self,
        study: Study,
        files_list: Iterable[dict],
        construct_registry: bool,
        threads: int,
        as_compressed: bool,
    ):
        self.file_registry: dict[str, str] | None = {} if construct_registry else None
        self.files_list = files_list
        self.processed_file_names: set[str] = set()
        self.total_bytes = 0
        self.thread_count = threads * 3 if as_compressed else threads
        self.as_compressed = as_compressed
        self.study = study
        self.batch_retrive_func = self._retrieve_no_decompress if as_compressed else self._retrieve_decompress
        self.stopped = False
    
    def stop(self) -> None:
        self.stopped = True
    
    def _retrieve_decompress(self, chunk: dict) -> tuple[dict, bytes | None]:
        """ Data is returned in the form (chunk_object, file_data), as the decompressed file. """
        if self.stopped:
            return chunk, None  # early exit if stopped
        
        try:
            return chunk, s3_retrieve(chunk["chunk_path"], self.study, raw_path=True)
        except NoSuchKeyException:
            with SentryUtils.report_webserver():
                raise
            return chunk, None
            
    
    def _retrieve_no_decompress(self, chunk: dict) -> tuple[dict, bytes | None]:
        """ Data is returned in the form (chunk_object, file_data), as a .zst file. """
        if self.stopped:
            return chunk, None  # early exit if stopped
        
        try:
            return chunk, s3_retrieve_no_decompress(chunk["chunk_path"], self.study, raw_path=True)
        except NoSuchKeyException:
            with SentryUtils.report_webserver():
                raise
            return chunk, None
    
    def get_file_name_from_chunk(self, chunk: dict) -> str:
        file_name = determine_base_file_name(chunk)
        return self.process_file_name(file_name)
    
    def process_file_name(self, file_name: str) -> str:
        if file_name in self.processed_file_names:
            file_name = self.generate_deduplicated_name(file_name)
        
        self.processed_file_names.add(file_name)
        return file_name + ".zst" if self.as_compressed else file_name
    
    def generate_deduplicated_name(self, filename: str) -> str:
        i, (filename_base, extension) = 1, filename.rsplit(".", 1)
        while True:
            i += 1
            if (new_filename := f"{filename_base}_{i}.{extension}") not in self.processed_file_names:
                return new_filename
    
    def __iter__(self) -> Generator[bytes, None, None]:
        pool = ThreadPool(self.thread_count)
        zip_output = StreamingBytesIO()
        zip_input = ZipFile(zip_output, mode="w", compression=ZIP_STORED, allowZip64=True)
        try:
            # chunks_and_content is a list of tuples, of the chunk and the content of the file.
            # chunksize (which is a keyword argument of imap, not to be confused with Beiwe Chunks,
            # this is just an unfortunate coincidence) is the size of the batches that are handed to
            # the pool. We always want to add the next file to retrieve to the pool asap, so we want
            # a chunk size of 1.
            
            # (In the documentation there are comments about the timeout, it is irrelevant under this construction.)
            chunks_and_content = pool.imap_unordered(self.batch_retrive_func, self.files_list, chunksize=1)
            
            for chunk, file_contents in chunks_and_content:
                
                if self.stopped or file_contents is None:
                    continue  # (when stopped, threading hangs if we don't exhaust the loop.)
                
                if self.file_registry is not None:
                    self.file_registry[chunk['chunk_path']] = chunk["chunk_hash"]
                
                zip_input.writestr(self.get_file_name_from_chunk(chunk), file_contents)
                
                # file_contents may be Megabytes, and we don't want them sticking around in memory
                # as we wait for the yield. It _may_ get garbage collected early depending on
                # implementation of ZipFile / BytesIO, because it was _probably_ just now
                # _memcopied_ into the BytesIO stream, leaving only this reference.
                del file_contents, chunk
                
                # write data to your stream, memory manage due to same logic as above, record stats
                one_file_in_a_zip = zip_output.getvalue()
                self.total_bytes += len(one_file_in_a_zip)
                yield one_file_in_a_zip
                del one_file_in_a_zip
                zip_output.empty()
            
            if not self.stopped:
                # construct the registry file
                if self.file_registry is not None:
                    zip_input.writestr("registry", json.dumps(self.file_registry))
                    yield zip_output.getvalue()
                    zip_output.empty()
                
                # close, then yield all remaining data in the zip.
                zip_input.close()
                yield zip_output.getvalue()
        
        finally:
            pool.close()        # For some reason close is not called inside the threadpool when you
            pool.terminate()    # use it in a with statement as a context processor.
            
            # if there is an error of any kind we want to (blindly) call these.
            with suppress(Exception):
                zip_output.empty()
            with suppress(Exception):
                zip_input.close()

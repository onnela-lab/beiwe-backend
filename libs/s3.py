from __future__ import annotations

import hashlib
from collections.abc import Generator
from os.path import join as path_join
from time import perf_counter_ns
from typing import TYPE_CHECKING
from unittest.mock import MagicMock

import boto3
import botocore
from botocore.client import BaseClient
from botocore.exceptions import ClientError as Boto3ClientError
from botocore.paginate import Paginator
from cronutils import ErrorHandler
from django.utils import timezone

from config.settings import (BEIWE_SERVER_AWS_ACCESS_KEY_ID, BEIWE_SERVER_AWS_SECRET_ACCESS_KEY,
    ENABLE_IOS_FILE_RECOVERY, S3_BUCKET, S3_ENDPOINT, S3_REGION_NAME)
from constants.common_constants import (CHUNKS_FOLDER, CUSTOM_ONDEPLOY_PREFIX, PROBLEM_UPLOADS,
    RUNNING_TESTS)
from constants.s3_constants import (BAD_FOLDER, BAD_FOLDER_2, BadS3PathException,
    COMPRESSED_DATA_MISSING_AT_UPLOAD, COMPRESSED_DATA_MISSING_ON_POP,
    COMPRESSED_DATA_PRESENT_AT_COMPRESSION, COMPRESSED_DATA_PRESENT_ON_ASSIGNMENT,
    COMPRESSED_DATA_PRESENT_ON_DOWNLOAD, IOSDataRecoveryDisabledException, MetaDotDict,
    MUST_BE_ZSTD_FORMAT, NoSuchKeyException, S3DeletionException, SMART_GET_ERROR,
    UNCOMPRESSED_DATA_MISSING_AT_COMPRESSION, UNCOMPRESSED_DATA_MISSING_ON_POP,
    UNCOMPRESSED_DATA_PRESENT_ON_ASSIGNMENT, UNCOMPRESSED_DATA_PRESENT_ON_DOWNLOAD,
    UNCOMPRESSED_DATA_PRESENT_WRONG_AT_UPLOAD)
from libs.aes import decrypt_server, encrypt_for_server
from libs.utils.compression import compress, decompress


## This file must be near-globally importable, including inside db models; so these imports fail.
# If you need to use a model you must to use a local import.
if TYPE_CHECKING:
    from database.models import Participant, Study
    StrPartStudy = str | Participant | Study


## Global S3 Connection

conn: BaseClient = boto3.client(
    's3',
    aws_access_key_id=BEIWE_SERVER_AWS_ACCESS_KEY_ID,
    aws_secret_access_key=BEIWE_SERVER_AWS_SECRET_ACCESS_KEY,
    region_name=S3_REGION_NAME,
    endpoint_url=S3_ENDPOINT,
    config=botocore.config.Config(max_pool_connections=100),  # type: ignore
)

if RUNNING_TESTS:                       # This lets us cut out some boilerplate in tests
    S3_REGION_NAME = "us-east-1"        # Tests that need to mock S3 can still mock the conn object
    S3_BUCKET = "test_bucket"
    conn = MagicMock()


#
## Smart Key and Path Getters
#

def smart_get_study_encryption_key(obj: StrPartStudy) -> bytes:
    from database.models import Participant, Study
    if isinstance(obj, Participant):
        return Study.value_get("encryption_key", pk=obj.study_id).encode()
    elif isinstance(obj, Study):
        return obj.encryption_key.encode()
    elif isinstance(obj, str) and len(obj) == 24:
        return Study.value_get("encryption_key", object_id=obj).encode()
    else:
        raise TypeError(SMART_GET_ERROR.format(type(obj)))


def s3_construct_study_key_path(key_path: str, obj: StrPartStudy) -> str:
    return get_just_prefix(obj) + "/" + key_path


def get_just_prefix(obj: StrPartStudy) -> str:
    from database.models import Participant, Study
    if isinstance(obj, Participant):
        return Study.value_get("object_id", pk=obj.study_id)
    elif isinstance(obj, Study):
        return obj.object_id
    elif isinstance(obj, str) and len(obj) == 24:  # extremely basic check
        return obj
    else:
        raise TypeError(SMART_GET_ERROR.format(type(obj)))

#
## The S3 Storage Class - shim class that is used when actually pulling down s3 files.
#

class S3Storage:
    """ A class that manages the lifecycle of encryption and compression of files on S3. """
    
    def __init__(self, s3_path: str, obj: StrPartStudy, bypass_study_folder: bool) -> None:
        from database.models import Participant, Study

        # todo: add handling of the None cose for smart_key_obj, where some api calls are disabled
        self.smart_key_obj = obj  # Study, Participant, or 24 char str
        self.validate_file_paths(s3_path, bypass_study_folder)
        
        # self.uncompressed_data = None  # DON'T ADD AS CONSTRUCTOR PARAMETER; FORCE MEMORY MANAGEMENT.
        self.metadata = MetaDotDict()
        
        # DB fields
        if isinstance(self.smart_key_obj, Study):
            self.metadata.study_id = self.smart_key_obj.pk
        elif isinstance(self.smart_key_obj, Participant):
            self.metadata.participant_id = self.smart_key_obj.pk
            self.metadata.study_id = self.smart_key_obj.study_id
    
    def validate_file_paths(self, path: str, bypass_study_folder: bool):
        # for historical reasons we only sometimes have the full file path. Files only go in a few places:
        #  - the study's folder, which is the object id volue on the Study (24 characters)
        #  - the chunks folder - time-binned chunked data in the chunk registry - file structure
        #    mimics the normal file structure.
        if path.endswith(".zst"):
            raise BadS3PathException("path should never end with .zst")
        
        # We don't validate file paths inside study folders.
        if not bypass_study_folder:
            self.s3_path_uncompressed = path_join(self.get_path_prefix, path)
            self.s3_path_zst = self.s3_path_uncompressed + ".zst"
            return
        
        # We have some very specific folders and extra clear error messages
        # (and we do allow the correct study folder on a raw path)
        path_start = path.split("/", 1)[0]
        if path_start == CUSTOM_ONDEPLOY_PREFIX:
            raise BadS3PathException(BAD_FOLDER.format(path_start=path_start, path=path))
        
        if not ENABLE_IOS_FILE_RECOVERY and path_start == PROBLEM_UPLOADS:
            raise IOSDataRecoveryDisabledException(BAD_FOLDER.format(path_start=path_start, path=path))
        
        if path_start not in [CHUNKS_FOLDER, PROBLEM_UPLOADS] and path_start != self.get_path_prefix:
            raise BadS3PathException(BAD_FOLDER_2.format(path_start=path_start, path=path))
        
        self.s3_path_uncompressed = path
        self.s3_path_zst = path + ".zst"
    
    ## File State Tracking
    
    def set_file_content_uncompressed(self, file_content: bytes) -> S3Storage:
        # validate - handles None case
        if not isinstance(file_content, bytes):
            raise TypeError(f"file_content must be bytes, received {type(file_content)}")
        # misuse
        assert not hasattr(self, "compressed_data"), COMPRESSED_DATA_PRESENT_ON_ASSIGNMENT
        assert not hasattr(self, "uncompressed_data"), UNCOMPRESSED_DATA_PRESENT_ON_ASSIGNMENT
        self.uncompressed_data = file_content
        return self
    
    def set_file_content_compressed(self, file_content: bytes) -> S3Storage:
        # validate - handles None case
        if not isinstance(file_content, bytes):
            raise TypeError(f"file_content must be bytes, received {type(file_content)}")
        # zstd format check
        if not file_content.startswith(b'(\xb5/\xfd'):
            raise ValueError(MUST_BE_ZSTD_FORMAT(file_content=file_content[:10].decode()))
        
        # missuse cases
        assert not hasattr(self, "compressed_data"), COMPRESSED_DATA_PRESENT_ON_ASSIGNMENT
        assert not hasattr(self, "uncompressed_data"), UNCOMPRESSED_DATA_PRESENT_ON_ASSIGNMENT
        
        self.compressed_data = file_content  # TLDR - only compressed_data should now be set
        self.metadata.size_compressed = len(self.compressed_data)
        return self
    
    def pop_uncompressed_file_content(self) -> bytes:
        assert hasattr(self, "uncompressed_data"), UNCOMPRESSED_DATA_MISSING_ON_POP
        uncompressed_data = self.uncompressed_data
        del self.uncompressed_data
        return uncompressed_data
    
    def pop_compressed_file_content(self) -> bytes:
        assert hasattr(self, "compressed_data"), COMPRESSED_DATA_MISSING_ON_POP
        compressed_data = self.compressed_data
        del self.compressed_data
        return compressed_data
    
    # Upload API
    
    def compress_and_push_to_storage_and_clear_memory(self):
        self.compress_and_push_to_storage_retaining_compressed()
        del self.compressed_data
    
    def compress_and_push_to_storage_retaining_compressed(self):
        self.compress_data_and_clear_uncompressed()
        self._s3_upload_zst_and_profile()
        self.update_s3_table()
    
    def push_to_storage_already_compressed_and_clear_memory(self):
        # when you have populated compressed_data in memory and want to push to s3
        assert hasattr(self, "compressed_data"), COMPRESSED_DATA_MISSING_AT_UPLOAD
        assert not hasattr(self, "uncompressed_data"), UNCOMPRESSED_DATA_PRESENT_WRONG_AT_UPLOAD
        self._s3_upload_zst_and_profile()
        del self.compressed_data
        self.update_s3_table()
    
    ## Compression
    
    def compress_data_and_clear_uncompressed(self):
        # when compressing data in real code, use this function.
        self._compress_data_and_profile()
        del self.uncompressed_data  # removes the uncompressed data from memory
    
    def _compress_data_and_profile(self):
        # it is important that this only occur once, and that all compression go through this function
        assert hasattr(self, "uncompressed_data"), UNCOMPRESSED_DATA_MISSING_AT_COMPRESSION
        assert not hasattr(self, "compressed_data"), COMPRESSED_DATA_PRESENT_AT_COMPRESSION
        self.metadata.size_uncompressed = len(self.uncompressed_data)
        
        # sha1 is twice as fast as md5, it is 20 bytes, we care about speed here.
        self.metadata.sha1 = hashlib.sha1(self.uncompressed_data).digest()
        
        t_compress = perf_counter_ns()
        self.compressed_data = compress(self.uncompressed_data)
        t_compress = perf_counter_ns() - t_compress
        self.metadata.compression_time_ns = t_compress
        self.metadata.size_compressed = len(self.compressed_data)
    
    ## Decompress
    
    def _decompress_and_profile(self):
        # data as optional because sometimes we want to avoid storing compressed data on the S3Storage object
        t_decompress = perf_counter_ns()
        self.uncompressed_data = decompress(self.compressed_data)
        t_decompress = perf_counter_ns() - t_decompress
        self.metadata.decompression_time_ns = t_decompress
        self.metadata.size_uncompressed = len(self.uncompressed_data)
    
    ## Download
    
    def download(self) -> S3Storage:
        assert not hasattr(self, "compressed_data"), COMPRESSED_DATA_PRESENT_ON_DOWNLOAD
        assert not hasattr(self, "uncompressed_data"), UNCOMPRESSED_DATA_PRESENT_ON_DOWNLOAD
        try:
            self._download_decompress_and_profile_clearing_compressed()
        except NoSuchKeyException:
            self._download_and_rewrite_s3_as_compressed_retaining_uncompressed()
        return self
    
    def download_no_decompress(self):
        assert not hasattr(self, "compressed_data"), COMPRESSED_DATA_PRESENT_ON_DOWNLOAD
        assert not hasattr(self, "uncompressed_data"), UNCOMPRESSED_DATA_PRESENT_ON_DOWNLOAD
        try:
            self.compressed_data = self._s3_retrieve_zst_and_profile()
        except NoSuchKeyException:
            # drop the uncompressed copy, even though we did the full loop
            self._download_and_rewrite_s3_retaining_compressed()
        return self
    
    def _download_decompress_and_profile_clearing_compressed(self):
        # This line should be the error on when there is no compressed copy
        self.compressed_data = self._s3_retrieve_zst_and_profile()
        self._decompress_and_profile()
        del self.compressed_data
        self.update_s3_table()
    
    ## Download-Rewrite
    
    def _download_and_rewrite_s3_as_compressed_retaining_uncompressed(self):
        self.uncompressed_data = self._download_and_rewrite_s3_retaining_compressed()
    
    def _download_and_rewrite_s3_retaining_compressed(self) -> bytes:
        raw_data = self._s3_retrieve_uncompressed()
        self.uncompressed_data = raw_data  # must be set for rewrite, gets cleared
        self.compress_and_push_to_storage_retaining_compressed()  # clears uncompressed
        self._s3_delete_uncompressed()
        return raw_data
    
    #
    ## DB ops
    #
    
    def update_metadata(self, **kwargs):
        for k,v in kwargs.items():
            self.metadata[k] = v
    
    def update_s3_table(self):
        from database.models import S3File
        self.metadata.last_updated = timezone.now()
        S3File.objects.update_or_create(path=self.s3_path_zst, defaults=self.metadata)
    
    def delete_s3_table_entry_zst(self):
        from database.models import S3File
        S3File.fltr(path=self.s3_path_zst).delete()  # can't actually fail
    
    # (these cached properties may need network/db ops)
    # Todo: cache for real? do we want to cache the encryption keys globally?
    
    @property
    def encryption_key(self) -> bytes:
        if hasattr(self, "_encryption_key"):
            return self._encryption_key
        self._encryption_key = smart_get_study_encryption_key(self.smart_key_obj)
        return self._encryption_key
    
    @property
    def get_path_prefix(self) -> str:
        if hasattr(self, "_the_path_prefix"):
            return self._the_path_prefix
        self._the_path_prefix = get_just_prefix(self.smart_key_obj)
        return self._the_path_prefix
    
    #
    ## S3 OPS
    #
    
    ## Delete
    
    def _s3_delete_zst(self):
        s3_delete(self.s3_path_zst)
        self.delete_s3_table_entry_zst()
    
    def _s3_delete_uncompressed(self):
        s3_delete(self.s3_path_uncompressed)
    
    ## Upload
    
    def _s3_upload_zst_and_profile(self):
        """ Manually manage these memory/reference count operations.  It matters.
        This is a critical performance path. DO NOT separate into further functions calls without
        profiling memory usage. """
        
        t_encrypt = perf_counter_ns()
        encrypted_compressed_data = encrypt_for_server(self.compressed_data, self.encryption_key)
        t_encrypt = perf_counter_ns() - t_encrypt
        self.metadata.encryption_time_ns = t_encrypt
        
        t_upload = perf_counter_ns()
        _do_upload(self.s3_path_zst, encrypted_compressed_data)  # probable 2x memory usage
        t_upload = perf_counter_ns() - t_upload
        self.metadata.upload_time_ns = t_upload
        
        del encrypted_compressed_data  # 0x memory
        self.update_s3_table()
    
    ## Retrieve
    
    def _s3_retrieve_uncompressed(self) -> bytes:
        return decrypt_server(self._raw_s3_retrieve(self.s3_path_uncompressed), self.encryption_key)
    
    def _s3_retrieve_zst_and_profile(self) -> bytes:
        key = self.encryption_key  # may have network/db op
        
        t_download = perf_counter_ns()
        try:
            data = self._raw_s3_retrieve(self.s3_path_zst)
        except NoSuchKeyException:
            # if it doesn't exist, delete the entry in the database
            self.delete_s3_table_entry_zst()
            raise
        
        t_download = perf_counter_ns() - t_download
        self.metadata.download_time_ns = t_download
        
        t_decrypt = perf_counter_ns()
        ret = decrypt_server(data, key)
        t_decrypt = perf_counter_ns() - t_decrypt
        self.metadata.decrypt_time_ns = t_decrypt
        
        self.metadata.size_compressed = len(ret)  # after decryption, no iv or padding
        del data
        
        return ret
    
    def _raw_s3_retrieve(self, path: str) -> bytes:
        return _do_retrieve(path)['Body'].read()


#
## S3 Operations
#


## MetaData

# todo: update list to provide this detail
def s3_get_size(key_path: str):
    try:
        return conn.head_object(Bucket=S3_BUCKET, Key=key_path)["ContentLength"]
    except Boto3ClientError as e:
        # ClientError: An error occurred (404) when calling the HeadObject operation: Not Found
        if e.response['Error']['Code'] == '404':
            raise NoSuchKeyException(f"{key_path}") from None


## Upload


def s3_upload(
    key_path: str, data_string: bytes, obj: StrPartStudy, raw_path=False
) -> None:
    """ Uploads a bytes object as a file, encrypted using the encryption key of the study it is
    associated with. Intelligently accepts a string, Participant, or Study object as needed. """
    storage = S3Storage(key_path, obj, raw_path).set_file_content_uncompressed(data_string)
    storage.compress_and_push_to_storage_and_clear_memory()


def s3_upload_no_compression(
    key_path: str, data_string: bytes, obj: StrPartStudy, raw_path=False
):
    storage = S3Storage(key_path, obj, raw_path).set_file_content_compressed(data_string)
    storage.push_to_storage_already_compressed_and_clear_memory()


def s3_upload_plaintext(upload_path: str, data_string: bytes) -> None:
    """ Extremely simple, uploads a file (bytes object) to s3 without any encryption.
    Intended for use with custom deploy scripts, etc. """
    conn.put_object(Body=data_string, Bucket=S3_BUCKET, Key=upload_path)


def _do_upload(key_path: str, data_string: bytes, number_retries=3):
    """ In ~April 2022 this api call started occasionally failing, so wrapping it in a retry. """
    try:
        conn.put_object(Body=data_string, Bucket=S3_BUCKET, Key=key_path)
    except Exception as e:
        # these boto errors are terrible, in this case all we can do is look at the string.
        if "Please try again" not in str(e):
            raise
        _do_upload(key_path, data_string, number_retries=number_retries - 1)


## Download


def s3_retrieve(key_path: str, obj: StrPartStudy, raw_path: bool = False, number_retries=3) -> bytes:
    """ Takes an S3 file path (key_path), and a study ID.  Takes an optional argument, raw_path,
    which defaults to false.  When set to false the path is prepended to place the file in the
    appropriate study_id folder. """
    # This reference pattern clears internal references before the return statement.
    return S3Storage(key_path, obj, raw_path).download().pop_uncompressed_file_content()


def s3_retrieve_no_decompress(key_path: str, obj: StrPartStudy, raw_path: bool = False) -> bytes:
    """ As s3_retrieve, but does not decompress the file. """
    return S3Storage(key_path, obj, raw_path).download_no_decompress().pop_compressed_file_content()


def s3_retrieve_plaintext(key_path: str, number_retries=3) -> bytes:
    """ Retrieves a file as-is as bytes. """
    return _do_retrieve(key_path, number_retries=number_retries)['Body'].read()


def _do_retrieve(key_path: str, number_retries=3) -> Boto3Response:
    """ Run-logic to do a data retrieval for a file in an S3 bucket."""
    try:
        return conn.get_object(Bucket=S3_BUCKET, Key=key_path, ResponseContentType='string')
    except Boto3ClientError as e:
        # Only retry if the error is not a "NoSuchKey" error.  This error class is JUST STUPID.
        # - `botocore.errorfactory.NoSuchKey` cannot be imported because it is generated at runtime.
        # - This error has the same structure as the Boto3ClientError as up in s3_get_size when there
        #   is "NoSuchKey", but it uses a different magic word - "NoSuchKey" instead of "404".
        if e.response['Error']['Code'] == 'NoSuchKey':
            raise NoSuchKeyException(f"{key_path}") from None
        
        # usually we want to try again
        if number_retries > 0:
            print(f"s3_retrieve failed, retrying on `{key_path}`")
            return _do_retrieve(key_path, number_retries=number_retries - 1)
        
        raise  # unknown cases: explode

#
## List Files Matching Prefix
#
def s3_list_files(prefix: str, as_generator=False, start_at=None) -> list[str]|Generator[str]:
    """ Lists s3 keys matching prefix. as generator returns a generator instead of a list.
    WARNING: passing in an empty string can be dangerous. """
    return _do_list_files(S3_BUCKET, prefix, as_generator=as_generator, start_at=start_at)


def smart_s3_list_study_files(prefix: str, obj: StrPartStudy, start_at=None) -> list[str]|Generator[str]:
    """ Lists s3 keys matching prefix, autoinserting the study object id at start of key path. """
    return s3_list_files(s3_construct_study_key_path(prefix, obj), start_at=start_at)


def _do_list_files(bucket_name: str, prefix: str, as_generator=False, start_at=None) -> list[str]|Generator[str]:
    """     Possibly useful params: Bucket, Prefix, StartAfter, and maybe PaginationConfig.
    - PaginationConfig, a dict that accepts MaxItems (not MaxKeys, this one limits the number of
      returns not the page size), StartingToken, PageSize (I have checked the code).
    - Useless params are ContinuationToken, RequestPayer, ExpectedBucketOwner, MaxKeys, FetchOwner,
      OptionalObjectAttributes, EncodingType, and Delimiter. """
    # PAGE SIZE DEFAULT IS 1,000 AND CANNOT BE MADE LARGER.  STOP TRYING.
    page_iterator: Paginator.PAGE_ITERATOR_CLS = conn.get_paginator('list_objects_v2').paginate(
        Bucket=bucket_name, Prefix=prefix, **{"StartAfter": start_at} if start_at else {}
    )
    if as_generator:
        return _do_list_files_generator(page_iterator)
    
    items = []
    for page in page_iterator:
        if 'Contents' in page:
            # strip() is the same speed as rstrip(), both are faster than endwith()
            items.extend(item['Key'].strip("/") for item in page['Contents'])
    return items


def _do_list_files_generator(page_iterator: Paginator.PAGE_ITERATOR_CLS) -> Generator[str]:
    # try-except is faster than checking twice, there doesn't seem to be faster option
    for page in page_iterator:
        try:
            for item in page['Contents']:
                yield item['Key'].strip("/")
        except KeyError as e:
            if 'Contents' not in page:
                return
            raise KeyError("Unknown KeyError in _do_list_files_generator") from e


def s3_list_versions(prefix: str) -> Generator[tuple[str, str|None]]:
    """ Generator of all matching key paths and their version ids.  Performance in unpredictable, it
    is based on the historical presence of key paths matching the prefix, it is paginated, but we
    don't care about deletion markers """
    for page in conn.get_paginator('list_object_versions').paginate(Bucket=S3_BUCKET, Prefix=prefix):
        # Page structure - each page is a dictionary with these keys:
        #    Name, ResponseMetadata, Versions, MaxKeys, Prefix, KeyMarker, IsTruncated, VersionIdMarker
        # We only care about 'Versions', which is a list of all object versions matching that prefix.
        # Versions is a list of dictionaries with these keys:
        #    LastModified, VersionId, ETag, StorageClass, Key, Owner, IsLatest, Size
        ## If versions is not present that means the entry is a deletion marker and can be skipped.
        if 'Versions' not in page:
            continue
        
        for s3_version in page['Versions']:
            # If versioning is disabled on the bucket then version id is "null", otherwise it will
            # be a real value. (Literally:  {'VersionId': 'null', 'Key': 'BEAUREGARD', ...}  )
            version = s3_version['VersionId']
            if version == "null":  # clean it up, no "null" strings, no INSANE boto formatting
                version = None
            yield s3_version['Key'], version


## Delete

def s3_delete(key_path: str) -> bool|None:
    """ None means no info. """
    # the actual response contains no state indicating that a file was deleted.
    # there are ~transaction IDs that we can probably use to track operations, but we don't care.
    resp = conn.delete_object(Bucket=S3_BUCKET, Key=key_path)
    # this only exists if there is/was versioning on the bucket
    if "DeleteMarker" not in resp:
        return None
    if not resp["DeleteMarker"]:
        raise S3DeletionException(f"Failed to delete {resp['Key']} version {resp['VersionId']}")
    return resp["DeleteMarker"]


def s3_delete_versioned(key_path: str, version_id: str) -> bool:
    resp = conn.delete_object(Bucket=S3_BUCKET, Key=key_path, VersionId=version_id)
    if not resp["DeleteMarker"]:
        raise S3DeletionException(f"Failed to delete {resp['Key']} version {resp['VersionId']}")
    return resp["DeleteMarker"]


def s3_delete_many_versioned(paths_version_ids: list[tuple[str, str]]):
    """ Takes a list of (key_path, version_id) and deletes them all using the boto3 delete_objects
    API.  Returns the number of files deleted, raises errors with reasonable clarity inside an
    errorhandler bundled error. """
    error_handler = ErrorHandler()  # use an ErrorHandler to bundle up all errors and raise them at the end.
    
    # construct the usual insane boto3 dict - if version id is falsey, it must be a string, not None.
    if not paths_version_ids:
        raise Exception("s3_delete_many_versioned called with no paths.")
    
    delete_params = {
        'Objects': [{'Key': key_path, 'VersionId': version_id or "null"}
                    for key_path, version_id in paths_version_ids]
    }
    resp = conn.delete_objects(Bucket=S3_BUCKET, Delete=delete_params)
    deleted = resp['Deleted'] if "Deleted" in resp else []
    errors = resp['Errors'] if 'Errors' in resp else []
    
    # make legible error messages, bundle them up
    for e in errors:
        with error_handler:
            raise S3DeletionException(
                f"Error trying to delete {e['Key']} version {e['VersionId']}: {e['Code']} - {e['Message']}"
            )
    if resp['ResponseMetadata']['HTTPStatusCode'] != 200:
        with error_handler:
            raise S3DeletionException(f"HTTP status code {resp['ResponseMetadata']['HTTPStatusCode']} from s3.delete_objects")
    if 'Deleted' not in resp:
        with error_handler:
            raise S3DeletionException("No Deleted key in response from s3.delete_objects")
    
    error_handler.raise_errors()
    return len(deleted)  # will always error above if empty, cannot return 0.


####################################################################################################


""" Research on getting a stream into the decryption code of pycryptodome

The StreamingBody StreamingBody object does not define the __len__ function, which is
necessary for creating a buffer somewhere in the decryption code, but it is possible to monkeypatch
it in like this:
    import botocore.response
    def monkeypatch_len(self):
        return int(self._content_length)
    botocore.response.StreamingBody.__len__ = monkeypatch_len

But that just results in this error from pycryptodome:
TypeError: Object type <class 'botocore.response.StreamingBody'> cannot be passed to C code """


####################################################################################################

# This is a class that caches encryption keys and study object ids to reduce database calls for
# those values, but it currently does not have tests.  And it is questionable if we should cache
# encryption keys without purging them.  But I couldn't help myself from prototyping it.


# class SmartKeyPath:

#     object_id_to_keys: dict[str, bytes] = {}
#     study_pk_to_keys: dict[int, bytes] = {}
#     study_pk_to_object_id: dict[int, str] = {}

#     both_fields = ("encryption_key", "object_id")

#     @classmethod
#     def populate(cls, obj: StrOrParticipantOrStudy) -> None:

#         if isinstance(obj, Participant):
#             cls._populate_participant(obj)  # query
#         elif isinstance(obj, Study):
#             cls._populate(obj.pk, obj.encryption_key.encode(), obj.object_id)  # we have it
#         elif isinstance(obj, str) and len(obj) == 24:
#             cls._populate_object_id_str(obj)  # query
#         else:
#             raise TypeError(f"expected Study, Participant, or 24 char str, received '{type(obj)}'")

#     ## Populate

#     @classmethod
#     def _populate(cls, study_id: int, key: bytes, study_object_id: str):
#         cls.study_pk_to_keys[study_id] = key
#         cls.object_id_to_keys[study_object_id] = key
#         cls.study_pk_to_object_id[study_id] = study_object_id

#     @classmethod
#     def _populate_participant(cls, p: Participant):
#         key, study_object_id = Study.fltr(pk=p.study_id).values_list(*cls.both_fields).get()
#         cls._populate(p.study_id, key.encode(), study_object_id)

#     @classmethod
#     def _populate_object_id_str(cls, s: str):
#         pk, key, study_object_id = Study.fltr(object_id=s).values_list("pk", *cls.both_fields).get()
#         cls._populate(pk, key.encode(), study_object_id)

#     ## getters

#     @classmethod
#     def smart_get_study_encryption_key(cls, obj: StrOrParticipantOrStudy) -> bytes:
#         # The easy one
#         if isinstance(obj, Study):
#             return obj.encryption_key.encode()

#         # The annoying ones
#         if isinstance(obj, Participant):
#             if obj.study_id not in cls.study_pk_to_keys:
#                 cls.populate(obj)
#             return cls.study_pk_to_keys[obj.study_id]

#         elif isinstance(obj, str) and len(obj) == 24:
#             if obj not in cls.object_id_to_keys:
#                 cls.populate(obj)
#             return cls.object_id_to_keys[obj]

#         else:
#             raise TypeError(SMART_GET_ERROR.format(type(obj)))

#     @classmethod
#     def s3_construct_study_key_path(cls, key_path: str, obj: StrOrParticipantOrStudy):
#         # The easy ones
#         if isinstance(obj, Study):
#             return obj.object_id + "/" + key_path
#         elif isinstance(obj, str) and len(obj) == 24:
#             return obj + "/" + key_path

#         # The annoying one
#         elif isinstance(obj, Participant):
#             if obj.study_id not in cls.study_pk_to_object_id:
#                 cls.populate(obj)
#             return cls.study_pk_to_object_id[obj.study_id] + "/" + key_path

#         else:
#             raise TypeError(SMART_GET_ERROR.format(type(obj)))

from __future__ import annotations

from typing import Generator, Sequence
from unittest.mock import MagicMock

import boto3
import zstd
from botocore.client import BaseClient
from cronutils import ErrorHandler
from Cryptodome.PublicKey import RSA

from config.settings import (BEIWE_SERVER_AWS_ACCESS_KEY_ID, BEIWE_SERVER_AWS_SECRET_ACCESS_KEY,
    S3_BUCKET, S3_ENDPOINT, S3_REGION_NAME)
from constants.common_constants import RUNNING_TESTS
from libs.aes import decrypt_server, encrypt_for_server
from libs.rsa import generate_key_pairing, get_RSA_cipher, prepare_X509_key_for_java


try:
    from libs.internal_types import StrOrParticipantOrStudy  # this is purely for ide assistance
except ImportError:
    pass


class NoSuchKeyException(Exception): pass
class S3DeletionException(Exception): pass
FULL_LOOP_DIDNT_REMOVE = "S3Compressed: full loop didn't remove file_content"
FULL_LOOP_DIDNT_CREATE = "S3Compressed: full loop didn't create compressed_data"
COMPRESSION_ALREADY_EXISTS = "S3Compressed: compressed_data already exists"
COMPRESSION_FILE_CONTENT_NOT_SET = "S3Compressed: file_content was not set before compression"
COMPRESSION_FILE_CONTENT_NONE = "S3Compressed: file_content was None at compression time"


conn: BaseClient = boto3.client(
    's3',
    aws_access_key_id=BEIWE_SERVER_AWS_ACCESS_KEY_ID,
    aws_secret_access_key=BEIWE_SERVER_AWS_SECRET_ACCESS_KEY,
    region_name=S3_REGION_NAME,
    endpoint_url=S3_ENDPOINT
)

if RUNNING_TESTS:                       # Nhis lets us cut out some boilerplate in tests
    S3_REGION_NAME = "us-east-1"        # Tests that need to mock S3 can mock the conn object
    S3_BUCKET = "test_bucket"
    conn = MagicMock()


def smart_get_study_encryption_key(obj: StrOrParticipantOrStudy) -> bytes:
    from database.models import Participant, Study  # circular imports
    
    if isinstance(obj, Participant):
        return obj.study.encryption_key.encode()
    elif isinstance(obj, Study):
        return obj.encryption_key.encode()
    elif isinstance(obj, str) and len(obj) == 24:
        return Study.objects.values_list("encryption_key", flat=True).get(object_id=obj).encode()
    else:
        raise TypeError(f"expected Study, Participant, or str, received '{type(obj)}'")


def s3_construct_study_key_path(key_path: str, obj: StrOrParticipantOrStudy):
    from database.study_models import Study
    from database.user_models_participant import Participant
    if isinstance(obj, Participant):
        study_object_id = obj.study.object_id
    elif isinstance(obj, Study):
        study_object_id = obj.object_id
    elif isinstance(obj, str) and len(obj) == 24:
        study_object_id = obj
    else:
        raise TypeError(f"expected Study, Participant, or 24 char str, received '{type(obj)}'")
    return study_object_id + "/" + key_path


class StorageManager:
    """ Class that handles encrypting, compressing, finding files on S3.  """
    
    bypass_study_folder: bool
    compressed_data: bytes
    file_content: bytes|None
    s3_path_raw: str 
    s3_path_zstd: str
    smart_key_obj: StrOrParticipantOrStudy
    
    def __init__(
        self, s3_path: str, obj: StrOrParticipantOrStudy, bypass_study_folder: bool = False
    ) -> None:
        if s3_path.endswith(".zstd"):
            raise ValueError("path should never end with .zstd")
        
        self.s3_path_raw = s3_path
        self.s3_path_zstd = s3_path + ".zstd"
        
        self.bypass_study_folder = bypass_study_folder
        self.smart_key_obj = obj
        
        self.file_content = None
        
        # only need for domain-specific compression.
        # self.s3_path_normalized = normalize_s3_file_path(s3_path)
    
    # Upload
    
    def push_to_storage(self):
        """ Compress local data, clear internal uncompressed state, send to S3 compressed. """
        if not hasattr(self, "compressed_data"):
            self.compress_data_and_clear()
        s3_upload(self.s3_path_zstd, self.compressed_data, self.smart_key_obj, raw_path=self.bypass_study_folder)
    
    def push_to_storage_and_clear(self):
        """ Compress local data, clear internal uncompressed state, send to S3 compressed, clear compressed state. """
        self.push_to_storage()
        del self.compressed_data
    
    ## Compression
    
    def compress_data_and_clear(self):
        self._do_compress()
        del self.file_content  # removes the uncompressed data from memory
    
    def _do_compress(self):
        assert not hasattr(self, "compressed_data"), COMPRESSION_ALREADY_EXISTS
        assert hasattr(self, "file_content"), COMPRESSION_FILE_CONTENT_NOT_SET
        assert self.file_content is not None, COMPRESSION_FILE_CONTENT_NONE
        
        self.compressed_data = zstd.compress(
            self.file_content,
            1,  # compression level (1 yields better compression on average across our data streams
            0,  # auto-tune the number of threads based on cpu cores (no apparent drawbacks)
        )
    
    ## Memory Management
    
    def clear(self):
        del self.file_content
        del self.compressed_data
    
    ## Download
    
    def download(self):
        try:
            self._download_as_compressed()
        except NoSuchKeyException:
            self._download_as_uncompressed_full_loop()
    
    def _download_as_compressed(self):
        raw_data = s3_retrieve(self.s3_path_zstd, self.smart_key_obj, self.bypass_study_folder)
        self.file_content = zstd.decompress(raw_data)
    
    def _download_as_uncompressed_full_loop(self):
        """ Force file to be stored compressed, handle internal and externalstate so it just works. """
        
        raw_data = s3_retrieve(self.s3_path_raw, self.smart_key_obj, self.bypass_study_folder)
        self.file_content = raw_data
        self.push_to_storage()  # compresses
        
        assert not hasattr(self, "file_content"), FULL_LOOP_DIDNT_REMOVE
        assert hasattr(self, "compressed_data"), FULL_LOOP_DIDNT_CREATE
        
        s3_delete(self.s3_path_raw)
        del self.compressed_data  # remove the compressed data from memory
        self.file_content = raw_data


## MetaData


def s3_get_size(key_path: str):
    return conn.head_object(Bucket=S3_BUCKET, Key=key_path)["ContentLength"]


## Upload


def s3_upload(
        key_path: str, data_string: bytes, obj: StrOrParticipantOrStudy, raw_path=False
) -> None:
    """ Uploads a bytes object as a file, encrypted using the encryption key of the study it is
    associated with. Intelligently accepts a string, Participant, or Study object as needed. """
    
    if not raw_path:
        key_path = s3_construct_study_key_path(key_path, obj)
    
    data = encrypt_for_server(data_string, smart_get_study_encryption_key(obj))
    _do_upload(key_path, data)


def s3_upload_plaintext(upload_path: str, data_string: bytes) -> None:
    """ Extremely simple, uploads a file (bytes object) to s3 without any encryption. """
    conn.put_object(Body=data_string, Bucket=S3_BUCKET, Key=upload_path)


def _do_upload(key_path: str, data_string: bytes, number_retries=3):
    """ In ~April 2022 this api call started occasionally failing, so wrapping it in a retry. """
    
    try:
        conn.put_object(Body=data_string, Bucket=S3_BUCKET, Key=key_path)
    except Exception as e:
        if "Please try again" not in str(e):
            raise
        _do_upload(key_path, data_string, number_retries=number_retries - 1)


## Download


def s3_retrieve(key_path: str, obj: StrOrParticipantOrStudy, raw_path: bool = False, number_retries=3) -> bytes:
    """ Takes an S3 file path (key_path), and a study ID.  Takes an optional argument, raw_path,
    which defaults to false.  When set to false the path is prepended to place the file in the
    appropriate study_id folder. """
    if not raw_path:
        key_path = s3_construct_study_key_path(key_path, obj)
    encrypted_data = _do_retrieve(S3_BUCKET, key_path, number_retries=number_retries)['Body'].read()
    return decrypt_server(encrypted_data, smart_get_study_encryption_key(obj))


def s3_retrieve_plaintext(key_path: str, number_retries=3) -> bytes:
    """ Retrieves a file as-is as bytes. """
    return _do_retrieve(S3_BUCKET, key_path, number_retries=number_retries)['Body'].read()


def _do_retrieve(bucket_name: str, key_path: str, number_retries=3):
    """ Run-logic to do a data retrieval for a file in an S3 bucket."""
    try:
        return conn.get_object(Bucket=bucket_name, Key=key_path, ResponseContentType='string')
    except Exception as boto_error_unknowable_type:
        # Some error types cannot be imported because they are generated at runtime through a factory
        if boto_error_unknowable_type.__class__.__name__ == "NoSuchKey":
            raise NoSuchKeyException(f"{bucket_name}: {key_path}")
        # usually we want to try again
        if number_retries > 0:
            print("s3_retrieve failed, retrying on %s" % key_path)
            return _do_retrieve(bucket_name, key_path, number_retries=number_retries - 1)
        # unknown cases: explode.
        raise


## List Files Matching Prefix

def s3_list_files(prefix: str, as_generator=False) -> list[str]|Generator[str]:
    """ Lists s3 keys matching prefix. as generator returns a generator instead of a list.
    WARNING: passing in an empty string can be dangerous. """
    return _do_list_files(S3_BUCKET, prefix, as_generator=as_generator)


def smart_s3_list_study_files(prefix: str, obj: StrOrParticipantOrStudy):
    """ Lists s3 keys matching prefix, autoinserting the study object id at start of key path. """
    return s3_list_files(s3_construct_study_key_path(prefix, obj))


def _do_list_files(bucket_name: str, prefix: str, as_generator=False) -> list[str]|Generator[str]:
    paginator = conn.get_paginator('list_objects_v2')
    
    page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=prefix)
    if as_generator:
        return _do_list_files_generator(page_iterator)  # type: ignore
    
    items = []
    for page in page_iterator:
        if 'Contents' in page:
            for item in page['Contents']:
                items.append(item['Key'].strip("/"))
    return items


def _do_list_files_generator(page_iterator: Sequence) -> Generator[str]:
    for page in page_iterator:
        if 'Contents' not in page:
            return
        for item in page['Contents']:
            yield item['Key'].strip("/")


def s3_list_versions(prefix: str) -> Generator[tuple[str, str|None], None, None]:
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


def s3_delete(key_path: str) -> bool:
    resp = conn.delete_object(Bucket=S3_BUCKET, Key=key_path)
    if not resp["DeleteMarker"]:
        raise S3DeletionException(f"Failed to delete {resp['Key']} version {resp['VersionId']}")
    return resp["DeleteMarker"]


def s3_delete_versioned(key_path: str, version_id: str) -> bool:
    resp = conn.delete_object(Bucket=S3_BUCKET, Key=key_path, VersionId=version_id)
    if not resp["DeleteMarker"]:
        raise S3DeletionException(f"Failed to delete {resp['Key']} version {resp['VersionId']}")
    return resp["DeleteMarker"]


def s3_delete_many_versioned(paths_version_ids: list[tuple[str, str]]):
    """ Takes a list of (key_path, version_id) tuples and deletes them all using the boto3
    delete_objects API.  Returns the number of files deleted, raises errors with reasonable
    clarity inside an errorhandler bundled error. """
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


#TODO: move to security_utils.py, fix circular imports

################################################################################
######################### Client Key Management ################################
################################################################################


def create_client_key_pair(patient_id: str, study_id: str):
    """Generate key pairing, push to database, return sanitized key for client."""
    public, private = generate_key_pairing()
    s3_upload("keys/" + patient_id + "_private", private, study_id)
    s3_upload("keys/" + patient_id + "_public", public, study_id)


def get_client_public_key_string(patient_id: str, study_id: str) -> str:
    """Grabs a user's public key string from s3."""
    key_string = s3_retrieve("keys/" + patient_id + "_public", study_id)
    return prepare_X509_key_for_java(key_string).decode()


def get_client_public_key(patient_id: str, study_id: str) -> RSA.RsaKey:
    """Grabs a user's public key file from s3."""
    key = s3_retrieve("keys/" + patient_id + "_public", study_id)
    return get_RSA_cipher(key)


def get_client_private_key(patient_id: str, study_id: str) -> RSA.RsaKey:
    """Grabs a user's private key file from s3."""
    key = s3_retrieve("keys/" + patient_id + "_private", study_id)
    return get_RSA_cipher(key)


###############################################################################
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

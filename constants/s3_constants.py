from typing import Any, Protocol


# Debugging / correctness checks
UNCOMPRESSED_DATA_MISSING_AT_COMPRESSION = "uncompressed_data not set before compression"
COMPRESSED_DATA_PRESENT_AT_COMPRESSION = "compressed_data was set before compression"

UNCOMPRESSED_DATA_MISSING_ON_POP = "uncompressed_data was missing before pop"
COMPRESSED_DATA_MISSING_ON_POP = "compressed_data was missing before pop"

COMPRESSED_DATA_MISSING_AT_UPLOAD = "compressed_data was misssing before upload"
UNCOMPRESSED_DATA_PRESENT_WRONG_AT_UPLOAD = "uncompressed_data was present before upload (when it shouldn't)"

UNCOMPRESSED_DATA_PRESENT_ON_DOWNLOAD = "uncompressed_data present on download"
COMPRESSED_DATA_PRESENT_ON_DOWNLOAD = "compressed_data present on download"

COMPRESSED_DATA_PRESENT_ON_ASSIGNMENT = "compressed_data was present on assignment"
UNCOMPRESSED_DATA_PRESENT_ON_ASSIGNMENT = "uncompressed_data was present on assignment"

# error messages
MUST_BE_ZSTD_FORMAT = lambda file_content: "Must be compressed zstd data conforming to the zstd format. data started with {file_content}"
BAD_FOLDER = "Files in the {path_start} folder should not use the S3Storage class. full path: {path}"
BAD_FOLDER_2 = "Unrecognized base folder: `{path_start}` in path: `{path}`"
SMART_GET_ERROR = "expected Study, Participant, or 24 char str, received '{}'"


# Error classes
class NoSuchKeyException(Exception): pass
class S3DeletionException(Exception): pass
class BadS3PathException(Exception): pass
class IOSDataRecoveryDisabledException(Exception): pass


## Types
# Boto3 doesn't have accessible type hints
class Readable(Protocol):
    def read(self) -> bytes: ...

Boto3Response = dict[str, Readable]


# S3 Storage validation and logging

METADATA_FIELDS = {
    "last_updated",
    
    "size_compressed",
    "size_uncompressed",
    "compression_time_ns",
    "decompression_time_ns",
    "encryption_time_ns",
    "download_time_ns",
    "upload_time_ns",
    "decrypt_time_ns",
    "sha1",
    
    "participant_id",
    "study_id",
}

class MetaDotDict(dict):
    def __getattr__(self, item):
        return self[item]
    
    def __setattr__(self, name: str, value: Any):
        self[name] = value
    
    def __setitem__(self, key: Any, value: Any) -> None:
        if key not in METADATA_FIELDS:
            raise AttributeError(f"'{key}' is not a validated metadata field")
        return super().__setitem__(key, value)

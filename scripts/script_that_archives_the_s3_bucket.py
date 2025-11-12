from collections.abc import Generator
from io import BytesIO
from multiprocessing.pool import ThreadPool
from zipfile import ZIP_STORED, ZipFile

import boto3
import botocore
from botocore.exceptions import ClientError as Boto3ClientError


"""
This script will download every .zst file from the "old" s3 bucket, wrap them up into zip files and
upload those zips to a different bucket with using the Glacient Instance Retrieval storage class.

.zst files will contain all data, provided your study was started after roughly may 2025, or have
run the script_that_compresses_s3_data.py script.

You might need to adjust the region_name in the s3 client declarations below.

This script does not delete files.

Zip files will be filled until they are at least 500MB in size. This is like a 10,000x reduction.

These zip files do not apply compression, they are used to wrap many files into one for reduced S3
costs and ease of future retrieval.

This script is mostly limited in speed by the small size of the source files.  It is multithreaded.
"""


# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
# If something goes wrong and it doesn't finish you can restart the process by setting this to the
# name of the last "first file path" that was listed in the output of the log file.
# Set the START_NUMBER to the number of the last zip file that was _completed_, or just a higher
# number if you want an obvious gap.
START_PATH = ""
START_NUMBER = 0
# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!


# Edit this script by populating these values and commenting out this lines.
# e = "You did not look at the scripts/script_that_archives_the_s3_bucket.py file, please set the configuration values!"
# print("\n\n\n", e, "\n\n\n")
# raise Exception(e)

# the names of the buckets - YOU MAY NEED TO UPDATE THE REGIEN NAMES IN THE S3 CLIENT DECLARATIONS BELOW
OLD_BUCKET = ""
NEW_BUCKET = ""

OLD_BUCKET_ACCESS_KEY = ""
OLD_BUCKET_SECRET_KEY = ""
NEW_BUCKET_ACCESS_KEY = ""
NEW_BUCKET_SECRET_KEY = ""


# Set the storage class for your new bucket objects.
# valid values at time of writing are STANDARD REDUCED_REDUNDANCY STANDARD_IA ONEZONE_IA
# INTELLIGENT_TIERING GLACIER DEEP_ARCHIVE OUTPOSTS GLACIER_IR SNOW EXPRESS_ONEZONE FSX_OPENZFS
STORAGE_CLASS = "GLACIER_IR"


## S3 Clients
old_s3 = boto3.client(
    's3',
    aws_access_key_id=OLD_BUCKET_ACCESS_KEY,
    aws_secret_access_key=OLD_BUCKET_SECRET_KEY,
    region_name="us-east-1",
    config=botocore.config.Config(
        max_pool_connections=100,
        tcp_keepalive=True,
        disable_request_compression=True,  # we already compress AND encrypt...
    ),
)
new_s3 = boto3.client(
    's3',
    aws_access_key_id=NEW_BUCKET_ACCESS_KEY,
    aws_secret_access_key=NEW_BUCKET_SECRET_KEY,
    region_name="us-east-1",
)


## this is a very quick little code snippet to get the size and number of files in the new bucket.
## (metrics on s3 console update very slowly)
# s = []
# count = 0
# for p in new_s3.get_paginator('list_objects_v2').paginate(Bucket=NEW_BUCKET, Prefix=""):
#     for d in p["Contents"]:
#         s.append(d['Size'])
#         count += 1
# print(count, "files")
# print(sum(s) / 1024/1024/1024, "GB")


#
## End configuration
#

class StreamingBytesIO(BytesIO):
    """ class comes from libs/streaming_io.py, necessary for the zip creation """
    _position = 0
    
    def empty(self):
        """ Clears the BytesIO object while retaining the current virtual position """
        self._position = self.tell()
        # order does not matter for truncate and seek
        self.truncate(0)
        super(StreamingBytesIO, self).seek(0)
    
    def tell(self) -> int:
        """ Returns the current stream's virtual position (where the stream would be if it had
        been running contiguously and self.empty() is not called) """
        return self._position + super(StreamingBytesIO, self).tell()
    
    def seek(self, *args, **kwargs) -> int:
        """ Sets the position explicitly, required for compatibility with Python 3 Zipfile """
        self._position = args[0]
        return super(StreamingBytesIO, self).seek(0)


def paginate_file_names() -> Generator[list[str], None, None]:
    paginator = old_s3.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(
        Bucket=OLD_BUCKET, Prefix="", **{"StartAfter": START_PATH} if START_PATH else {}
    )
    
    for page in page_iterator:
        if 'Contents' not in list(page.keys()):
            return
        # if it is not a .zst file we do not care about it.
        yield [item['Key'] for item in page['Contents'] if item['Key'].endswith('.zst')]


def upload(upload_path: str, data_string: bytes) -> None:
    new_s3.put_object(Body=data_string, Bucket=NEW_BUCKET, Key=upload_path, StorageClass=STORAGE_CLASS)


def download(key_path: str, number_retries=3) -> bytes:
    try:
        return old_s3.get_object(Bucket=OLD_BUCKET, Key=key_path, ResponseContentType='string')["Body"].read()
    except Boto3ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            raise
        
        if number_retries > 0:
            print(f"s3_retrieve failed, retrying on `{key_path}`")
            return download(key_path, number_retries=number_retries - 1)
        
        raise


def down_func(path: str) -> tuple[str, bytes]:
    return path, download(path)


def get_zip_file_name(zip_file_number: int, first_file: str) -> str:
    zip_file_number_str = str(zip_file_number).zfill(5)
    
    # run a sanity check on the file structure
    suffix = first_file.split("/")[:2]  # type: ignore
    assert len(suffix) == 2, f"Unexpected file path structure: {first_file}"  # type: ignore
    
    # shows some info about contents
    # example: backup_part_0001_6h5DkqQyZ4BQ3Z3lqvY6awuO-df765q8e.zip
    return f"backup_part_{zip_file_number_str}_{'-'.join(suffix)}.zip"


def download_files_to_zip(
    pool: ThreadPool, file_names: list[str], zip_input: ZipFile, zip_output: StreamingBytesIO
) -> list[bytes]:
    """ Downloads files and literally adds them directly to the zip file.  Returns a list of bytes
    that when combined make up the zip file. (This wasn't quite the original intent but it works?) """
    
    all_data = list[bytes]()
    # threaded download - files in the zip retain their full path from S3 bucket prepended by "backup"
    print(f"downloading {len(file_names)} files...", end="", flush=True)
    for file_path, file_data in pool.imap_unordered(down_func, file_names):
        zip_input.writestr(f"backup/{file_path}", file_data)
        all_data.append(zip_output.getvalue())
        
        del file_path, file_data  # explicit free memory....
        zip_output.empty()  # free memory used by zip output buffer
    print(f"done. {sum(len(_) for _ in all_data) / 1024 / 1024:.2f} MB downloaded.")
    
    return all_data


def main():
    pool = ThreadPool(50)
    zip_file_number = START_NUMBER
    
    # this gets overwritten immediately, it's just making the IDE happy.
    final_file_name = ""  # overwritten immediately
    all_zip_segments = list[bytes]()  # overwritten immediately
    zip_output = StreamingBytesIO()  # overwritten immediately
    zip_input = ZipFile(zip_output, mode="w", compression=ZIP_STORED, allowZip64=True)  # overwritten immediately
    zip_input.close()
    zip_output.close()
    
    
    TIME_FOR_NEW_ZIP_FILE = True
    
    for file_names in paginate_file_names():
        if TIME_FOR_NEW_ZIP_FILE:
            zip_file_number += 1
            final_file_name = get_zip_file_name(zip_file_number, file_names[0])
            
            print(f"Starting batch {zip_file_number}, first file in this batch:", file_names[0])
            print(f"final file name: `{final_file_name}`")
            
            all_zip_segments = list[bytes]()
            
            zip_output = StreamingBytesIO()
            zip_input = ZipFile(zip_output, mode="w", compression=ZIP_STORED, allowZip64=True)
            TIME_FOR_NEW_ZIP_FILE = False
        
        # download into all data
        all_zip_segments.extend(download_files_to_zip(pool, file_names, zip_input, zip_output))
        
        if sum(len(_) for _ in all_zip_segments) < (500 * 1024 * 1024):  # 500 MB
            continue  # continue adding into this zip file until we hit the size limit
        
        TIME_FOR_NEW_ZIP_FILE = True
        
        # finalize current zip file
        zip_input.close()
        all_zip_segments.append(zip_output.getvalue())
        zip_output.close()
        
        # create zip, clear memory - not thrilled with the 2x memory usage here, was hoping to avoid
        the_zip_file = b"".join(all_zip_segments)
        all_zip_segments = []  # explicit free memory?
        
        # upload and log
        size = len(the_zip_file) / 1024 / 1024  # size in MB
        final_number = f"{size:.2f}"
        print(f"Uploading {final_file_name}, size: {final_number}MB\n")
        
        upload(final_file_name, the_zip_file)
    
    # on the last iteration it is very unlikely that we ended exactly on a zip file boundary, so...
    if not TIME_FOR_NEW_ZIP_FILE:
        # finalize current zip file
        zip_input.close()
        all_zip_segments.append(zip_output.getvalue())
        zip_output.close()
        
        # create zip, clear memory - not thrilled with the 2x memory usage here, was hoping to avoid
        the_zip_file = b"".join(all_zip_segments)
        all_zip_segments = []  # explicit free memory?
        
        # upload and log
        size = len(the_zip_file) / 1024 / 1024  # size in MB
        final_number = f"{size:.2f}"
        print(f"Uploading {final_file_name}, size: {final_number}MB\n")
        
        upload(final_file_name, the_zip_file)
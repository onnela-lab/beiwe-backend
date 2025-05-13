from multiprocessing.pool import ThreadPool

import orjson

from constants.common_constants import (CHUNKS_FOLDER, CUSTOM_ONDEPLOY_PREFIX, LOGS_FOLDER,
    PROBLEM_UPLOADS)
from database.models import S3File, Study
from libs.s3 import (BadS3PathException, s3_delete, s3_list_files, s3_retrieve_plaintext,
    s3_upload_plaintext, S3Storage)
from libs.utils.compression import compress
from libs.utils.http_utils import numformat


#
## If you need to restart the full script add a parameter here.  Provide a file path that is printed
## in the output and the compression of all files will restart from there. (it takes on the order of
## 100ms to query-and-receive a single page of 1,000 files, plus substantial execution time.)
#
START_GLOBAL_FILE_SEARCH_HERE = None


"""
Performance characteristics:

- Even on a server with multiple cores we are seeing 150-300% cpu with 25 threads, it doesn change
  much at 50 threads.  When just iterating over files it hovers at 30-50% cpu.
- Files happen to be iterated over by data stream type, so the vacillation of the cpu usage is
  probably because this tends to clump files together with larger or smaller sizes. Bigger=faster.
- There could be literally hundreds of millions of files.  We are talking weeks, if not months, for
  large servers.  Onnela lab had over 600 million files. (This script takes so long that we have to
  handle the case of studies that did not exist when it started.)
- Memory usage is unusual, it might be that either zstd or compression reserves memory? I see 10-14%
  of the 16GB total system memory on a long running compression job.
"""

# the "logs/" folder is the s3 access logging folder, it is enabled in the s3 bucket settings
VALID_JUNK_FOLDERS = (LOGS_FOLDER, PROBLEM_UPLOADS, CUSTOM_ONDEPLOY_PREFIX, "logs")

THREAD_POOL_SIZE = 50

#
## logging sorta-kinda
#

class stats:
    number_paths_total = 0
    number_already_compressed = 0
    number_of_skipped_files = 0
    number_files_failed_with_error = 0
    number_files_compressed = 0
    number_files_failed_bad_path = 0
    
    @classmethod
    def stats(cls):
        print()
        print("number_paths_total:", numformat(cls.number_paths_total))
        print("number_already_compressed:", numformat(cls.number_already_compressed))
        print("number_of_skipped_files:", numformat(cls.number_of_skipped_files))
        print("number_files_compressed:", numformat(cls.number_files_compressed))
        print("number_files_failed_bad_path:", numformat(cls.number_files_failed_bad_path))
        print("number_files_failed_with_error:", numformat(cls.number_files_failed_with_error))
        print()


def compress_file(path_study):
    # download forcing compression
    path, study = path_study
    try:
        # if we do S3Storage._download_and_rewrite_s3_as_compressed it doesn't it skips an S3 query
        s = S3Storage(path, study, bypass_study_folder=True)
        s._download_and_rewrite_s3_as_compressed_retaining_uncompressed()
        s.pop_uncompressed_file_content()  # manual memory management to maybe reduce cycles in gc?
        stats.number_files_compressed += 1
    except BadS3PathException as e:
        print("bad s3 path:", e)
        stats.number_files_failed_bad_path += 1
    except Exception as e:
        print(f"uhoh, encountered an `{e}` on {path}.")
        stats.number_files_failed_with_error += 1


# we can bypass a whole database query by having the study to hand
ALL_STUDIES = {}
NOT_A_STUDY = set()

#
##  High level .... stuff
#

def main():
    """ This is called when run through the run_script.sh script. """
    # go through every study file (encrypted file) in the s3 bucket and compress it.
    compress_study_files_matching_prefix("", START_GLOBAL_FILE_SEARCH_HERE)


def compress_a_study(study_object_id_or_study: str|Study):
    if isinstance(study_object_id_or_study, str):
        study_object_id = study_object_id_or_study
        if len(study_object_id) != 24:
            raise ValueError("compress_a_study requires a study_object_id or a Study object")
    elif isinstance(study_object_id_or_study, Study):
        study_object_id = study_object_id_or_study.object_id
    else:
        raise TypeError("compress_a_study requires a study_object_id or a Study object")
    
    compress_study_files_matching_prefix(study_object_id)
    compress_study_files_matching_prefix(CHUNKS_FOLDER + "/" + study_object_id)


#
## utility functions
#

# we can save on database queries by caching the study object and keys that are invalid
def get_update_study(study_object_id: str) -> Study|None:
    if study_object_id in NOT_A_STUDY:  # this is a study that we know does not exist
        return None
    
    study = ALL_STUDIES.get(study_object_id)
    if not study:
        try:
            study = Study.objects.get(object_id=study_object_id)
            ALL_STUDIES[study_object_id] = study
        except Study.DoesNotExist:
            NOT_A_STUDY.add(study_object_id)  # it doesn't exist
            return None
    return study


def retrieve_s3_log_file(path: str) -> tuple[str,str]:
    return path, s3_retrieve_plaintext(path).decode()


def batch_unencrypted_compress_S3File(path: str):
    try:
        log_data = s3_retrieve_plaintext(path)
        size_log_data = len(log_data)
        compressed_data = compress(log_data)
        del log_data
        size_compressed_data = len(compressed_data)
        s3_upload_plaintext(path + ".zst", compressed_data)
        s3_delete(path)
        stats.number_files_compressed += 1
        S3File.objects.create(
            path=path,
            size_uncompressed=size_log_data,
            size_compressed=size_compressed_data,
        )
    except Exception as e:
        print(f"uhoh, encountered an `{e}` on `{path}`.")
        stats.number_files_failed_with_error += 1


#
## Functions that iterate over the s3 files
#


def compress_study_files_matching_prefix(prefix: str, start_at: str = None):
    """ compresses files in the CHUNKS_FOLDER that are uploaded from the chunks folder """
    pool = ThreadPool(THREAD_POOL_SIZE)
    compression_args = []
    
    for path in s3_list_files(prefix, as_generator=True, start_at=start_at):
        if stats.number_paths_total % 100_000 == 0:  # 10,000 was too frequent..
            print(f'file {numformat(stats.number_paths_total)} for your reference: "{path}"')
            stats.stats()
        
        stats.number_paths_total += 1
        
        # file already compressed
        if path.endswith(".zst"):
            stats.number_already_compressed += 1
            continue
        
        path_start = path.split("/", 1)[0]
        
        # illegal folders
        if path_start in VALID_JUNK_FOLDERS:
            # print(f"skipping '{path}'")
            stats.number_of_skipped_files += 1
            continue
        
        # participant folders - study_object_id/patient_id - exist and are empty.
        if path.count("/") == 1:
            stats.number_of_skipped_files += 1
            continue
        
        # get study object id in the chunks folder or participant folder
        study_object_id = path.split("/")[1] if path_start == CHUNKS_FOLDER else path_start
        
        # for unrecognized studies (root folders), we stash those prefixes to print them
        # later on to review them. We have a script script_that_deletes_data_from_unknown_studies
        # to delete these.
        study = get_update_study(study_object_id)  # passing in a study means no inner db query
        if not study:
            stats.number_of_skipped_files += 1
            continue
        
        # its a file in a study folder, so we can compress it and create our db entry.
        compression_args.append((path, study,))
        if len(compression_args) >= 10_000:
            list(  # just a fast iterate
                pool.imap_unordered(compress_file, compression_args, chunksize=1)
            )
            print(f'Most recent compressed file for your reference: "{path}"')
            compression_args = []
    
    if compression_args:
        list(  # just a fast iterate
            pool.imap_unordered(compress_file, compression_args, chunksize=1)
        )
    stats.stats()
    
    del compression_args
    
    pool.close()
    pool.terminate()
    
    if NOT_A_STUDY:
        print("discovered these invalid root folders, you should run the script_that_deletes_data_from_unknown_studies:")
        print(NOT_A_STUDY)


def compress_logs():
    """ Compresses files in the "LOGS/" folder that are uploaded from the data processing servers.
    This only needs to be run once ever on older servers. Usually takes a minute or two. """
    
    args = []
    pool = ThreadPool(THREAD_POOL_SIZE)
    
    for path in s3_list_files(LOGS_FOLDER, as_generator=True):
        stats.number_paths_total += 1
        
        if path.endswith(".zst"):
            continue
        
        args.append(path)
        if len(args) >= 10_000:
            list(  # just a fast iterate
                pool.imap_unordered(batch_unencrypted_compress_S3File, args, chunksize=1)
            )
            args = []
            stats.stats()
    
    if args:
        list(  # just a fast iterate
            pool.imap_unordered(batch_unencrypted_compress_S3File, args, chunksize=1)
        )
    stats.stats()
    
    pool.close()
    pool.terminate()


def compress_s3_logging_logs():
    """ Compresses files in the "logs/" folder that is created by the s3 access logging setting.
    
    On the Onnela Lab production at time of development we generated just under 500k per day.
    
    This batches together 10,000 files at a time, and then uploads them to the logs/compressed.
    achieves ~14x compression on the logs. You probably have millions of these files if you have any
    at all.  These files are generated automatically by the s3 bucket settings, they are not enabled
    by default. """
    
    paths = list[str]()
    pool = ThreadPool(THREAD_POOL_SIZE)
    
    def concatenate_s3_log_files():
        # Concatenates the log files into a json dict, compress, upload, then delete the originals.
        # names in this folder contain some date information, they will look like:
        # logs/compressed/logs/2016-09-27-23-34-26-69B50546FD03CF06-logs/2016-09-29-01-40-59-409F9A199BFDBAEC.zst
        #    aaaaand just realized that has extra / in it well ... it doesn't matter nobady cares...
        
        start, end = paths[0], paths[-1]
        print("downloading")
        data = dict(pool.imap_unordered(retrieve_s3_log_file, paths, chunksize=1))
        json_repr = orjson.dumps(data)
        name = f"logs/compressed/{start}-{end}.zst"
        print("uploading", name)
        s3_upload_plaintext(name, compress(json_repr, level=4))
        print("deleting")
        
        # TODO: this can be changed to the versioned delete-many boto3 api outside the thread pool,
        #  but that is.... hard to debug because it requires the version id and I needed a way to
        #  distinguish already deleted files and can't be bothered to work that out right now.
        list(pool.imap_unordered(s3_delete, paths, chunksize=1))
        print("done deleting\n")
    
    
    # go through paths, skip obviously wrong files, create a list, dispatch to compress
    # logs have a name starting with the isodate, a prefix of logs/2 skips compressed files.
    for number_paths_total, path in enumerate(s3_list_files("logs/2", as_generator=True)):
        
        paths.append(path)
        if len(paths) >= 10_000:
            print("number_paths_total:", numformat(number_paths_total))
            concatenate_s3_log_files()
            paths = []
    
    if paths:
        concatenate_s3_log_files()
        paths = []  # reset
    
    pool.close()
    pool.terminate()

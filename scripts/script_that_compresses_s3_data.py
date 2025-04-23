from multiprocessing.pool import ThreadPool

from constants.common_constants import (CHUNKS_FOLDER, CUSTOM_ONDEPLOY_PREFIX, LOGS_FOLDER,
    PROBLEM_UPLOADS)
from database.models import S3File, Study
from libs.s3 import (BadS3PathException, s3_delete, s3_list_files, s3_retrieve,
    s3_retrieve_plaintext, s3_upload_plaintext)
from libs.utils.compression import compress
from libs.utils.http_utils import numformat


"""
Performance characteristics:
- even on a server with multiple cores we are seeing a limit of 150% cpu with 25 threads
- sometimes it does use all available cpu, so this is probably an artifact of iterating over
  many small files. Maybe.
- There could be literally hundreds of millions of files.  This script takes a while to run.
- This script takes so long that we have to handle studies that did not exist at the start of the script.
"""

VALID_JUNK_FOLDERS = (LOGS_FOLDER, PROBLEM_UPLOADS, CUSTOM_ONDEPLOY_PREFIX)

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
    path, study = path_study
    # download forcing compression
    try:
        s3_retrieve(path, study, raw_path=True)
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


def main():
    pool = ThreadPool(25)
    compression_args = []
    
    for path in s3_list_files("", as_generator=True):
        if stats.number_paths_total % 100_000 == 0:  # MANY FILES.
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
        
        # participant folders - study_object_id/patient_id or study_object_id/patient_id.zst
        # there is some difficulty with these files, but they are tiny so we can skip them
        if path.count("/") == 1:
            stats.number_of_skipped_files += 1
            continue
        
        # get study object id in the chunks folder or participant folder
        study_object_id = path.split("/")[1] if path_start == CHUNKS_FOLDER else path_start
        
        # for unrecognized studies (root folders), we stash those prefixes to print them
        # later on to review them. We have a script script_that_deletes_data_from_unknown_studies
        # to delete these.
        study = get_update_study(study_object_id)
        if not study:
            stats.number_of_skipped_files += 1
            continue
        
        # its a file in a study folder, so we can compress it and create our db entry.
        compression_args.append((path, study,))
        if len(compression_args) >= 10_000:
            list(  # just a fast iterate
                pool.imap_unordered(compress_file, compression_args, chunksize=1)
            )
            compression_args = []
    
    if compression_args:
        list(  # just a fast iterate
            pool.imap_unordered(compress_file, compression_args, chunksize=1)
        )
    stats.stats()
    
    del compression_args
    
    pool.close()
    pool.terminate()
    print("discovered these invalid root folders, you should run the script_that_deletes_data_from_unknown_studies:")
    print(NOT_A_STUDY)


def batch_compress_log_file(path: str):
    
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


def compress_logs():
    args = []
    pool = ThreadPool(25)
    
    for path in s3_list_files(LOGS_FOLDER, as_generator=True):
        stats.number_paths_total += 1
        
        if path.endswith(".zst"):
            continue
        
        args.append(path)
        if len(args) >= 10_000:
            list(  # just a fast iterate
                pool.imap_unordered(batch_compress_log_file, args, chunksize=1)
            )
            args = []
            stats.stats()
    
    if args:
        list(  # just a fast iterate
            pool.imap_unordered(batch_compress_log_file, args, chunksize=1)
        )
    stats.stats()
    
    pool.close()
    pool.terminate()
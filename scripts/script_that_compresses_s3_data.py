from multiprocessing.pool import ThreadPool

from constants.common_constants import CHUNKS_FOLDER, CUSTOM_ONDEPLOY_PREFIX, PROBLEM_UPLOADS
from database.models import Study
from libs.s3 import BadS3PathException, s3_list_files, s3_retrieve
from libs.utils.http_utils import numformat


# we can bypass a whole database query by having the study to hand
encryption_keys = {s.object_id: s for s in Study.objects.all()}

class stats:
    number_paths_total = 0
    number_already_compressed = 0
    number_participant_folders = 0
    number_illegal_folders = 0
    number_files_failed = 0
    number_files_compressed = 0
    number_bad_paths = 0
    
    @classmethod
    def stats(cls):
        print()
        print("number_paths_total:", numformat(cls.number_paths_total))
        print("number_already_compressed:", numformat(cls.number_already_compressed))
        print("number_participant_folders:", numformat(cls.number_participant_folders))
        print("number_illegal_folders:", numformat(cls.number_illegal_folders))
        print("number_files_failed:", numformat(cls.number_files_failed))
        print("number_files_compressed:", numformat(cls.number_files_compressed))
        print()


# performance characteristics:
# - even on a server with more cores we are seeing a limit of 150% cpu with 25 threads
# - sometimes it does use all available cpu, so this is probably an artifact of iterating over
#   many small files.
pool = ThreadPool(25)

illegal_folders = set()

def compress_file(path_study):
    path, study = path_study
    # download forcing compression
    try:
        x = s3_retrieve(path, study, raw_path=True)
        del x
        stats.number_files_compressed += 1
    except BadS3PathException as e:
        print(e)
        stats.number_bad_paths += 1
    except Exception as e:
        print(f"uhoh, encountered an `{e}` on {path}.")
        stats.number_files_failed += 1


the_args = []
for path in s3_list_files("", as_generator=True):
    
    stats.number_paths_total += 1
    
    # file already compressed
    if path.endswith(".zst"):
        stats.number_already_compressed += 1
        continue
    
    # participant folders - study_object_id/patient_id
    if path.count("/") == 1:
        print(f"skipping '{path}'")
        stats.number_participant_folders += 1
        continue
    
    path_start = path.split("/", 1)[0]
    
    # illegal folders
    if path_start in (PROBLEM_UPLOADS, CUSTOM_ONDEPLOY_PREFIX):
        print(f"skipping '{path}'")
        stats.number_illegal_folders += 1
        continue
    
    # get study object id in the chunks folder
    study_object_id = path.split("/")[1] if path_start == CHUNKS_FOLDER else path_start
    
    # we don't want to stop on unknown folders, we want to stash those prefixes to print them later
    # on to review them
    study = encryption_keys.get(study_object_id)
    if not study:
        print(f"skipping '{path}'")
        stats.number_illegal_folders += 1
        continue
    
    the_args.append((path, study,))
    
    if len(the_args) >= 10_000:
        # print(the_args)
        list(  # just a fast iterate
            pool.imap_unordered(compress_file, the_args, chunksize=1)
        )
        the_args = []
        stats.stats()

stats.stats()

print()
print("illegal folders:")
print(illegal_folders)
print()
# nohup time python -u run_script.py script_that_compresses_s3_data > compression.log &

from multiprocessing.pool import ThreadPool
from constants.common_constants import CHUNKS_FOLDER, CUSTOM_ONDEPLOY_PREFIX, PROBLEM_UPLOADS
from database.models import Study
from libs.s3 import BadS3PathException, s3_retrieve, s3_list_files


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
        print("number_paths_total:", cls.number_paths_total)
        print("number_already_compressed:", cls.number_already_compressed)
        print("number_participant_folders:", cls.number_participant_folders)
        print("number_illegal_folders:", cls.number_illegal_folders)
        print("number_files_failed:", cls.number_files_failed)
        print("number_files_compressed:", cls.number_files_compressed)
        print()



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
    
    if stats.number_paths_total % 1000 == 0:
        stats.stats()
    
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
    if path_start == CHUNKS_FOLDER:
        study_object_id = path.split("/")[1]
    else:
        study_object_id = path_start
    
    # we don't want to stop on unknown folders, we want to stash those prefixes to print them later
    # on to review them
    study = encryption_keys.get(study_object_id)
    if not study:
        print(f"skipping '{path}'")
        stats.number_illegal_folders += 1
        continue
    
    the_args.append((path, study,))
    
    if len(the_args) >= 250:
        # print(the_args)
        list(  # just a fast iterate
            pool.imap_unordered(compress_file, the_args, chunksize=1)
        )
        the_args = []

stats.stats()

print()
print("illegal folders:")
print(illegal_folders)
print()
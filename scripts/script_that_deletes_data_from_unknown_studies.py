# nohup time nice -n 20 python -u run_script.py script_that_deletes_data_from_unknown_studies > delete_unknown.log & tail -f delete_unknown.log

from multiprocessing.pool import ThreadPool
from constants.common_constants import CHUNKS_FOLDER, CUSTOM_ONDEPLOY_PREFIX, PROBLEM_UPLOADS
from database.models import Participant, S3File, Study
from libs.s3 import BadS3PathException, S3Storage, s3_delete, s3_get_size, s3_retrieve, s3_list_files


class stats:
    number_paths_total = 0
    total_deleted = 0
    total_bytes_s3 = 0
    number_bad_paths = 0
    number_files_failed = 0
    
    @classmethod
    def stats(cls):
        print()
        print("number_paths_total:", cls.number_paths_total)
        print("number_illegal_folders:", len(definitely_illegal_folders))
        print("number_studies_found:", len(study_object_ids))
        print("number_participants_found:", len(patient_ids_with_study))
        print("number_files_failed:", cls.number_files_failed)
        print("number_bad_paths:", cls.number_bad_paths)
        print()


definitely_illegal_folders = set[str]()
study_object_ids = set[str]()
patient_ids_with_study = set[tuple[str,str]]()
for path in s3_list_files("", as_generator=True):
    
    stats.number_paths_total += 1
    path_start = path.split("/", 1)[0]
    
    # illegal folders
    if path_start in (PROBLEM_UPLOADS, CUSTOM_ONDEPLOY_PREFIX):
        continue
    
    study_object_id = path.split("/")[1] if path_start == CHUNKS_FOLDER else path_start
    
    if len(study_object_id) != 24:  # if it is not the right
        definitely_illegal_folders.add(path_start)
        continue
    
    # we now definitely have an object-id-like string
    study_object_ids.add(study_object_id)
    
    # get the patient_id, always follows the study_object_id
    start_is_patient_id = path.split(f"{study_object_id}/", 1)[-1]
    patient_id = start_is_patient_id.split("/", 1)[0]
    patient_ids_with_study.add((study_object_id, patient_id, ))
    
    if stats.number_paths_total % 10000 == 0:
        stats.stats()

stats.stats()


# lookups
valid_study_object_ids = set(Study.vlist("object_id"))
valid_patient_ids = set(Participant.vlist("patient_id"))
prefixes_to_empty = []


# get bad studies
for possible_study_object_id in study_object_ids:
    if possible_study_object_id not in valid_study_object_ids:
        prefixes_to_empty.append(possible_study_object_id)
        prefixes_to_empty.append(CHUNKS_FOLDER + "/" + possible_study_object_id)

# get bad participants in valid studies
for possible_study_object_id, patient_id in patient_ids_with_study:
    if possible_study_object_id not in valid_study_object_ids:
        continue  # already added
    
    if patient_id not in valid_patient_ids:
        prefixes_to_empty.append(possible_study_object_id + "/" + patient_id)
        prefixes_to_empty.append(CHUNKS_FOLDER + "/" + possible_study_object_id + "/" + patient_id)


print("prefixes_to_empty:")
print(prefixes_to_empty)

pool = ThreadPool(25)

def delete_and_stat_file(path: str):
    try:
        stats.total_bytes_s3 += s3_get_size(path) or 0
        s3_delete(path)
        stats.total_deleted += 1
    except BadS3PathException as e:
        print(e)
        stats.number_bad_paths += 1
    except Exception as e:
        print(f"uhoh, encountered an `{e}` on {path}.")
        stats.number_files_failed += 1


for path in prefixes_to_empty:
    print("deleting", path)
    S3File.fltr(path__startswith=path).delete()
    list(
        pool.imap_unordered(s3_delete, s3_list_files(path), chunksize=1)
    )
    stats.stats()


#  patient_ids = set(Study.vlist("object_id"))
#  object_ids = set(Participant.vlist("patient_id"))
#  for path in paths:
#     if path.startswith("CHUNK"):
#         continue
#     if path.count("/") == 0:
#         print("object_id:", path)
#         assert path not in object_ids
#         continue
#     if path.count("/") == 1:
#         patient_id, objid = path.split("/")
#         print("parts:", patient_id, objid)
#         assert objid not in object_ids
#         assert patient_id not in patient_ids

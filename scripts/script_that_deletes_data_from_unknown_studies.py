# nohup time nice -n 20 python -u run_script.py script_that_deletes_data_from_unknown_studies > delete_unknown.log & tail -f delete_unknown.log

from multiprocessing.pool import ThreadPool

from constants.common_constants import (CHUNKS_FOLDER, CUSTOM_ONDEPLOY_PREFIX, LOGS_FOLDER,
    PROBLEM_UPLOADS)
from database.models import Participant, S3File, Study
from libs.s3 import s3_delete, s3_get_size, s3_list_files
from libs.utils.http_utils import numformat


class stats:
    number_paths_total = 0
    number_total_deleted = 0
    bytes_deleted_s3 = 0
    number_deletes_failed = 0
    
    @classmethod
    def reset(cls):
        for attr in cls.__dict__:
            if attr.startswith('number_'):
                setattr(cls, attr, 0)
        
        cls.valid_study_object_ids = set[str]()
        cls.invalid_root_folders = set[str]()
        cls.valid_patient_ids = set[str]()
        cls.invalid_participant_prefixes = set[str]()
    
    @classmethod
    def stats(cls):
        print()
        print("number_paths_total:", numformat(cls.number_paths_total))
        print("invalid_root_folders:", numformat(len(cls.invalid_root_folders)))
        print("invalid_participant_prefixes:", numformat(len(cls.invalid_participant_prefixes)))
        print("valid_study_object_ids:", numformat(len(cls.valid_study_object_ids)))
        print("valid_patient_ids:", numformat(len(cls.valid_patient_ids)))
        print()
    
    @classmethod
    def deleted_stats(cls):
        print("number_total_deleted:", numformat(cls.number_total_deleted))
        print("bytes_deleted_s3:", numformat(cls.bytes_deleted_s3))
        print("number_deletes_failed:", numformat(cls.number_deletes_failed))


def delete_and_stat_file(path: str):
    try:
        stats.bytes_deleted_s3 += s3_get_size(path) or 0
        s3_delete(path)
        stats.number_total_deleted += 1
    except Exception as e:
        print(f"uhoh, encountered an `{e}` on {path}.")
        stats.number_deletes_failed += 1


VALID_JUNK_FOLDERS = (PROBLEM_UPLOADS, CUSTOM_ONDEPLOY_PREFIX, LOGS_FOLDER)


def get_obj_patient_file(path: str) -> tuple[str, str]:
    # returns the study_object_id, the patient_id, the rest of the file path
    if path.startswith("CHUNKED_DATA"):
        path = path[13:]  # remove the prefix
    
    if path.count("/") > 1:
        return path.split("/", 2)[:2]
    
    # the patient_id may be patient_id.zst, in principle other extensions in the future
    obj_id, patient_id = path.split("/")
    patient_id = patient_id.split(".", 1)[0]
    return obj_id, patient_id


def main():
    stats.reset()
    pool = ThreadPool(25)
    
    for path in s3_list_files("", as_generator=True):
        if stats.number_paths_total % 100_000 == 0:  # We have a lot of files.
            stats.stats()
            print("invalid_root_folders:", stats.invalid_root_folders)
            print("invalid_participant_prefixes:", stats.invalid_participant_prefixes)
            print()
        
        stats.number_paths_total += 1
        path_start = path.split("/", 1)[0]
        
        # if it is in these folders it can be ignored
        if path_start in VALID_JUNK_FOLDERS:
            continue
        
        # if its not a 24 character string then it is not a study, and this folder is unknewn and
        # the file should be deleted.
        if len(path_start) != 24 and path_start != CHUNKS_FOLDER:
            stats.invalid_root_folders.add(path_start)
            continue
        
        # get the patient_id, always follows the study_object_id
        study_object_id, patient_id = get_obj_patient_file(path)
        
        # if it is not a study, put it in the illegal folder list, 
        if study_object_id in stats.invalid_root_folders:
            continue  # anything in this folder is illegal, skip the participant
        
        # test the study object efficiently
        if study_object_id not in stats.valid_study_object_ids:
            try:
                if Study.fltr(object_id=study_object_id).exists():
                    stats.valid_study_object_ids.add(study_object_id)
            except Study.DoesNotExist:
                stats.invalid_root_folders.add(study_object_id)
                continue  # anything in this folder is illegal, skip the participant
        
        # test the participant id efficiently
        if patient_id not in stats.valid_patient_ids:
            # check if we have already invalidated this participant-study pair to avoid a database call
            if (path_prefix:= f"{study_object_id}/{patient_id}") in stats.invalid_participant_prefixes:
                continue
            try:
                if Participant.fltr(patient_id=patient_id).exists():  # validate the patient_id
                    stats.valid_patient_ids.add(patient_id)
            except Participant.DoesNotExist:
                stats.invalid_participant_prefixes.add(path_prefix)
    
    stats.stats()
    ## We have now separated invalid object ids as invalid folders, so we can delete all of those.
    ## This is fundamentally sound because we _cannot decrypt them_.
    ## We have validated the participant ids and stored their prefix info.
    # To delete we issue delete on the raw folder prefix, and with a CHUNKED_DATA prefix.
    
    # lookups
    prefixes_to_empty = list(stats.invalid_root_folders)
    prefixes_to_empty.extend([f"{CHUNKS_FOLDER}/s" for s in stats.invalid_root_folders])
    prefixes_to_empty.extend(stats.invalid_participant_prefixes)
    prefixes_to_empty.extend([f"{CHUNKS_FOLDER}/s" for s in stats.invalid_participant_prefixes])
    
    print("prefixes_to_empty:")
    print(prefixes_to_empty)
    
    exit(1)
    ## this script is being tested so it won't delete those files yet.
    for path in prefixes_to_empty:
        print("deleting", path)
        deleted = S3File.fltr(path__startswith=path).delete()
        print("deleted", deleted, "database entries")
        list(
            pool.imap_unordered(s3_delete, s3_list_files(path), chunksize=1)
        )
        print()
    stats.deleted_stats()

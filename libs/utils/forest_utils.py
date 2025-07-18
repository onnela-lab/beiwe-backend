# FILES IN UTILS SHOULD HAVE SPARSE IMPORTS SO THAT THEY CAN BE USED ANYWHERE.
# IF YOU ARE IMPORTING FROM A DATABASE MODEL YOU SHOULD PLACE IT ELSEWHERE. (ANNOTATION IMPORTS ARE OK)
from __future__ import annotations

import pickle
from posixpath import join as path_join
from typing import TYPE_CHECKING

from constants.common_constants import BEIWE_PROJECT_ROOT
from libs.s3 import s3_retrieve


if TYPE_CHECKING:
    from database.forest_models import ForestTask


# Cached data set serialization for Jasmine
def get_jasmine_all_bv_set_dict(task: ForestTask) -> dict:
    """ Return the unpickled all_bv_set dict. """
    if not task.all_bv_set_s3_key:
        return None  # Forest expects None if it doesn't exist
    return pickle.loads(
        s3_retrieve(task.all_bv_set_s3_key, task.participant.study.object_id, raw_path=True)
    )


def get_jasmine_all_memory_dict_dict(task: ForestTask) -> dict:
    """ Return the unpickled all_memory_dict dict. """
    if not task.all_memory_dict_s3_key:
        return None  # Forest expects None if it doesn't exist
    return pickle.loads(
        s3_retrieve(task.all_memory_dict_s3_key, task.participant.study.object_id, raw_path=True)
    )


def save_all_bv_set_bytes(task: ForestTask, all_bv_set_bytes):
    from libs.s3 import s3_upload
    task.all_bv_set_s3_key = task.all_bv_set_s3_key_path
    s3_upload(task.all_bv_set_s3_key, all_bv_set_bytes, task.participant, raw_path=True)
    task.save(update_fields=["all_bv_set_s3_key"])


def save_all_memory_dict_bytes(task: ForestTask, all_memory_dict_bytes):
    from libs.s3 import s3_upload
    task.all_memory_dict_s3_key = task.all_memory_dict_s3_key_path
    s3_upload(task.all_memory_dict_s3_key, all_memory_dict_bytes, task.participant, raw_path=True)
    task.save(update_fields=["all_memory_dict_s3_key"])


def save_output_file(task: ForestTask, output_file_bytes):
    from libs.s3 import s3_upload

    # output_zip_s3_path includes the study id, so we can use raw path
    s3_upload(task.output_zip_s3_path, output_file_bytes, task.participant, raw_path=True)
    task.save(update_fields=["output_zip_s3_path"])  # its already committed to the database


def download_output_file(task: ForestTask) -> bytes:
    return s3_retrieve(task.output_zip_s3_path, task.participant, raw_path=True)


# our extremely fragile mechanism to get the git commit of the "current" forest version
def get_forest_git_hash() -> str:
    shibboleth = "current_forest_git_hash"
    HASH_MALFORMED = "(Git Hash Malformed)"
    HASH_MISSING = "(Git Hash Missing)"
    
    with open(path_join(BEIWE_PROJECT_ROOT, "requirements.txt")) as f:
        requirements_file_lines = f.read().splitlines()
    
    for line in requirements_file_lines:
        # in the insane case of multiple matches we are getting the first instance, not the last.
        if shibboleth in line:
            
            git_hash = line.split(shibboleth)[1]  # second after current_forest_git_hash
            git_hash = git_hash.strip()           # remove whitespace, its fine just do it....
            if not git_hash.startswith(":"):      # if there is no colon it is wrong.
                print("Forest git hash is malformed")
                return HASH_MALFORMED
            
            git_hash = git_hash[1:]
            git_hash = git_hash.strip()  # remove any intermediate whitespace
            
            if len(git_hash) == 40:
                return git_hash
            else:
                print(f"Forest git hash is not 40 characters long, it was only {len(git_hash)}")
                return HASH_MALFORMED
    
    return HASH_MISSING
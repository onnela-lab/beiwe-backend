# This code is based offthe purge particpant code


from constants.common_constants import PROBLEM_UPLOADS
from libs.s3 import s3_delete_many_versioned, s3_list_versions


DELETION_PAGE_SIZE = 1000




many_file_version_ids = []
s3_prefix = PROBLEM_UPLOADS + "/"

for i, key_path_version_id_tuple in enumerate(s3_list_versions(s3_prefix)):
    if i % DELETION_PAGE_SIZE * 10 == 0:
        print(i)
    
    # yield a page of files, reset page
    many_file_version_ids.append(key_path_version_id_tuple)
    if len(many_file_version_ids) % DELETION_PAGE_SIZE == 0:
        
        s3_delete_many_versioned(many_file_version_ids)
        
        many_file_version_ids = []

if many_file_version_ids:
    s3_delete_many_versioned(many_file_version_ids)

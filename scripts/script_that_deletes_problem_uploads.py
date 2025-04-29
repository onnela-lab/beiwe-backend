from constants.common_constants import PROBLEM_UPLOADS
from database.models import GenericEvent, IOSDecryptionKey, S3File
from libs.s3 import s3_delete_many_versioned, s3_list_versions


DELETION_PAGE_SIZE = 1000


# based off the pattern over in particpiant purge
def main():
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
    
    IOSDecryptionKey.objects.all().delete()
    # GenericEvent.objects.filter(tag__startswith="problem_upload").delete()  # actually we can just clear it completely
    GenericEvent.objects.all().delete()
    S3File.objects.filter(path__startswith=PROBLEM_UPLOADS).delete()

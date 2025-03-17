from typing import Tuple

from django.utils import timezone

from constants.data_processing_constants import CHUNK_EXISTS_CASE
from database.data_access_models import ChunkRegistry
from libs.s3 import s3_upload
from libs.utils.compression import decompress
from libs.utils.security_utils import chunk_hash


# from datetime import datetime
# GLOBAL_TIMESTAMP = datetime.now().isoformat()


def batch_upload(upload: Tuple[ChunkRegistry or dict, str, bytes, str]):
    """ Used for mapping an s3_upload function.  the tuple is unpacked, can only have one parameter. """
    chunk, chunk_path, new_contents, study_object_id = upload
    del upload
    # there is an external reference to the original new_contents object so there's no way to 
    # free it up without a refactor.
    new_contents = decompress(new_contents)
    
    if "b'" in chunk_path:
        raise Exception(chunk_path)
    
    # for use with test script to avoid network uploads
    # with open("processing_tests/" + GLOBAL_TIMESTAMP, 'ba') as f:
    #     f.write(b"\n\n")
    #     f.write(new_contents)
    #     return ret
    # print("uploading:", chunk_path)
    s3_upload(chunk_path, new_contents, study_object_id, raw_path=True)
    
    # if the chunk object is a chunk registry then we are updating an old one,
    # otherwise we are creating a new one.
    if chunk == CHUNK_EXISTS_CASE:
        # If the contents are being appended to an existing ChunkRegistry object
        ChunkRegistry.objects.filter(chunk_path=chunk_path).update(
            file_size=len(new_contents),
            chunk_hash=chunk_hash(new_contents).decode(),
            last_updated=timezone.now()
        )
    else:
        ChunkRegistry.register_chunked_data(**chunk, file_contents=new_contents)
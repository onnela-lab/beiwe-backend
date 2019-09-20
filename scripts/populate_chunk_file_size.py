from os.path import abspath as _abspath
from sys import path as _path
_one_folder_up = _abspath(__file__).rsplit('/',2)[0]
_path.insert(1, _one_folder_up)

from config import load_django
from datetime import datetime

from database.data_access_models import ChunkRegistry
from libs.s3 import conn, S3_BUCKET

print("start:", datetime.now())

# stick study object ids here to process particular studies
study_object_ids = []

filters = dict(file_size__isnull=True)
if study_object_ids:
    filters["study__object_id__in"] = study_object_ids

# this could be a huge query, use the iterator
query = ChunkRegistry.objects.filter(**filters).values_list("pk", "chunk_path").iterator()


for i, (pk, path) in enumerate(query):
    if i % 1000 == 0:
        print(i)
    size = conn.head_object(Bucket=S3_BUCKET, Key=path)["ContentLength"]
    ChunkRegistry.objects.filter(pk=pk).update(file_size=size)

print("end:", datetime.now())

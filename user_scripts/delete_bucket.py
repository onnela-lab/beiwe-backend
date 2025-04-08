# add the root of the project into the path to allow cd-ing into this folder and running the script.
from os.path import abspath
from sys import path
path.insert(0, abspath(__file__).rsplit('/', 2)[0])

import boto3
from pprint import pprint

conn = boto3.client(
    's3',
    # You have to fill these in
    # aws_access_key_id="",
    # aws_secret_access_key="",
    region_name="us-east-1"
)

def paginate_files(prefix):
    paginator = conn.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket="beiwe-backup", Prefix=prefix)
    
    for page in page_iterator:
        if 'Contents' not in list(page.keys()):
            return
        yield [item['Key'] for item in page['Contents']]


def paginate_versions(prefix):
    paginator = conn.get_paginator('list_object_versions')
    page_iterator = paginator.paginate(Bucket="beiwe-backup", Prefix=prefix)
    print(page_iterator)
    for page in page_iterator:
        pprint(page)
        # pprint(page.keys())
        # pprint(page['DeleteMarkers'])
        # pprint(page["Versions"])
        if 'Versions' not in list(page.keys()):
            continue
        
        yield [(item['Key'], item['VersionId'])  for item in page['Versions']]

#
# accumulator = 0
# for page, file_list in enumerate(paginate_files("")):
#     accumulator += len(file_list)
#     print(page, accumulator)
#     objects = [
#         {'Key': fp} for fp in file_list
#     ]
#
#     delete_args = {
#         "Bucket": "beiwe-backup",
#         "Delete": {
#             'Objects': objects,
#             'Quiet': False,
#         },
#     }
#
#     conn.delete_objects(**delete_args)


accumulator = 0
for page, file_list in enumerate(paginate_versions("")):
    accumulator += len(file_list)
    print((page, accumulator))
    objects = [
        {'Key': fp, "VersionId": version_id} for fp, version_id in file_list
    ]
    
    delete_args = {
        "Bucket": "beiwe-backup",
        "Delete": {
            'Objects': objects,
            'Quiet': False,
        },
    }
    # conn.delete_objects(**delete_args)

import boto3
from pprint import pprint
from botocore.exceptions import ClientError
from Cryptodome.Cipher import AES

# THIS SCRIPT IS NOT INTENDED TO BE RUN VIA run_scripts.py

# Manually edit and run this script with the required variables below to test credentials generated
# for raw s3 data access.

BEIWE_SERVER_AWS_ACCESS_KEY_ID = ""
BEIWE_SERVER_AWS_SECRET_ACCESS_KEY = ""
S3_REGION_NAME = ""
S3_BUCKET = ""
STUDY_FOLDER = ""
ENCRYPTION_KEY = b""

# the file we pick to test decryption on
RANDOM_FILE = 100

conn = boto3.client(
    's3',
    aws_access_key_id=BEIWE_SERVER_AWS_ACCESS_KEY_ID,
    aws_secret_access_key=BEIWE_SERVER_AWS_SECRET_ACCESS_KEY,
    region_name=S3_REGION_NAME,
)


def iterate_s3(prefix: str):
    # its a paginated thing, this just it as  generator
    paginator = conn.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix)
    for page in page_iterator:
        if 'Contents' not in page:
            print(page)
            raise Exception("'Contents' not in page? somethings up with the api call?")
        for item in page['Contents']:
            yield item['Key'].strip("/")


def decrypt_s3(data: bytes) -> bytes:
    """ effectively copy-pasted from beiwe-backend """
    iv = data[:16]
    data = data[16:]
    return AES.new(ENCRYPTION_KEY, AES.MODE_CFB, segment_size=8, IV=iv).decrypt(data)


# cannot access the bucket's root
the_error = None
try:
    # this one should fail!
    for i, path in enumerate(iterate_s3("")):
        print(path)
        if i > 10:
            break
except ClientError as e:
    the_error = e

# disable this assert if you have permissions to the root of the
assert "An error occurred (AccessDenied) when calling the ListObjectsV2 operation: Access Denied" in str(
    the_error
)

# can list files
for i, path in enumerate(iterate_s3(STUDY_FOLDER)):
    print(i, path)
    if i > RANDOM_FILE:
        print()
        print("okay that's enough")
        break

print("will now read a file:")
api_response = conn.get_object(Bucket=S3_BUCKET, Key=path, ResponseContentType='string')
print("API response body:")
pprint(api_response)
print()
print("Some file content")
file_content = api_response["Body"].read()
print("raw file content of", path)
print(file_content[:1000])
print()
print("Some decrypted content of", path)
print(decrypt_s3(file_content)[:1000])
print()
print("done")
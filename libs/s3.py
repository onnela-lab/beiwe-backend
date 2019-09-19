import boto3

from config.constants import DEFAULT_S3_RETRIES
from config.settings import (S3_BUCKET, BEIWE_SERVER_AWS_ACCESS_KEY_ID,
    BEIWE_SERVER_AWS_SECRET_ACCESS_KEY, S3_REGION_NAME)
from libs import encryption

class S3VersionException(Exception): pass

conn = boto3.client('s3',
                    aws_access_key_id=BEIWE_SERVER_AWS_ACCESS_KEY_ID,
                    aws_secret_access_key=BEIWE_SERVER_AWS_SECRET_ACCESS_KEY,
                    region_name=S3_REGION_NAME)


def s3_upload(key_path, data_string, study_object_id, raw_path=False):
    if not raw_path:
        key_path = study_object_id + "/" + key_path
    data = encryption.encrypt_for_server(data_string, study_object_id)
    conn.put_object(Body=data, Bucket=S3_BUCKET, Key=key_path, ContentType='string')


def s3_retrieve(key_path, study_object_id, raw_path=False, number_retries=DEFAULT_S3_RETRIES):
    """ Takes an S3 file path (key_path), and a study ID.  Takes an optional argument, raw_path,
    which defaults to false.  When set to false the path is prepended to place the file in the
    appropriate study_id folder. """
    if not raw_path:
        key_path = study_object_id + "/" + key_path
    encrypted_data = _do_retrieve(S3_BUCKET, key_path, number_retries=number_retries)['Body'].read()
    return encryption.decrypt_server(encrypted_data, study_object_id)


def _do_retrieve(bucket_name, key_path, number_retries=DEFAULT_S3_RETRIES):
    """ Run-logic to do a data retrieval for a file in an S3 bucket."""
    try:
        return conn.get_object(Bucket=bucket_name, Key=key_path, ResponseContentType='string')
    except Exception:
        if number_retries > 0:
            print("s3_retrieve failed, retrying on %s" % key_path)
            return _do_retrieve(bucket_name, key_path, number_retries=number_retries - 1)
        
        raise


def s3_list_files(prefix, as_generator=False):
    """ Method fetches a list of filenames with prefix.
        note: entering the empty string into this search without later calling
        the object results in a truncated/paginated view."""
    return _do_list_files(S3_BUCKET, prefix, as_generator=as_generator)


def s3_list_versions(prefix, allow_multiple_matches=False):
    """
    Page structure - each page is a dictionary with these keys:
     Name, ResponseMetadata, Versions, MaxKeys, Prefix, KeyMarker, IsTruncated, VersionIdMarker
    We only care about 'Versions', which is a list of all object versions matching that prefix.
    Versions is a list of dictionaries with these keys:
     LastModified, VersionId, ETag, StorageClass, Key, Owner, IsLatest, Size

    returns a list of dictionaries.
    If allow_multiple_matches is False the keys are LastModified, VersionId, IsLatest.
    If allow_multiple_matches is True the key 'Key' is added, containing the s3 file path.
    """

    paginator = conn.get_paginator('list_object_versions')
    page_iterator = paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix)

    versions = []
    for page in page_iterator:
        # versions are not guaranteed, usually this means the file was deleted and only has deletion markers.
        if 'Versions' not in page:
            continue

        for s3_version in page['Versions']:
            if not allow_multiple_matches and s3_version['Key'] != prefix:
                raise S3VersionException("the prefix '%s' was not an exact match" % prefix)
            versions.append({
                'VersionId': s3_version["VersionId"],
                'Key': s3_version['Key'],
            })
    return versions


def _do_list_files(bucket_name, prefix, as_generator=False):
    paginator = conn.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=prefix)
    if as_generator:
        return _do_list_files_generator(page_iterator)
    else:
        items = []
        for page in page_iterator:
            if 'Contents' in page:
                for item in page['Contents']:
                    items.append(item['Key'].strip("/"))
        return items


def _do_list_files_generator(page_iterator):
    for page in page_iterator:
        if 'Contents' not in page:
            return
        for item in page['Contents']:
            yield item['Key'].strip("/")


def s3_delete(key_path):
    raise Exception("NO DONT DELETE")

################################################################################
######################### Client Key Management ################################
################################################################################

def create_client_key_pair(patient_id, study_id):
    """Generate key pairing, push to database, return sanitized key for client."""
    public, private = encryption.generate_key_pairing()
    s3_upload("keys/" + patient_id + "_private", private, study_id )
    s3_upload("keys/" + patient_id + "_public", public, study_id )


def get_client_public_key_string(patient_id, study_id):
    """Grabs a user's public key string from s3."""
    key_string = s3_retrieve( "keys/" + patient_id +"_public" , study_id)
    return encryption.prepare_X509_key_for_java( key_string )


def get_client_public_key(patient_id, study_id):
    """Grabs a user's public key file from s3."""
    key = s3_retrieve( "keys/" + patient_id +"_public", study_id )
    return encryption.import_RSA_key( key )


def get_client_private_key(patient_id, study_id):
    """Grabs a user's private key file from s3."""
    key = s3_retrieve( "keys/" + patient_id +"_private", study_id)
    return encryption.import_RSA_key( key )

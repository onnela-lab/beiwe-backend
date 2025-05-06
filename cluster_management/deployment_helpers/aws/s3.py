import json

from deployment_helpers.aws.boto_helpers import (create_s3_client, create_s3_resource,
    create_sts_client)
from deployment_helpers.constants import get_global_config
from deployment_helpers.general_utils import log, random_alphanumeric_string


def s3_create_bucket(bucket_name):
    kwargs = {}
    # If region is us-east-1, then we cannot send this argument, or else the create_bucket command will fail
    global_configuration = get_global_config()
    if global_configuration["AWS_REGION"] != 'us-east-1':
        kwargs = {
            'CreateBucketConfiguration': {
                'LocationConstraint': global_configuration["AWS_REGION"]
            }
        }
    s3_client = create_s3_client()
    s3_client.create_bucket(ACL='private', Bucket=bucket_name, **kwargs)


def s3_encrypt_bucket(bucket_name):
    """ Set the policy for the given bucket to enable encryption of data at rest. Note that this 
    causes increased costs on all data uses of S3, very roughly doubling the cost of downloading
    data - and actually after applying compression this is likely to be MORE than that. """
    s3_client = create_s3_client()
    # Add default encryption of data.
    s3_client.put_bucket_encryption(
        Bucket=bucket_name,
        ServerSideEncryptionConfiguration={
            'Rules': [{
                'ApplyServerSideEncryptionByDefault': {
                    'SSEAlgorithm': 'aws:kms',
                }
            },]
        }
    )


def s3_encrypt_eb_bucket():
    """ This function obtain the account ID and the region, constructs the _elasticbeanstalk_ s3 
    bucket name and applies a encrypt by default policy to the bucket.
    (This is the EB bucket NOT the Beiwe data bucket. This detail should improve compliance with
    security requirements generally but is not critical because this bucket stores, like, versions
    of deployed code and some server logs.) """
    global_config = get_global_config()
    account_id = create_sts_client().get_caller_identity().get('Account')
    # There ought to be an easier way to get this name.... (not really)
    s3_eb_bucket = 'elasticbeanstalk-{}-{}'.format(global_config['AWS_REGION'], account_id)
    log.info(f'Enabling encryption on S3 bucket: `s3_eb_bucket`')
    s3_encrypt_bucket(s3_eb_bucket)



def s3_require_tls(bucket_name):
    """ This enforces encryption of data in transit for any calls. """
    
    s3_client = create_s3_client()
    
    # Policy that enforces the use of TLS/SSL for all actions.
    bucket_policy = {
        'Version': '2012-10-17',
        'Id': 'Policy1565726245376',
        'Statement': [
            {
                'Sid': 'Stmt1565726242462',
                'Effect': 'Deny',
                'Principal': '*',
                'Action': '*',
                'Resource': 'arn:aws:s3:::{}/*'.format(bucket_name),
                'Condition': {
                    'Bool': {
                        'aws:SecureTransport': 'false'
                    }
                }
            }
        ]
    }
    
    s3_client.put_bucket_policy(Bucket=bucket_name, Policy=json.dumps(bucket_policy))


def check_bucket_name_available(bucket_name):
    s3_resource = create_s3_resource()
    return s3_resource.Bucket(bucket_name) not in s3_resource.buckets.all()


def clean_eb_bucket_name(eb_environment_name):
    return eb_environment_name


def create_data_bucket(eb_environment_name):
    for i in range(10):
        name = "beiwe-data-{}-{}".format(eb_environment_name, random_alphanumeric_string(63))[:63].lower()
        log.info("checking availability of bucket name '%s'" % name)
        if check_bucket_name_available(name):
            s3_create_bucket(name)
            s3_encrypt_bucket(name)
            s3_require_tls(name)
            return name
    raise Exception("Was not able to construct a bucket name that is not in use.")

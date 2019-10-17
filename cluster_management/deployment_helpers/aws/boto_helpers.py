import boto3

from deployment_helpers.constants import (get_aws_credentials,
    get_global_config)

AWS_CREDENTIIALS = get_aws_credentials()
GLOBAL_CONFIGURATION = get_global_config()

def _get_client(client_type):
    """ connect to a boto3 CLIENT in the appropriate type and region. """
    return boto3.client(
            client_type,
            aws_access_key_id=AWS_CREDENTIIALS["AWS_ACCESS_KEY_ID"],
            aws_secret_access_key=AWS_CREDENTIIALS["AWS_SECRET_ACCESS_KEY"],
            region_name=GLOBAL_CONFIGURATION["AWS_REGION"],
    )

def _get_resource(client_type):
    """ connect to a boto3 RESOURCE in the appropriate type and region. """
    return boto3.resource(
            client_type,
            aws_access_key_id=AWS_CREDENTIIALS["AWS_ACCESS_KEY_ID"],
            aws_secret_access_key=AWS_CREDENTIIALS["AWS_SECRET_ACCESS_KEY"],
            region_name=GLOBAL_CONFIGURATION["AWS_REGION"],
    )

def create_ec2_client():
    return _get_client('ec2')

def create_eb_client():
    return _get_client('elasticbeanstalk')

def create_iam_client():
    return _get_client('iam')

def create_rds_client():
    return _get_client('rds')

def create_batch_client():
    return _get_client('batch')

def create_s3_client():
    return _get_client('s3')

def create_s3_client():
    return _get_client('s3')

def create_sts_client():
    return _get_client('sts')

## Resources.
def create_ec2_resource():
    return _get_resource('ec2')

def create_iam_resource():
    return _get_resource('iam')

def create_s3_resource():
    return _get_resource('s3')

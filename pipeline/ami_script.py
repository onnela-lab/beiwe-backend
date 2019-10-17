"""
A script for creating an AMI to be used for AWS Batch jobs
"""

import json
import os
from time import sleep

import boto3
from botocore.exceptions import ClientError

from .script_helpers import set_default_region
from .configuration_getters import get_configs_folder, get_aws_object_names


def run():
    """
    Run the code
    :return: The AMI's id, to be used for attaching it to the batch jobs
    """

    # Load a bunch of JSON blobs containing policies and other things that boto3 clients
    # require as input.
    configs_folder = get_configs_folder()
    
    with open(os.path.join(configs_folder, 'ami-ec2-instance-props.json')) as fn:
        ami_ec2_instance_props_dict = json.load(fn)
    aws_object_names = get_aws_object_names()
    print('JSON loaded')
    
    # Get the AMI ID for the local region
    set_default_region()
    ec2_client = boto3.client('ec2')
    image_name = ami_ec2_instance_props_dict.pop('ImageName')
    resp = ec2_client.describe_images(Filters=[{'Name': 'name', 'Values': [image_name]}])
    ami_ec2_instance_props_dict['ImageId'] = resp['Images'][0]['ImageId']
    
    # Create an EC2 instance to model the AMI off of
    resp = ec2_client.run_instances(**ami_ec2_instance_props_dict)
    ec2_instance_id = resp['Instances'][0]['InstanceId']
    print('EC2 instance created')
    
    # Create an AMI based off of the EC2 instance. It takes some time for the EC2 instance to
    # be ready, so we delay up to thirty seconds.
    print('Waiting for unencrypted AMI...')
    tries = 0
    while True:
        try:
            resp = ec2_client.create_image(
                InstanceId=ec2_instance_id,
                Name=aws_object_names['ami_name'] + '-unencrypted',
            )
        except ClientError:
            # In case the EC2 instance isn't ready yet
            tries += 1
            if tries > 30:
                raise
            sleep(1)
        else:
            break
    unencrypted_ami_id = resp['ImageId']
    print('Unencrypted AMI created')
    
    # Create an encrypted AMI based off of the previous AMI. This is the quickest way to
    # create an encrypted AMI, because you can't create an EC2 instance with an encrypted root
    # drive, and you can't create an encrypted AMI directly from an unencrypted EC2 instance.
    region_name = boto3.session.Session().region_name
    print('Waiting to encrypt AMI...')
    tries = 0
    while True:
        try:
            resp = ec2_client.copy_image(
                SourceImageId=unencrypted_ami_id,
                SourceRegion=region_name,
                Encrypted=True,
                Name=aws_object_names['ami_name'],
            )
        except ClientError:
            # In case the unencrypted AMI isn't ready yet
            tries += 1
            if tries > 300:
                raise
            else:
                print("waiting on unencrypted ami...")
            sleep(1)
        else:
            break
    ami_id = resp['ImageId']
    print('Encrypted AMI created')
    
    # Delete the EC2 instance and the unencrypted AMI; only the encrypted AMI is useful
    # going forward.
    ec2_client.terminate_instances(InstanceIds=[ec2_instance_id])
    # ec2_client.deregister_image(ImageId=unencrypted_ami_id)
    
    return ami_id

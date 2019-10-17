"""
A script for creating a setup to run AWS Batch jobs: a compute environment, a job queue and a
job definition to use as a template for actual jobs.
"""
import json
import os.path
from pprint import pprint
from time import sleep

import boto3

from configuration_getters import get_aws_object_names, get_configs_folder, get_current_region
from script_helpers import set_default_region


def run(repo_uri, ami_id):
    """
    Run the code
    :param repo_uri: string, the URI of an existing AWS ECR repository.
    :param ami_id: string, the id of an existing AWS AMI.
    """
    
    # Load a bunch of JSON blobs containing policies and other things that boto3 clients
    # require as input.
    configs_folder = get_configs_folder()
    
    with open(os.path.join(configs_folder, 'assume-batch-role.json')) as fn:
        assume_batch_role_policy_json = json.dumps(json.load(fn))
    with open(os.path.join(configs_folder, 'batch-service-role.json')) as fn:
        batch_service_role_policy_json = json.dumps(json.load(fn))
    with open(os.path.join(configs_folder, 'assume-ec2-role.json')) as fn:
        assume_ec2_role_policy_json = json.dumps(json.load(fn))
    with open(os.path.join(configs_folder, 'batch-instance-role.json')) as fn:
        batch_instance_role_policy_json = json.dumps(json.load(fn))
    with open(os.path.join(configs_folder, 'compute-environment.json')) as fn:
        compute_environment_dict = json.load(fn)
    with open(os.path.join(configs_folder, 'container-props.json')) as fn:
        container_props_dict = json.load(fn)
    aws_object_names = get_aws_object_names()
    print('JSON loaded')

    # Grab the names from aws_object_names
    comp_env_role = aws_object_names['comp_env_role']
    instance_profile = aws_object_names['instance_profile']
    security_group = aws_object_names['security_group']

    # default names for entities that we may chang the name of.
    default_comp_env_name = aws_object_names['comp_env_name']
    default_queue_name = aws_object_names['queue_name']
    default_job_definition_name = aws_object_names['job_defn_name']

    if "subnets" not in compute_environment_dict:
        # "subnets": ["subnet-af1f02e6"]
        ec2_client = boto3.client('ec2')
        subnets = ec2_client.describe_subnets()['Subnets']
        if len(set([y['VpcId'] for y in subnets])) != 1:
            print("\n")
            print("It looks like you have multiple VPCs in this region, which means this script")
            print("cannot automatically determine the correct subnets on which to place")
            print("the data pipeline compute servers.")
            print("You can resolve this by adding a line with the key 'subnets' like the following")
            print("to the compute-environment.json file in the configs folder.")
            print("""  "subnets": ["subnet-abc123"]""")
            exit(1)
        else:
            # add a 1 item list containing a valid subnet
            compute_environment_dict['subnets'] = [subnets[0]['SubnetId']]
    
    # Create a new IAM role for the compute environment
    set_default_region()
    iam_client = boto3.client('iam')

    try:
        comp_env_role_arn = iam_client.create_role(
            RoleName=comp_env_role,
            AssumeRolePolicyDocument=assume_batch_role_policy_json,
        )['Role']['Arn']
    except Exception as e:
        if "Role with name AWSBatchServiceRole already exists." in str(e):
            comp_env_role_arn = iam_client.get_role(RoleName=comp_env_role)['Role']['Arn']
        else:
            raise

    try:
        iam_client.put_role_policy(
            RoleName=comp_env_role,
            PolicyName='aws-batch-service-policy',  # This name isn't used anywhere else
            PolicyDocument=batch_service_role_policy_json,
        )
        print('Batch role created')
    except Exception:
        print('WARNING: Batch service role creation failed, assuming that this means it already exists.')
    
    # Create an EC2 instance profile for the compute environment
    try:
        iam_client.create_role(
            RoleName=instance_profile,
            AssumeRolePolicyDocument=assume_ec2_role_policy_json,
        )
    except Exception:
        print('WARNING: Batch role creation failed, assuming that this means it already exists.')

    try:
        iam_client.put_role_policy(
            RoleName=instance_profile,
            PolicyName='aws-batch-instance-policy',  # This name isn't used anywhere else
            PolicyDocument=batch_instance_role_policy_json,
        )
    except Exception:
        print('WARNING: assigning role creation failed, assuming that this means it already exists.')


    try:
        resp = iam_client.create_instance_profile(InstanceProfileName=instance_profile)
    except Exception as e:
        if "Instance Profile ecsInstanceRole already exists." in str(e):
            resp = iam_client.get_instance_profile(InstanceProfileName=instance_profile)

    compute_environment_dict['instanceRole'] = resp['InstanceProfile']['Arn']
    try:
        iam_client.add_role_to_instance_profile(
            InstanceProfileName=instance_profile,
            RoleName=instance_profile,
        )
        print('Instance profile created')
    except Exception as e:
        if not "Cannot exceed quota for InstanceSessionsPerInstanceProfile" in str(e):
            raise
    
    # Create a security group for the compute environment
    ec2_client = boto3.client('ec2')

    try:
        group_id = ec2_client.describe_security_groups(GroupNames=[security_group])['SecurityGroups'][0]['GroupId']
    except Exception:
        try:
            group_id = ec2_client.create_security_group(
                Description='Security group for AWS Batch',
                GroupName=security_group,
            )['GroupId']
        except Exception as e:
            if "InvalidGroup.Duplicate" not in str(e):
                # unknown case.
                raise
    
    # setup for batch compute environment creation
    # (the raise condition above is sufficient for this potential unbound local error)
    batch_client = boto3.client('batch')
    compute_environment_dict['imageId'] = ami_id
    compute_environment_dict['securityGroupIds'] = [group_id]

    final_comp_env_name = create_compute_environment(
        batch_client, compute_environment_dict, default_comp_env_name, comp_env_role_arn
    )

    # Then create the job queue
    final_jobq_name = create_batch_job_queue(batch_client, default_queue_name, final_comp_env_name)

    # Create a batch job definition
    container_props_dict['image'] = repo_uri
    container_props_dict['environment'] = [
        {
            'name': 'access_key_ssm_name',
            'value': aws_object_names['access_key_ssm_name'],
        }, {
            'name': 'secret_key_ssm_name',
            'value': aws_object_names['secret_key_ssm_name'],
        }, {
            'name': 'region_name',
            'value': get_current_region(),
        }
    ]

    final_job_definition_name = create_job_definition(
        batch_client, default_job_definition_name, container_props_dict
    )

    print("\n\nFINAL NOTES for settings you will need to set your Beiwe server:")
    print("You will need to set 'comp_env_name' to '%s'" % final_comp_env_name)
    print("You will need to set 'queue_name' to '%s'" % final_jobq_name)
    print("You will need to set 'job_defn_name' to '%s'" % final_job_definition_name)


def create_compute_environment(batch_client, compute_environment_dict, original_comp_env_name, comp_env_role_arn):
    """ Determine if compute environment exists with this name, create a similarly named one if it does
     or the original name if it does not. """

    # Get a list of the existing compute environments.
    # The compute environment defines the ami that is used, which means that if it needs to exist we
    # need to try and handle that case because it is the most obnoxious and slow thing to delete.
    # Todo: cannot determine if this will paginate correctly
    extant_compute_environments = [
        compute_environment['computeEnvironmentName'] for compute_environment in
        batch_client.describe_compute_environments()['computeEnvironments']
        if "computeEnvironmentName" in compute_environment
    ]

    final_comp_env_name = find_available_name(
        original_comp_env_name, extant_compute_environments, "Batch Compute Environment"
    )

    print("Creating a new Batch Compute Environment named '%s'..." % final_comp_env_name)
    batch_client.create_compute_environment(
        computeEnvironmentName=final_comp_env_name,
        type='MANAGED',
        computeResources=compute_environment_dict,
        serviceRole=comp_env_role_arn,
    )
    print("Created new Batch Compute Environment named '%s'." % final_comp_env_name)

    # The compute environment takes somewhere between 10 and 45 seconds to create. Until it
    # is created, we cannot create a job queue. So first, we wait until the compute environment
    # has finished being created.
    print('Waiting for compute environment to become available...')
    while True:
        # Ping the AWS server for a description of the compute environment
        resp = batch_client.describe_compute_environments(computeEnvironments=[final_comp_env_name])
        status = resp['computeEnvironments'][0]['status']

        if status == 'VALID':
            # If the compute environment is valid, we can proceed to creating the job queue
            break
        elif status == 'CREATING' or status == 'UPDATING':
            # If the compute environment is still being created (or has been created and is
            # now being modified), we wait one second and then ping the server again.
            sleep(1)
        else:
            # If the compute environment is invalid (or deleting or deleted), we cannot
            # continue with job queue creation. Raise an error and quit the script.
            raise RuntimeError('Compute Environment is Invalid')

    print('Compute environment available.')
    return final_comp_env_name


def create_batch_job_queue(batch_client, orig_jobq_name, comp_env_name):
    # determine if job queue with the given name exists, attempt to create similarly named job queues
    # Todo: cannot determine if this will paginate correctly
    extant_job_queues = [
        jobq['jobQueueName'] for jobq in batch_client.describe_job_queues()['jobQueues']
        if "jobQueueName" in jobq
    ]

    final_jobq_name = find_available_name(
        orig_jobq_name, extant_job_queues, "Batch Compute Environment"
    )

    print('Creating Job Queue named "%s"...' % final_jobq_name)
    batch_client.create_job_queue(
        jobQueueName=final_jobq_name,
        priority=1,
        computeEnvironmentOrder=[{'order': 0, 'computeEnvironment': comp_env_name}],
    )
    print('Created Job Queue named "%s"...' % final_jobq_name)
    return final_jobq_name


def create_job_definition(batch_client, original_job_definition_name, container_props_dict):
    # Todo: cannot determine if this will paginate correctly
    extant_job_definitions = [
        job_dfn['jobDefinitionName'] for job_dfn in
        batch_client.describe_job_definitions()['jobDefinitions']
        if "jobDefinitionName" in job_dfn
    ]

    final_job_definition = find_available_name(
        original_job_definition_name, extant_job_definitions, "Batch Job Definition"
    )

    print('Creating Job Definition "%s"...' % final_job_definition)
    batch_client.register_job_definition(
        jobDefinitionName=final_job_definition,
        type='container',
        containerProperties=container_props_dict,
    )
    print('Created Job Definition "%s".' % final_job_definition)
    return final_job_definition


def find_available_name(base_name, extant_names, printable_identifier):
    found_a_name = False
    current_name = base_name
    for i in range(2, 100):
        # if the current name already exists (first iteration will be of the original name) change
        # the name and return to top of loop and start again.
        if current_name in extant_names:
            print("%s '%s' already exists..." % (printable_identifier, current_name))
            current_name = "%s_%s" % (base_name, i)
            continue
        else:
            found_a_name = True
            break

    if not found_a_name:
        print("base name:", base_name)
        print("for: ", printable_identifier)
        print("extant names:")
        pprint(extant_names)
        raise Exception("could not find a %s that was not in use.")

    return current_name

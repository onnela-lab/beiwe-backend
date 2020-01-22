"""
A script for creating a docker image and uploading it to an AWS ECS repository.
This should be run on a machine running Amazon Linux.
"""

import json
import os.path
import subprocess

import boto3
from botocore.exceptions import ClientError

from script_helpers import set_default_region
from configuration_getters import get_pipeline_folder, get_configs_folder, get_aws_object_names


def run():
    """
    Run the code
    :return: The repository's URI, to be used in creating AWS Batch jobs elsewhere
    """
    
    # Load basic info about the Beiwe instance setup
    configs_folder = get_configs_folder()
    with open(os.path.join(configs_folder, 'git-repository-info.json')) as fn:
        git_repository_info_dict = json.load(fn)
    
    # Install docker, git and AWS command line interface
    # -y means "don't ask for confirmation"
    # check_call will raise an error if the command fails (i.e. returns nonzero)
    subprocess.check_call(['sudo', 'yum', 'update', '-y'])
    subprocess.check_call(['sudo', 'yum', 'install', '-y', 'docker'])
    subprocess.check_call(['sudo', 'yum', 'install', '-y', 'git'])
    subprocess.check_call(['pip', 'install', 'awscli', '--upgrade', '--user'])
    print('Installations complete')
    
    # Get git repo to put in the docker
    pipeline_folder = get_pipeline_folder()
    git_destination = os.path.join(pipeline_folder, 'Beiwe-Analysis')
    git_repo = git_repository_info_dict['repository_url']
    git_branch = git_repository_info_dict['branch']
    try:
        subprocess.check_call(['git', 'clone', git_repo, git_destination, '--branch', git_branch])
        print('Git repository cloned')
    except subprocess.CalledProcessError:
        # The repository already exists in git_destination
        subprocess.check_call(['git', '-C', git_destination, 'checkout', git_branch])
        subprocess.check_call(['git', '-C', git_destination, 'pull'])
        print('Git repository updated')
    
    # Create the docker image
    subprocess.check_call(['sudo', 'service', 'docker', 'start'])
    subprocess.check_call(['sudo', 'docker', 'build', '-t', 'beiwe-analysis', pipeline_folder])
    print('Docker image created')
    
    # Create an AWS ECR repository to put the docker image into, and get the repository's URI
    # If such a repository already exists, get the repository's URI
    aws_object_names = get_aws_object_names()
    ecr_repo_name = aws_object_names['ecr_repo_name']
    set_default_region()
    client = boto3.client('ecr')
    try:
        resp = client.create_repository(
            repositoryName=ecr_repo_name,
        )
        repo_uri = resp['repository']['repositoryUri']
        print('ECR repository created')
    except ClientError:
        resp = client.describe_repositories(
            repositoryNames=(ecr_repo_name,),
        )
        repo_uri = resp['repositories'][0]['repositoryUri']
        print('Existing ECR repository found')
    
    # Tag the local docker image with the remote repository's URI. This is similar to
    # having a local git branch track a remote one.
    subprocess.check_call(['sudo', 'docker', 'tag', 'beiwe-analysis', repo_uri])
    
    # Push the docker file to our new repository
    # FIXME: using get-login is not ideal because it puts the password in process lists
    ecr_login = subprocess.check_output(['aws', 'ecr', 'get-login', '--no-include-email'])
    ecr_login_as_list = ['sudo'] + ecr_login.strip('\n').split(' ')
    subprocess.check_call(ecr_login_as_list)
    subprocess.check_call(['sudo', 'docker', 'push', repo_uri])
    print('Docker pushed')
    
    return repo_uri

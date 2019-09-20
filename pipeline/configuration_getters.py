import json
import os
from os.path import join as path_join, abspath

# We raise this error, it is eventually used to raise a useful error on the frontend if pipeline
# is not set up.
from subprocess import check_output


class DataPipelineNotConfigured(Exception): pass

#
# Path getters
#
def get_pipeline_folder():
    return abspath(__file__).rsplit('/', 1)[0]

def get_configs_folder():
    return path_join(get_pipeline_folder(), 'configs')

def get_aws_object_names_file():
    return path_join(get_configs_folder(), 'aws-object-names.json')

#
# Configuration getters and validator
#

# generic config components are not recommended to be set by the user.
# Problems with these reduce to generic AWS administration problems.
generic_config_components = [
    "ami_name",
    "ecr_repo_name",
    "instance_profile",
    "comp_env_name",
    "comp_env_role",
    "queue_name",  # used in creating a job
    "job_defn_name",  # used in creating a job
    "job_name",  # used in creating a job
    "access_key_ssm_name",
    "secret_key_ssm_name",
    "security_group",
]


def get_generic_config():
    return _validate_and_get_configs(generic_config_components, get_aws_object_names_file())


def get_eb_config():
    try:
        config_data = _load_json_file(get_aws_object_names_file())
    except DataPipelineNotConfigured:
        config_data = {}

    # override/populate everything that has an environment variable for it
    for setting in generic_config_components:
        if setting in os.environ:
            config_data[setting] = os.environ[setting]

    config_data["region_name"] = get_current_region()
    return config_data

def get_aws_object_names():
    return get_generic_config()


def _validate_and_get_configs(config_list, config_file_path):
    """ There are two cases that need to be handled, and one significant administrative feature.
    1. we check for the json files (expected to be used by the pipeline setup scripts)
    2. we override/populate all environment variables
    """
    # attempt to load the json settings file...
    try:
        config_data = _load_json_file(config_file_path)
    except DataPipelineNotConfigured:
        config_data = {}

    # override/populate everything that has an environment variable for it
    for setting in config_list:
        if setting in os.environ:
            config_data[setting] = os.environ[setting]

    config_data["region_name"] = get_current_region()

    # if there are any missing settings, fail with helpful error message
    missing_configs = [setting for setting in config_list if setting not in config_data]
    if missing_configs:
        raise DataPipelineNotConfigured(
                "could not find the following settings: %s" % ", ".join(missing_configs)
        )
    return config_data


def _load_json_file(file_path):
    try:
        with open(file_path) as fn:
            return json.load(fn)
    except IOError as e:
        raise DataPipelineNotConfigured(e)


def get_current_region():
    try:
        try:
            # AWS linux: full_region is of the form "placement: us-east-1a"
            full_region = check_output(["ec2-metadata", "--availability-zone"]).strip()
            return full_region.split(" ")[1][:-1]
        except Exception:
            # on ubuntu: sudo apt-get -y install cloud-utils, and the executable doesn't have a dash.
            # return is of the form "us-east-1b" possibly with whitespace.
            return check_output(["ec2metadata", "--availability-zone"]).strip()[:-1]
    except OSError:
        raise Exception("get_current_region can only be called on an amazon server with ec2metadata/ec2-metadata installed")

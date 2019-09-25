import json
import os
import re
from os.path import relpath, exists as file_exists
from time import sleep

from deployment_helpers.aws.iam import create_server_access_credentials
from deployment_helpers.aws.rds import get_full_db_credentials
from deployment_helpers.aws.s3 import create_data_bucket
from deployment_helpers.constants import (AWS_CREDENTIALS_FILE, get_global_config,
    GLOBAL_CONFIGURATION_FILE, get_aws_credentials,
    VALIDATE_GLOBAL_CONFIGURATION_MESSAGE, VALIDATE_AWS_CREDENTIALS_MESSAGE,
    get_pushed_full_processing_server_env_file_path, get_beiwe_environment_variables,
    get_beiwe_python_environment_variables_file_path, get_finalized_credentials_file_path,
    get_finalized_environment_variables, GLOBAL_CONFIGURATION_FILE_KEYS, AWS_CREDENTIALS_FILE_KEYS,
    get_server_configuration_file, get_server_configuration_file_path, RABBIT_MQ_PASSWORD_FILE)
from deployment_helpers.general_utils import log, random_alphanumeric_string, EXIT

# Sentry changed their default DSN formatting in early 2018, we test for the old and the new.
DSN_REGEX = re.compile('^(https://[\S]+:[\S]+@sentry\.io/[\S]+$|^https://[\S]+@sentry\.io/[\S]+$)')

####################################################################################################
################################### Reference Configs ##############################################
####################################################################################################

def reference_environment_configuration_file():
    return {
        "DOMAIN": "studies.mywebsite.com",
        "SENTRY_ELASTIC_BEANSTALK_DSN": "https://XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX@sentry.io/######",
        "SENTRY_DATA_PROCESSING_DSN": "https://XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX@sentry.io/######",
        "SENTRY_ANDROID_DSN": "https://XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX@sentry.io/######",
        "SENTRY_JAVASCRIPT_DSN": "https://XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX@sentry.io/######",
    }

def reference_data_processing_server_configuration():
    return {
        "WORKER_SERVER_INSTANCE_TYPE": "m4.large",
        "MANAGER_SERVER_INSTANCE_TYPE": "t2.medium",
        "ELASTIC_BEANSTALK_INSTANCE_TYPE": "t2.medium",
        "DB_SERVER_TYPE": "m4.large"
    }

####################################################################################################
################################### Reference Configs ##############################################
####################################################################################################

def _simple_validate_required(getter_func, file_path, appropriate_keys, display_name):
    """ returns False if invalid, True if valid.  For use with fully required keys, prints useful messages."""
    # try and load, fail usefully.
    try:
        json_config = getter_func()
    except Exception:
        log.error("could not load the %s file '%s'." % (display_name, file_path))
        sleep(0.1)
        return False  # could not load, did not pass
    
    # check for invalid values and keyserrors
    error_free = True
    for k, v in json_config.items():
        if k not in appropriate_keys:
            log.error("a key '%s' is present in %s, but was not expected." % (k, display_name))
            error_free = False
        if not v:
            error_free = False
            log.error("'%s' must be present in %s and have a value." % (k, display_name))
    
    for key in appropriate_keys:
        if key not in json_config:
            log.error("the key '%s' was expected in %s but not present." % (key, display_name))
            error_free = False
    
    sleep(0.1)  # python logging is dumb, wait so logs actually appear
    return error_free


def are_aws_credentials_present():
    ret = _simple_validate_required(get_aws_credentials,
                                    AWS_CREDENTIALS_FILE,
                                    AWS_CREDENTIALS_FILE_KEYS,
                                    relpath(AWS_CREDENTIALS_FILE))
    if not ret:
        log.error(VALIDATE_AWS_CREDENTIALS_MESSAGE)
    return ret


def is_global_configuration_valid():
    ret = _simple_validate_required(get_global_config,
                                    GLOBAL_CONFIGURATION_FILE,
                                    GLOBAL_CONFIGURATION_FILE_KEYS,
                                    relpath(GLOBAL_CONFIGURATION_FILE))
    if not ret:
        log.error(VALIDATE_GLOBAL_CONFIGURATION_MESSAGE)
    return ret


def ensure_nonempty_string(value, value_name, errors_list, subject):
    """
    Checks that an inputted value is a nonempty string
    :param value: A value to be checked
    :param value_name: The name of the value, to be used in the error string
    :param errors_list: The pass-by-reference list of error strings which we append to
    :return: Whether or not the value is in fact a nonempty string
    """
    if not isinstance(value, (str, unicode)):
        # log.error(value_name + " encountered an error")
        errors_list.append('({}) {} must be a string'.format(subject, value))
        return False
    elif not value:
        # log.error(value_name + " encountered an error")
        errors_list.append('({}) {} cannot be empty'.format(subject, value_name))
        return False
    else:
        return True


def validate_beiwe_environment_config(eb_environment_name):
    # DOMAIN_NAME
    # SENTRY_ANDROID_DSN
    # SENTRY_DATA_PROCESSING_DSN
    # SENTRY_ELASTIC_BEANSTALK_DSN
    # SENTRY_JAVASCRIPT_DSN
    # SYSADMIN_EMAILS
    errors = []
    try:
        aws_credentials = get_aws_credentials()
        global_config = get_global_config()
        beiwe_variables = get_beiwe_environment_variables(eb_environment_name)
    except Exception as e:
        log.error("encountered an error while trying to read configuration files.")
        log.error(e)
        EXIT(1)
    
    beiwe_variables_name = os.path.basename(get_beiwe_python_environment_variables_file_path(eb_environment_name))
    reference_environment_configuration_keys = reference_environment_configuration_file().keys()
    # Validate the data
    
    sysadmin_email = global_config.get('SYSTEM_ADMINISTRATOR_EMAIL', "")
    if not sysadmin_email:
        errors.append('(Global Configuration) System administrator email cannot be empty.')
    else:
        if not re.match('^[\S]+@[\S]+\.[\S]+$', sysadmin_email):
            errors.append('(Global Configuration) Invalid email address: {}'.format(sysadmin_email))
    
    # check sentry urls
    sentry_dsns = {
        "SENTRY_ELASTIC_BEANSTALK_DSN": beiwe_variables.get('SENTRY_ELASTIC_BEANSTALK_DSN', ''),
        "SENTRY_DATA_PROCESSING_DSN": beiwe_variables.get('SENTRY_DATA_PROCESSING_DSN', ''),
        "SENTRY_ANDROID_DSN": beiwe_variables.get('SENTRY_ANDROID_DSN', ''),
        "SENTRY_JAVASCRIPT_DSN": beiwe_variables.get('SENTRY_JAVASCRIPT_DSN', ''),
    }
    
    for name, dsn in sentry_dsns.items():
        if ensure_nonempty_string(dsn, name, errors, beiwe_variables_name):
            if not DSN_REGEX.match(dsn):
                errors.append('({}) Invalid DSN: {}'.format(beiwe_variables_name, dsn))
            # if name == "SENTRY_JAVASCRIPT_DSN":
            #     if not PUBLIC_DSN_REGEX.match(dsn):
            #         errors.append('({}) Invalid DSN: {}'.format(beiwe_variables_name, dsn))
            # elif not PRIVATE_DSN_REGEX.match(dsn):
            #     errors.append('({}) Invalid DSN: {}'.format(beiwe_variables_name, dsn))
                
    domain_name = beiwe_variables.get('DOMAIN', None)
    ensure_nonempty_string(domain_name, 'Domain name', errors, beiwe_variables_name)

    for key in reference_environment_configuration_keys:
        if key not in beiwe_variables:
            errors.append("{} is missing.".format(key))

    for key in beiwe_variables:
        if key not in reference_environment_configuration_keys:
            errors.append("{} is present but was not expected.".format(key))
    
    # Raise any errors
    if errors:
        for e in errors:
            log.error(e)
        sleep(0.1)  # python logging has some issues if you exit too fast... isn't it supposed to be synchronous?
        EXIT(1)  # forcibly exit, do not continue to run any code.
        
    # Check for presence of the server settings file:
    if not file_exists(get_server_configuration_file_path(eb_environment_name)):
        log.error("No server settings file exists at %s." % get_server_configuration_file_path(eb_environment_name))
        EXIT(1)
        
    # Put the data into one dict to be returned
    return {
        'DOMAIN_NAME': domain_name,
        'SYSADMIN_EMAILS': sysadmin_email,
        'SENTRY_ELASTIC_BEANSTALK_DSN': sentry_dsns['SENTRY_ELASTIC_BEANSTALK_DSN'],
        'SENTRY_DATA_PROCESSING_DSN': sentry_dsns['SENTRY_DATA_PROCESSING_DSN'],
        'SENTRY_ANDROID_DSN': sentry_dsns['SENTRY_ANDROID_DSN'],
        'SENTRY_JAVASCRIPT_DSN': sentry_dsns['SENTRY_JAVASCRIPT_DSN']
    }


def create_finalized_configuration(eb_environment_name):
    # requires an rds server has been created for the environment.
    # FLASK_SECRET_KEY
    # S3_BUCKET
    finalized_cred_path = get_finalized_credentials_file_path(eb_environment_name)
    if os.path.exists(finalized_cred_path):
        log.error("Encountered a finalized configuration file at %s." % finalized_cred_path)
        log.error("This file contains autogenerated parameters which must be identical between "
                  "data processing servers and the Elastic Beanstalk frontend servers.  This file "
                  "should not exist at this time, so the deployment process has been aborted.")
        EXIT(1)
    
    config = validate_beiwe_environment_config(eb_environment_name)
    config.update(get_full_db_credentials(eb_environment_name))
    config['FLASK_SECRET_KEY'] = random_alphanumeric_string(80)
    config["S3_BUCKET"] = create_data_bucket(eb_environment_name)
    config.update(create_server_access_credentials(config["S3_BUCKET"]))
    
    with open(finalized_cred_path, 'w') as f:
        json.dump(config, f, indent=1)
    return config


def create_processing_server_configuration_file(eb_environment_name):
    list_to_write = ['import os']
    
    for key, value in get_finalized_environment_variables(eb_environment_name).items():
        next_line = "os.environ['{key}'] = '{value}'".format(key=key.upper(), value=value)
        list_to_write.append(next_line)
    string_to_write = '\n'.join(list_to_write) + '\n'
    with open(get_pushed_full_processing_server_env_file_path(eb_environment_name), 'w') as fn:
        fn.write(string_to_write)


def create_rabbit_mq_password():
    with open(RABBIT_MQ_PASSWORD_FILE, 'w') as f:
        f.write(random_alphanumeric_string(20))

def get_rabbit_mq_password():
    with open(RABBIT_MQ_PASSWORD_FILE, 'r') as f:
        return f.read()

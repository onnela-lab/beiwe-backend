import json
import os
import re
from os.path import exists as file_exists, relpath
from time import sleep

from deployment_helpers.aws.iam import create_server_access_credentials
from deployment_helpers.aws.rds import get_full_db_credentials
from deployment_helpers.aws.s3 import create_data_bucket
from deployment_helpers.constants import (AWS_CREDENTIALS_FILE, AWS_CREDENTIALS_FILE_KEYS,
    DB_SERVER_TYPE, ELASTIC_BEANSTALK_INSTANCE_TYPE, get_aws_credentials,
    get_beiwe_environment_variables, get_beiwe_environment_variables_file_path,
    get_finalized_settings_file_path, get_finalized_settings_variables, get_global_config,
    get_pushed_full_processing_server_env_file_path, get_rabbit_mq_manager_ip_file_path,
    get_server_configuration_variables_path, GLOBAL_CONFIGURATION_FILE,
    GLOBAL_CONFIGURATION_FILE_KEYS, MANAGER_SERVER_INSTANCE_TYPE, VALIDATE_AWS_CREDENTIALS_MESSAGE,
    VALIDATE_GLOBAL_CONFIGURATION_MESSAGE, WORKER_SERVER_INSTANCE_TYPE)
from deployment_helpers.general_utils import EXIT, log, random_alphanumeric_string


# Sentry DSNs are key_value@some.domain.com/6ish_numbers
# Sentry has changed their DSN structure again, so I'm making the regex much weaker.
# (still matches legacy and legacy-legacy).  \S should not be confused with \s.
DSN_REGEX = re.compile(r'^https://[\S]+@[\S]+/[\S]+$')

####################################################################################################
################################### Reference Configs ##############################################
####################################################################################################

def reference_environment_configuration_file():
    return {
        "DOMAIN": "studies.mywebsite.com",
        "SENTRY_ELASTIC_BEANSTALK_DSN": "https://XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX@sentry.io/######",
        "SENTRY_DATA_PROCESSING_DSN": "https://XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX@sentry.io/######",
        "SENTRY_JAVASCRIPT_DSN": "https://XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX@sentry.io/######",
    }

def reference_data_processing_server_configuration():
    return {
        WORKER_SERVER_INSTANCE_TYPE: "m5.large",
        MANAGER_SERVER_INSTANCE_TYPE: "t3.medium",
        ELASTIC_BEANSTALK_INSTANCE_TYPE: "t3.medium",
        DB_SERVER_TYPE: "m5.large"
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
    
    # check for invalid values and keys errors
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
    if not isinstance(value, str):
        # log.error(value_name + " encountered an error")
        errors_list.append(f'({subject}) {value} must be a string')
        return False
    elif not value:
        # log.error(value_name + " encountered an error")
        errors_list.append(f'({subject}) {value_name} cannot be empty')
        return False
    else:
        return True


def email_param_validation(src_name: str, key_name: str, source: dict, errors: list):
    # we have to run this twice, pulled out into ugly function.
    email_string = source.get(key_name, "")
    if "," in email_string:
        errors.append(
            f'({src_name}) You can only have one email in {key_name}: {email_string}'
        )
    
    if not email_string:
        errors.append(f'({src_name}) {key_name} cannot be empty.')
    else:
        if not re.match(r'^[\S]+@[\S]+\.[\S]+$', email_string):
            errors.append(f'({src_name}) Invalid email address: {email_string}')


def validate_beiwe_environment_config(eb_environment_name):
    # DOMAIN_NAME
    # SENTRY_DATA_PROCESSING_DSN
    # SENTRY_ELASTIC_BEANSTALK_DSN
    # SENTRY_JAVASCRIPT_DSN
    # SYSADMIN_EMAILS
    
    finalized = os.path.exists(get_finalized_settings_file_path(eb_environment_name))
    errors = []
    try:
        aws_credentials = get_aws_credentials()
        global_config = get_global_config()
        beiwe_variables = get_beiwe_environment_variables(eb_environment_name)
        finalized_variables = get_finalized_settings_variables(eb_environment_name) if finalized else {}
    
    except Exception as e:
        log.error("encountered an error while trying to read configuration files.")
        log.error(e)
        beiwe_variables, global_config = None, None  # ide warnings
        EXIT(1)
    
    beiwe_variables_name = os.path.basename(get_beiwe_environment_variables_file_path(eb_environment_name))
    reference_environment_configuration_keys = reference_environment_configuration_file().keys()
    
    # Validation Start
    # Email validation
    email_param_validation(
        "Global Configuration", 'SYSTEM_ADMINISTRATOR_EMAIL', global_config, errors
    )
    # this configuration is possible on very old deployments
    if finalized:
        email_param_validation(
            "finalized settings and remote_db_env.py", 'SYSADMIN_EMAILS', finalized_variables, errors
        )
    
    # check sentry urls
    sentry_dsns = {
        "SENTRY_ELASTIC_BEANSTALK_DSN": beiwe_variables.get('SENTRY_ELASTIC_BEANSTALK_DSN', ''),
        "SENTRY_DATA_PROCESSING_DSN": beiwe_variables.get('SENTRY_DATA_PROCESSING_DSN', ''),
        "SENTRY_JAVASCRIPT_DSN": beiwe_variables.get('SENTRY_JAVASCRIPT_DSN', ''),
    }
    
    for name, dsn in sentry_dsns.items():
        if ensure_nonempty_string(dsn, name, errors, beiwe_variables_name):
            if not DSN_REGEX.match(dsn):
                errors.append(f'({beiwe_variables_name}) Invalid DSN: {dsn}')
    
    domain_name = beiwe_variables.get('DOMAIN', None)
    ensure_nonempty_string(domain_name, 'Domain name', errors, beiwe_variables_name)
    
    for key in reference_environment_configuration_keys:
        if key not in beiwe_variables:
            errors.append(f"{key} is missing.")
    
    for key in beiwe_variables:
        if key == "SENTRY_ANDROID_DSN":
            errors.append("'SENTRY_ANDROID_DSN' is no longer needed by the Beiwe Backend. "
                          "Please remove it from your environment variables settings file to continue.")
            continue
        if key not in reference_environment_configuration_keys:
            errors.append(f"{key} is present but was not expected.")
    
    # Raise any errors
    if errors:
        for e in errors:
            log.error(e)
        sleep(0.1)  # python logging has some issues if you exit too fast... isn't it supposed to be synchronous?
        EXIT(1)  # forcibly exit, do not continue to run any code.
    
    # Check for presence of the server settings file:
    if not file_exists(get_server_configuration_variables_path(eb_environment_name)):
        log.error(f"No server settings file exists at {get_server_configuration_variables_path(eb_environment_name)}.")
        EXIT(1)
    
    # Put the data into one dict to be returned
    sysadmin_email = finalized_variables["SYSADMIN_EMAILS"] \
        if finalized else global_config["SYSTEM_ADMINISTRATOR_EMAIL"]

    return {
        'DOMAIN_NAME': domain_name,
        'SYSADMIN_EMAILS': sysadmin_email,
        'SENTRY_ELASTIC_BEANSTALK_DSN': sentry_dsns['SENTRY_ELASTIC_BEANSTALK_DSN'],
        'SENTRY_DATA_PROCESSING_DSN': sentry_dsns['SENTRY_DATA_PROCESSING_DSN'],
        'SENTRY_JAVASCRIPT_DSN': sentry_dsns['SENTRY_JAVASCRIPT_DSN']
    }


def create_finalized_configuration(eb_environment_name):
    # requires an rds server has been created for the environment.
    # FLASK_SECRET_KEY
    # S3_BUCKET
    finalized_cred_path = get_finalized_settings_file_path(eb_environment_name)
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
    
    for key, value in get_finalized_settings_variables(eb_environment_name).items():
        next_line = f"os.environ['{key.upper()}'] = '{value}'"
        list_to_write.append(next_line)
    string_to_write = '\n'.join(list_to_write) + '\n'
    with open(get_pushed_full_processing_server_env_file_path(eb_environment_name), 'w') as fn:
        fn.write(string_to_write)


def create_rabbit_mq_password_file(eb_environment_name):
    with open(get_rabbit_mq_manager_ip_file_path(eb_environment_name), 'w') as f:
        f.write(random_alphanumeric_string(20))


def get_rabbit_mq_password(eb_environment_name):
    with open(get_rabbit_mq_manager_ip_file_path(eb_environment_name)) as f:
        return f.read()

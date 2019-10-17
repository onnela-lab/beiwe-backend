import os
from os.path import abspath, dirname, join, exists

# The explicit remote env file should be beiwe-backend/config/remote_db_env.py
EXPLICIT_REMOTE_ENV = join(abspath(dirname(__file__)), "remote_db_env.py")
ELASTIC_BEANSTALK_ENV = join(abspath(dirname(dirname(dirname(__file__)))), "env")
errors = []

if exists(EXPLICIT_REMOTE_ENV) and not exists(ELASTIC_BEANSTALK_ENV):
    import config.remote_db_env

if exists(EXPLICIT_REMOTE_ENV) or exists(ELASTIC_BEANSTALK_ENV):
    os.environ['DJANGO_DB_ENV'] = "remote"
    # If you are running with a remote database (e.g. on a server in a beiwe cluster) you need
    # some extra environment variables to be set.
    
    for env_var in ["RDS_DB_NAME", "RDS_USERNAME", "RDS_PASSWORD", "RDS_HOSTNAME"]:
        if env_var not in os.environ:
            errors.append("environment variable %s was not found" % env_var)
    
else:
    # if you are not running as part of a beiwe cluster (e.g. for local development)
    # we configure django to use a locale sqlite database.
    os.environ['DJANGO_DB_ENV'] = "local"


from config import settings, constants
provided_settings = vars(settings)

# Check that all values provided actually contain something
for attr_name, attr_value in provided_settings.items():
    if not attr_value and attr_name[0] != '_':
        errors.append(attr_name + " was not provided with a value.")

MANDATORY_VARS = {
    'DOMAIN_NAME',
    'FLASK_SECRET_KEY',
    'IS_STAGING',
    'S3_BUCKET',
    'SENTRY_ANDROID_DSN',
    'SENTRY_DATA_PROCESSING_DSN',
    'SENTRY_ELASTIC_BEANSTALK_DSN',
    'SENTRY_JAVASCRIPT_DSN',
    'SYSADMIN_EMAILS',
}

# Check that all the mandatory variables exist...
for mandatory_var in MANDATORY_VARS:
    if mandatory_var not in provided_settings:
        errors.append(mandatory_var + " was not provided in your settings.")

# Environment variables might be unpredictable, so we sanitize the numerical ones as ints.
constants.DEFAULT_S3_RETRIES = int(constants.DEFAULT_S3_RETRIES)
constants.CONCURRENT_NETWORK_OPS = int(constants.CONCURRENT_NETWORK_OPS)
constants.FILE_PROCESS_PAGE_SIZE = int(constants.FILE_PROCESS_PAGE_SIZE)
constants.CELERY_EXPIRY_MINUTES = int(constants.CELERY_EXPIRY_MINUTES)

# email addresses are parsed from a comma separated list
# whitespace before and after addresses are stripped
if settings.SYSADMIN_EMAILS:
    settings.SYSADMIN_EMAILS = [_email_address.strip()
                                for _email_address in settings.SYSADMIN_EMAILS.split(",")]

# IS_STAGING needs to resolve to False except under specific settings.
# The default needs to be production.
if settings.IS_STAGING is True or settings.IS_STAGING.upper() == "TRUE":
    settings.IS_STAGING = True
else:
    settings.IS_STAGING = False

#
# Stick any warning about environment variables that may have changed here
#
old_credentials_warning = \
"WARNING: This runtime environment is be using the out-of-date environment variable '%s', " \
"please change it to the new environment variable '%s'. (The system will continue to work " \
"with the old environment variable).\n"

pipeline_warning = \
"(Due to the condition below you may need to update the access credentials in order to use " \
"the Data Pipeline feature of Beiwe.)"

if os.getenv("S3_ACCESS_CREDENTIALS_USER") and not os.getenv("BEIWE_SERVER_AWS_ACCESS_KEY_ID"):
    print(pipeline_warning)
    print(old_credentials_warning % ("S3_ACCESS_CREDENTIALS_USER", "BEIWE_SERVER_AWS_ACCESS_KEY_ID"))
    

if os.getenv("S3_ACCESS_CREDENTIALS_KEY") and not os.getenv("BEIWE_SERVER_AWS_SECRET_ACCESS_KEY"):
    print(pipeline_warning)
    print(old_credentials_warning % ("S3_ACCESS_CREDENTIALS_KEY", "BEIWE_SERVER_AWS_SECRET_ACCESS_KEY"))

#
# print a useful error and cease execution if any required environment variables showed up.
if errors:
    class BadServerConfigurationError(Exception): pass
    raise BadServerConfigurationError("\n" + "\n".join(sorted(errors)))

import os
from os.path import abspath, dirname, exists as file_exists, join

# stick all errors into this list and raise a special exception at the end.
ERRORS = []

# The explicit remote env file should be beiwe-backend/config/remote_db_env.py
CELERY_SERVER_ENV_FILE = join(abspath(dirname(__file__)), "remote_db_env.py")
ELASTIC_BEANSTALK_ENV_FILE = join(abspath(dirname(dirname(dirname(__file__)))), "env")
ELASTIC_BEANSTALK_2_VENV_FILE = join(abspath(dirname(dirname(dirname(__file__)))), "venv")
POSTGRES_DATABASE_SETTINGS = ("RDS_DB_NAME", "RDS_USERNAME", "RDS_PASSWORD", "RDS_HOSTNAME")
SENTRY_ENVS = ('SENTRY_DATA_PROCESSING_DSN', 'SENTRY_ELASTIC_BEANSTALK_DSN','SENTRY_JAVASCRIPT_DSN')

SENTRY_DISABLED = not all(SENTRY_ENVS)
SENTRY_ENABLED = not SENTRY_DISABLED
if SENTRY_DISABLED:
    print("\nRunning with Sentry disabled")
    SENTRY_DATA_PROCESSING_DSN = None
    SENTRY_ELASTIC_BEANSTALK_DSN = None
    SENTRY_JAVASCRIPT_DSN = None

# these variables are strictly required.
MANDATORY_VARS = {
    'DOMAIN_NAME',
    'FLASK_SECRET_KEY',
    'S3_BUCKET',
    'SYSADMIN_EMAILS',
}

# Evaluate the config.remote_db_env file if it exists.  This file is auto generated during the
# deploy process and populates the environment variables on the data processing servers
if file_exists(CELERY_SERVER_ENV_FILE):
    import config.remote_db_env

# postgres database details
for env_var in POSTGRES_DATABASE_SETTINGS:
    if env_var not in os.environ:
        ERRORS.append(f"Environment variable '{env_var}' was not found.")

####################################################################################################
#### Start introspecting to validate parameters and inform user of missing required parameters. ####
####################################################################################################

from config import settings
PROVIDED_SETTINGS = vars(settings)

# Check that all the mandatory variables exist...
for mandatory_var in MANDATORY_VARS:
    if mandatory_var not in PROVIDED_SETTINGS:
        ERRORS.append(f"'{mandatory_var}' was not provided in your settings.")
    if mandatory_var in PROVIDED_SETTINGS and not PROVIDED_SETTINGS[mandatory_var]:
        ERRORS.append(f"The setting '{mandatory_var}' was not provided with a value.")

# Environment variable type can be unpredictable, sanitize the numerical ones.
settings.FILE_PROCESS_PAGE_SIZE = int(settings.FILE_PROCESS_PAGE_SIZE)
settings.INACTIVITY_LOCKOUT_DAYS = int(settings.INACTIVITY_LOCKOUT_DAYS)

# email addresses are parsed from a comma separated list, strip whitespace.
if settings.SYSADMIN_EMAILS:
    settings.SYSADMIN_EMAILS = settings.SYSADMIN_EMAILS.strip().strip(",")

# DATA_DELETION_USERTYPE must be a specific usertype.
if settings.DATA_DELETION_USERTYPE not in ("study_researcher", "study_admin", "site_admin"):
    ERRORS.append(f"DATA_DELETION_USERTYPE must be one of 'researcher', 'study_admin', or 'site_admin', got '{settings.DATA_DELETION_USERTYPE}'.")

# we currently are limiting compression level due to the dfast parameter
settings.DATA_COMPRESSION_LEVEL = int(settings.DATA_COMPRESSION_LEVEL)
if settings.DATA_COMPRESSION_LEVEL > 4:
    ERRORS.append(f"DATA_COMPRESSION_LEVEL must be less than 4, got '{settings.DATA_COMPRESSION_LEVEL}'.")

#
# Stick any warning about environment variables that may have changed here
#
old_credentials_warning = \
    "WARNING: This runtime environment is be using the out-of-date environment variable '%s', " \
    "please change it to the new environment variable '%s'. (The system will continue to work " \
    "with the old environment variable).\n"

if os.getenv("S3_ACCESS_CREDENTIALS_USER") and not os.getenv("BEIWE_SERVER_AWS_ACCESS_KEY_ID"):
    print(old_credentials_warning % ("S3_ACCESS_CREDENTIALS_USER", "BEIWE_SERVER_AWS_ACCESS_KEY_ID"))


if os.getenv("S3_ACCESS_CREDENTIALS_KEY") and not os.getenv("BEIWE_SERVER_AWS_SECRET_ACCESS_KEY"):
    print(old_credentials_warning % ("S3_ACCESS_CREDENTIALS_KEY", "BEIWE_SERVER_AWS_SECRET_ACCESS_KEY"))


# print a useful error and cease execution if any required environment variables showed up.
if ERRORS:
    class BadServerConfigurationError(Exception): pass
    raise BadServerConfigurationError("\n" + "\n".join(sorted(ERRORS)))

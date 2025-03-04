from os import getenv


"""
Keep this document legible for non-developers, it is linked in the ReadMe and the wiki, and is the
official documentation for all runtime parameters.

On data processing servers, instead of environment varables, append a line to your
config/remote_db_env.py file, formatted like this:
    os.environ['S3_BUCKET'] = 'bucket_name'

For options below that use this syntax:
    getenv('BLOCK_QUOTA_EXCEEDED_ERROR', 'false').lower() == 'true'
This means Beiwe is looking for the word 'true' but also accepts "True", "TRUE", etc.
If not provided with a value, or provided with any other value, they will be treated as false.
"""

#
# General server settings
#

# Credentials for running AWS operations, like retrieving data from S3 (AWS Simple Storage Service)
#  This parameter was renamed in the past, we continue to check for the old variable name in order
#  to support older deployments that have been upgraded over time.
BEIWE_SERVER_AWS_ACCESS_KEY_ID: str = getenv("BEIWE_SERVER_AWS_ACCESS_KEY_ID") or getenv("S3_ACCESS_CREDENTIALS_USER")
BEIWE_SERVER_AWS_SECRET_ACCESS_KEY: str = getenv("BEIWE_SERVER_AWS_SECRET_ACCESS_KEY") or getenv("S3_ACCESS_CREDENTIALS_KEY")

# This is the secret key for the website, mostly it is used to sign cookies. You should provide a
#  long string with high quality random characters. Recommend keeping it alphanumeric for safety.
# (Beiwe started as a Flask app, so for legacy reasons we just have never updated this parameter.)
FLASK_SECRET_KEY: str = getenv("FLASK_SECRET_KEY")

# The name of the S3 bucket that will be used to store user generated data.

S3_BUCKET: str = getenv("S3_BUCKET")

# The endpoint for the S3 bucket, this is used to specify a non-AWS S3 compatible service.
S3_ENDPOINT = getenv("S3_ENDPOINT", None)

# S3 region (not all regions have S3, so this value may need to be specified)
#  Defaults to us-east-1, A.K.A. US East (N. Virginia),
S3_REGION_NAME: str = getenv("S3_REGION_NAME", "us-east-1")

# Domain name for the server, this is used for various details, and should be match the address of
#  the frontend server.
DOMAIN_NAME: str = getenv("DOMAIN_NAME")

# A list of email addresses that will receive error emails. This value must be a comma separated
#  list; whitespace before and after addresses will be stripped.
# (This variable may be removed entirely or replaced with a database setting in the future.)
SYSADMIN_EMAILS: str = getenv("SYSADMIN_EMAILS")

# Sentry DSNs for error reporting
# While technically optional, we strongly recommended creating a sentry account populating
#  these parameters.  Very little support is possible without it.
SENTRY_DATA_PROCESSING_DSN = getenv("SENTRY_DATA_PROCESSING_DSN")
SENTRY_ELASTIC_BEANSTALK_DSN = getenv("SENTRY_ELASTIC_BEANSTALK_DSN")
SENTRY_JAVASCRIPT_DSN = getenv("SENTRY_JAVASCRIPT_DSN")

# Location of the downloadable Android APK file that'll be served from /download
DOWNLOADABLE_APK_URL: str = getenv(
    "DOWNLOADABLE_APK_URL",
    "https://beiwe-app-backups.s3.amazonaws.com/release/Beiwe-LATEST-commStatsCustomUrl.apk"
)

#
# File processing and Data Access API options
#

# This is number of files to be pulled in and processed simultaneously on data processing servers,
# it has no effect on frontend servers. Mostly this affects the ram utilization of file processing.
# A larger "page" of files to process is more efficient with respect to network bandwidth (and
# therefore S3 costs), but will use more memory. Individual file sizes ranges from bytes to tens of
# megabytes, so memory usage can be spikey and difficult to predict.
#   Expects an integer number.
FILE_PROCESS_PAGE_SIZE: int = getenv("FILE_PROCESS_PAGE_SIZE", 100)

#
# Push Notification directives
#

# The number of attempts when sending push notifications to unreachable devices. Send attempts run
# every 6 minutes, a value of 720 is 3 days. (24h * 3days * 10 attempts per hour = 720)
PUSH_NOTIFICATION_ATTEMPT_COUNT: int = getenv("PUSH_NOTIFICATION_ATTEMPT_COUNT", 720)

# Disables the QuotaExceededError in push notifications.  Enable if this error drowns your Sentry
# account. Note that under the conditions where you need to enable this flag, those events will
# still cause push notification failures, which interacts with PUSH_NOTIFICATION_ATTEMPT_COUNT, so
# you may want to raise that value.
#   Expects (case-insensitive) "true" to block errors.
BLOCK_QUOTA_EXCEEDED_ERROR: bool = getenv('BLOCK_QUOTA_EXCEEDED_ERROR', 'false').lower() == 'true'

#
# User Authentication and Permissions
#

#
# Global MFA setting
# This setting forces site admin users to enable MFA on their accounts.  There is already a 20
# character password requirement so this is an opt-in, deployment-specific parameter.
REQUIRE_SITE_ADMIN_MFA: bool = getenv('REQUIRE_SITE_ADMIN_MFA', 'false').lower() == 'true'

# Allow data deletion usertype setting
# This setting restricts the type of user that can dispatch data deletion on a participant.
# Valid values are study_admin, study_researcher, and site_admin.
# (This feature will eventually be replaced with a database setting.)
DATA_DELETION_USERTYPE: str = getenv('DATA_DELETION_USERTYPE', 'study_researcher')

#
# Developer options
#

# Developer debugging settings for working on decryption issues, which are particularly difficult to
# manage and may require storing [substantially] more data than there is in a Sentry error report.
#   Expects (case-insensitive) "true" to enable, otherwise it is disabled.
# todo: remove this setting.
STORE_DECRYPTION_LINE_ERRORS: bool = getenv('STORE_DECRYPTION_LINE_ERRORS', 'false').lower() == 'true'

# upload logging is literally the logging of details of file uploads from mobile devices.
# (most logging is limited to a single file, this particular logging is spread across multiple
# files that would have a cross-import, so it needs to be stuck elsewhere.)
# (This will eventually be replaced with better logging controls.)
UPLOAD_LOGGING_ENABLED: bool = getenv('UPLOAD_LOGGING_ENABLED', 'false').lower() == 'true'

# Some features for study participants are experimental or in-development, so access to them is not
# enabled by default. These features are not guaranteed to work or may be removed without notice.
# These features should not be relied upon by any studies without supervision by a Beiwe sofware
# developer.
# Even with this enabled only site admins have access to the experiment settings, which can be found
# under a new option on the view participant page.
ENABLE_EXPERIMENTS: bool = getenv('ENABLE_EXPERIMENTS', 'false').lower() == 'true'

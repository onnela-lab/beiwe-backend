from os import cpu_count, getenv

"""
Keep this document legible for non-developers, it is linked in ReadMe, and is the official
documentation for all runtime parameters.

On data processing servers, instead of environment varables, append a line to your
config/remote_db_env.py file, formatted like this:
    os.environ['S3_BUCKET'] = 'bucket_name'

For options below that use this syntax:
    getenv('REPORT_DECRYPTION_KEY_ERRORS', 'false').lower() == 'true'
This means Beiwe is looking for the word 'true' and also accept "True", "TRUE", etc.
If not provided with a value, or provided with any other value, they will be treated as false.
"""

# Credentials for running AWS operations, like retrieving data from S3 (AWS Simple Storage Service)
#  This parameter was renamed in the past, we continue to check for the old variable name in order
#  to support older deployments that have been upgraded over time.
BEIWE_SERVER_AWS_ACCESS_KEY_ID = getenv("BEIWE_SERVER_AWS_ACCESS_KEY_ID") or getenv("S3_ACCESS_CREDENTIALS_USER")
BEIWE_SERVER_AWS_SECRET_ACCESS_KEY = getenv("BEIWE_SERVER_AWS_SECRET_ACCESS_KEY") or getenv("S3_ACCESS_CREDENTIALS_KEY")

# This is the secret key for the website, mostly it is used to sign cookies. You should provide a
#  long string with high quality random characters. Recommend keeping it alphanumeric for safety.
FLASK_SECRET_KEY = getenv("FLASK_SECRET_KEY")

# The name of the S3 bucket that will be used to store user generated data.
S3_BUCKET = getenv("S3_BUCKET")

# Domain name for the server, this is used for various details, and should be match the address of
#  the frontend server.
DOMAIN_NAME = getenv("DOMAIN_NAME")

# A list of email addresses that will receive error emails. This value must be a comma separated
# list; whitespace before and after addresses will be stripped.
SYSADMIN_EMAILS = getenv("SYSADMIN_EMAILS")

# Sentry DSNs for error reporting
# While technically optional, we strongly recommended creating a sentry account populating
# these parameters.  Very little support is possible without it.
SENTRY_DATA_PROCESSING_DSN = getenv("SENTRY_DATA_PROCESSING_DSN")
SENTRY_ELASTIC_BEANSTALK_DSN = getenv("SENTRY_ELASTIC_BEANSTALK_DSN")
SENTRY_JAVASCRIPT_DSN = getenv("SENTRY_JAVASCRIPT_DSN")

# S3 region (not all regions have S3, so this value may need to be specified)
#  Defaults to us-east-1, A.K.A. US East (N. Virginia),
S3_REGION_NAME = getenv("S3_REGION_NAME", "us-east-1")

# Location of the downloadable Android APK file that'll be served from /download
DOWNLOADABLE_APK_URL = getenv("DOWNLOADABLE_APK_URL", "https://s3.amazonaws.com/beiwe-app-backups/release/Beiwe-2.4.1-onnelaLabServer-release.apk")

#
# File processing options

# Modifies the number of concurrent network operations that the server will use with respect to
# accessing data on S3.  Note that frontend servers can have different values than data processing
# servers.  By default this is based on the CPU core count.
#   Expects an integer number.
CONCURRENT_NETWORK_OPS = getenv("CONCURRENT_NETWORK_OPS") or cpu_count() * 2

# This is number of files to be pulled in and processed simultaneously on data processing servers,
# it has no effect on frontend servers. Mostly this affects the ram utilization of file processing.
# A larger "page" of files to process is more efficient with respect to network bandwidth (and
# therefore S3 costs), but will use more memory. Individual file size ranges from bytes to tens of
# megabytes, so memory usage can be spikey and difficult to predict.
#   Expects an integer number.
FILE_PROCESS_PAGE_SIZE = getenv("FILE_PROCESS_PAGE_SIZE", 250)

#
# Push Notification directives

# The number of attempts when sending push notifications to unreachable devices. Send attempts run
# every 6 minutes, a value of 720 is 3 days. (24h * 3days * 10 attempts per hour = 720)
PUSH_NOTIFICATION_ATTEMPT_COUNT = getenv("PUSH_NOTIFICATION_ATTEMPT_COUNT", 720)

# Disables the QuotaExceededError in push notifications.  Enable if this error drowns your Sentry
# account. Note that under the conditions where you need to enable this flag, those events will
# still cause push notification failures, which interacts with PUSH_NOTIFICATION_ATTEMPT_COUNT, so
# you may want to raise that value.
#   Expects (case-insensitive) "true" to block errors.
BLOCK_QUOTA_EXCEEDED_ERROR = getenv('BLOCK_QUOTA_EXCEEDED_ERROR', 'false').lower() == 'true'

#
# Developer options

# Developer debugging settings for working on decryption issues, which are particularly difficult to
# manage and may require storing [substantially] more data than there is in a Sentry error report.
#   Expects (case-insensitive) "true" to enable, otherwise it is disabled.
REPORT_DECRYPTION_KEY_ERRORS = getenv('REPORT_DECRYPTION_KEY_ERRORS', 'false').lower() == 'true'
STORE_DECRYPTION_KEY_ERRORS = getenv('STORE_DECRYPTION_KEY_ERRORS', 'false').lower() == 'true'
STORE_DECRYPTION_LINE_ERRORS = getenv('STORE_DECRYPTION_LINE_ERRORS', 'false').lower() == 'true'

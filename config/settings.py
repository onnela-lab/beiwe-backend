from os import cpu_count, getenv

"""
On processing servers, instead of environment varables, append a line to config/remote_db_env.py,
such as: os.environ['S3_BUCKET'] = 'bucket_name'
"""

# for we retain the old access key in case someone deployed a super old EB server.
BEIWE_SERVER_AWS_ACCESS_KEY_ID = getenv("BEIWE_SERVER_AWS_ACCESS_KEY_ID") or getenv("S3_ACCESS_CREDENTIALS_USER")
BEIWE_SERVER_AWS_SECRET_ACCESS_KEY = getenv("BEIWE_SERVER_AWS_SECRET_ACCESS_KEY") or getenv("S3_ACCESS_CREDENTIALS_KEY")

# This is the secret key for the website. Mostly it is used to sign cookies. You should provide a
#  cryptographically secure string to this value.
FLASK_SECRET_KEY = getenv("FLASK_SECRET_KEY")

# The name of the s3 bucket that will be used to store user generated data, and backups of local
# database information.
S3_BUCKET = getenv("S3_BUCKET")

# Domain name for the server
DOMAIN_NAME = getenv("DOMAIN_NAME")

# A list of email addresses that will receive error emails. This value must be a comma separated
# list; whitespace before and after addresses will be stripped.
SYSADMIN_EMAILS = getenv("SYSADMIN_EMAILS")

# Sentry DSNs (technically optional)
SENTRY_DATA_PROCESSING_DSN = getenv("SENTRY_DATA_PROCESSING_DSN")
SENTRY_ELASTIC_BEANSTALK_DSN = getenv("SENTRY_ELASTIC_BEANSTALK_DSN")
SENTRY_JAVASCRIPT_DSN = getenv("SENTRY_JAVASCRIPT_DSN")

# Production/Staging: set to "TRUE" if this is a staging/testing/development server to enable some
# extra features.
IS_STAGING = getenv("IS_STAGING") or "PRODUCTION"
REPORT_DECRYPTION_KEY_ERRORS = bool(getenv("REPORT_DECRYPTION_KEY_ERRORS", False))
STORE_DECRYPTION_KEY_ERRORS = bool(getenv("STORE_DECRYPTION_KEY_ERRORS", False))

# S3 region (not all regions have S3, so this value may need to be specified)
S3_REGION_NAME = getenv("S3_REGION_NAME", "us-east-1")

# Location of the downloadable Android APK file that'll be served from /download
DOWNLOADABLE_APK_URL = getenv("DOWNLOADABLE_APK_URL", "https://s3.amazonaws.com/beiwe-app-backups/release/Beiwe-2.4.1-onnelaLabServer-release.apk")

# File processing directives
# Used in data download and data processing, base this on CPU core count.
CONCURRENT_NETWORK_OPS = getenv("CONCURRENT_NETWORK_OPS") or cpu_count() * 2
# Used in file processing, number of files to be pulled in and processed simultaneously. Mostly this
# changes the ram utilization of file processing, higher is more efficient on network bandwidth, but
# will use more memory.  Individual file size ranges from bytes to tens of megabytes.
FILE_PROCESS_PAGE_SIZE = getenv("FILE_PROCESS_PAGE_SIZE", 250)

# number of attempts on sending push notifications to unreachable devices. Send attempts run every
# 6 minutes.  720 is 3 days (24h * 3days * 10 attempts per hour = 720)
PUSH_NOTIFICATION_ATTEMPT_COUNT = getenv("PUSH_NOTIFICATION_ATTEMPT_COUNT", 720)

# Disables the QuotaExceededError in push notifications if any non-blank string is provided. Note
# that under the conditions where you need to enable this flag, those events will cause push
# notification failures, which interacts with PUSH_NOTIFICATION_ATTEMPT_COUNT.
BLOCK_QUOTA_EXCEEDED_ERROR = bool(getenv("BLOCK_QUOTA_EXCEEDED_ERROR", False))

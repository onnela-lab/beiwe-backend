from subprocess import check_output

from django.utils import timezone

from constants.common_constants import LOGS_FOLDER
from database.models import S3File
from libs.s3 import s3_upload_plaintext
from libs.utils.compression import compress


"""
This script runs periodically. The default system logrotate periodicity should be weekly, so as long
as this script is run more frequently than that all log data in the auth log will be uploaded.
"""

def main():
    with open("/var/log/auth.log", "rb") as f:
        auth_log = f.read()
    
    log_size_uncompressed = len(auth_log)
    auth_log = compress(auth_log)  # compress the log file
    log_size_compressed = len(auth_log)
    
    now = timezone.now().isoformat()
    # should be something like ip-172-31-67-107
    hostname = check_output("hostname").strip().decode()
    path = f"{LOGS_FOLDER}/auth_log/{hostname}-{now}.log"
    # file name should sort by hostname then date
    s3_upload_plaintext(path, auth_log)
    print("uploaded auth_log")
    
    S3File.objects.create(
        path=path, size_uncompressed=log_size_uncompressed, size_compressed=log_size_compressed
    )

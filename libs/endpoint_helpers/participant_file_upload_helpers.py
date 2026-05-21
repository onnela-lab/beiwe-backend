import logging

from django.core.exceptions import ValidationError
from django.db import IntegrityError
from django.http.response import HttpResponse
from django.utils import timezone

from config.settings import UPLOAD_LOGGING_ENABLED
from constants.common_constants import PROBLEM_UPLOADS
from constants.message_strings import (S3_FILE_PATH_UNIQUE_CONSTRAINT_ERROR_1 as ERR1,
    S3_FILE_PATH_UNIQUE_CONSTRAINT_ERROR_2 as ERR2)
from database.models import FileToProcess, Participant, UploadTracking
from libs.encryption import DeviceDataDecryptor
from libs.s3 import s3_upload, smart_s3_list_study_files
from libs.utils.file_name_utils import generate_duplicate_name
from libs.utils.security_utils import generate_easy_alphanumeric_string

log = logging.getLogger(__name__)
if UPLOAD_LOGGING_ENABLED:
    log.setLevel(logging.DEBUG)


class NewDuplicateErrorScenario(Exception): pass


def upload_and_create_file_to_process_and_log(
    s3_path: str, participant: Participant, decryptor: DeviceDataDecryptor
) -> HttpResponse:
    
    # test if the file exists on s3, handle ios duplicate file merge.
    if not list(smart_s3_list_study_files(s3_path, participant)):
        s3_upload(s3_path, decryptor.decrypted_file, participant)
        duplicate = False
    else:
        # duplicate file
        old_file_location = s3_path
        s3_path = generate_duplicate_name(s3_path)
        log.debug(f"renamed duplicate '{old_file_location}' to '{s3_path}'")
        s3_upload(s3_path, decryptor.decrypted_file, participant)
        duplicate = True
    
    if not duplicate:
        # race condition: multiple _concurrent_ uploads with same file path. Behavior without
        # try-except is correct, but we don't care about reporting it. Just send the device a 500
        # error so it skips the file, the followup attempt receives 200 code and deletes the file.
        try:
            FileToProcess.append_file_for_processing(s3_path, participant)
        except (IntegrityError, ValidationError) as e:
            # there are 2 error cases that can occur here (race condition with 2 concurrent uploads)
            if ERR1 in str(e) or ERR2 in str(e):
                # don't abort 500, we want to limit 500 errors on the ELB in production (uhg)
                log.debug(f"backoff for duplicate race condition, {e}")
                return HttpResponse(content=b"backoff, duplicate race condition.", status=400)
            
            msg = f"Unexpected error creating FileToProcess for {s3_path}: {e}"
            raise NewDuplicateErrorScenario(msg) from e
    
    # record that an upload occurred
    UploadTracking.objects.create(
        file_path=s3_path,
        file_size=len(decryptor.decrypted_file),
        timestamp=timezone.now(),
        participant=participant,
    )
    return HttpResponse(content=b"upload successful.", status=200)


def upload_problem_file(
    file_contents: bytes, participant: Participant, s3_file_path: str, exception: Exception
):
    file_path = f"{PROBLEM_UPLOADS}/{participant.study.object_id}/" + s3_file_path + generate_easy_alphanumeric_string(10)
    s3_upload(file_path, file_contents, participant, raw_path=True)

from django.utils import timezone

from database.system_models import DataProcessingStatus
from libs.celery_control import CeleryScriptTask, DAILY, HOURLY, SIX_MINUTELY
from scripts import (purge_participant_data, repopulate_push_notifications,
    script_that_compresses_s3_data, script_that_deletes_known_junk_uploads,
    script_that_removes_data_from_invalid_time_sources, update_forest_version, upload_logs)


"""
To create a "script" just wrap it in @CeleryScriptTask(), name it to start with "six_minutes_",
"hourly_", or "daily_".  See CeleryScriptTask for details, but it handles wrapping in error
reporting, warning of long runs, etc. etc.
"""


# trunk-ignore(ruff/E402)
from libs.celery_control import scripts_celery_app  # must be in the namespace for celery to work.


####################################### Six Minutely ###############################################

#
## Check the forest version in the update_forest_version script
#
@CeleryScriptTask()
def six_minutes_update_forest_version():
    update_forest_version.main()


@CeleryScriptTask()
def six_minutes_update_background_last_run():
    DataProcessingStatus.singleton().update_only(last_run=timezone.now())

######################################### Hourly ###################################################

#
## Push Notification
#
@CeleryScriptTask()
def hourly_run_push_notification_scheduledevent_rebuild():
    repopulate_push_notifications.main()

#
## Participant data deletion
#
@CeleryScriptTask()
def hourly_participant_data_deletion():
    purge_participant_data.main()

######################################### Daily ####################################################

#
## Upload the ssh auth log to S3 - this is a very basic security/audit measure, so we just do it.
#
@CeleryScriptTask()
def daily_upload_logs():
    upload_logs.main()

#
## Purge all data that is from impossible timestamps - we test for this now, but have still seen it.
#
@CeleryScriptTask()
def daily_purge_invalid_time_data():
    script_that_removes_data_from_invalid_time_sources.main()

#
## Compress the S3 bucket access logs - these files are only present if a bucket policy is in place
## to log access.  It creates a LOT of files, so we compress them daily.  
#
@CeleryScriptTask()
def daily_compress_bucket_access_logs():
    script_that_compresses_s3_data.compress_s3_logging_logs()

#
## deletes files on s3 that are known to be junk (currently defined as csvs just containing headers)
#
@CeleryScriptTask()
def daily_clear_s3_known_junk_uploads():
    script_that_deletes_known_junk_uploads.main()


####################################################################################################
############################# Script Runner Logic ##################################################
####################################################################################################


def enqueue_six_minute_scripts_tasks():
    CeleryScriptTask.enqueue_tasks(SIX_MINUTELY)


def enqueue_hourly_scripts_tasks():
    CeleryScriptTask.enqueue_tasks(HOURLY)


def enqueue_daily_script_tasks():
    CeleryScriptTask.enqueue_tasks(DAILY)

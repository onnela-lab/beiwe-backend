from libs.celery_control import CeleryScriptTask, DAILY, HOURLY, SIX_MINUTELY
from scripts import (purge_participant_data, repopulate_push_notifications,
    script_that_removes_data_from_invalid_time_sources, update_forest_version, upload_logs)


"""
To create a "script" just wrap it in @CeleryScriptTask(), name it to start with "six_minutes_",
"hourly_", or "daily_".  See CeleryScriptTask for details, but it handles wrapping in error
reporting, warning of long runs, etc. etc.
"""

from libs.celery_control import scripts_celery_app  # must be in the namespace for celery to work.


####################################### Six Minutely ###############################################

#
## Check the forest version in the update_forest_version script
#
@CeleryScriptTask()
def six_minutes_update_forest_version():
    update_forest_version.main()


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


####################################################################################################
############################# Script Runner Logic ##################################################
####################################################################################################


def enqueue_six_minute_scripts_tasks():
    CeleryScriptTask.enqueue_tasks(SIX_MINUTELY)


def enqueue_hourly_scripts_tasks():
    CeleryScriptTask.enqueue_tasks(HOURLY)


def enqueue_daily_script_tasks():
    CeleryScriptTask.enqueue_tasks(DAILY)

import importlib
from datetime import timedelta
from modulefinder import Module
from time import sleep
from typing import Callable

from django.utils import timezone

from constants.celery_constants import SCRIPTS_QUEUE
from database.study_models import Study
from database.system_models import GlobalSettings
from libs.celery_control import safe_apply_async, scripts_celery_app
from libs.schedules import repopulate_all_survey_scheduled_events
from libs.sentry import make_error_sentry, SentryTypes


SIX_MINUTELY = "six_minutely"  # SOME DAY we will have better than 6 minute minute celery tasks.
HOURLY = "hourly"
DAILY = "daily"


SCRIPT_ERROR_SENTRY = make_error_sentry(sentry_type=SentryTypes.script_runner)  # we only need one.


def queue_script(a_celery_task: Callable, expiry: str):
    """ Forces enqueueing with an expiry. """
    if expiry not in (SIX_MINUTELY, HOURLY, DAILY):
        raise ValueError("Expiry must be one of the constants in this file.")
    
    if expiry == SIX_MINUTELY:
        expiry = timezone.now() + timedelta(minutes=6)
    if expiry == DAILY:
        expiry = timezone.now() + timedelta(hours=24)
    if expiry == HOURLY:
        expiry = timezone.now() + timedelta(hours=1)
    expiry = expiry.replace(second=0, microsecond=0)  # clear out seconds and microseconds
    
    print(f"Queueing script '{a_celery_task.__name__}', expires at {expiry}")
    safe_apply_async(
        a_celery_task,
        max_retries=0,
        expires=expiry,
        task_track_started=True,
        task_publish_retry=False,
        retry=False,
    )


####################################### Six Minutely ###############################################


#
## Check the forest version in the update_forest_version script
#
def create_task_update_celery_version():
    with SCRIPT_ERROR_SENTRY:
        print("Queueing update celery version task.")
        queue_script(celery_update_forest_version, SIX_MINUTELY)


@scripts_celery_app.task(queue=SCRIPTS_QUEUE)
def celery_update_forest_version():
    with SCRIPT_ERROR_SENTRY:
        print("running script update_forest_version.")
        from scripts import update_forest_version
        update_forest_version.main()


######################################### Hourly ###################################################

#
## push notification 
#

def create_task_run_push_notification_scheduledevent_rebuild():
    with SCRIPT_ERROR_SENTRY:
        print("Queueing ios bad decryption keys script.")
        queue_script(celery_process_run_push_notification_scheduledevent_rebuild, HOURLY)


@scripts_celery_app.task(queue=SCRIPTS_QUEUE)
def celery_process_run_push_notification_scheduledevent_rebuild():
    with SCRIPT_ERROR_SENTRY:
        print("running script to rebuilld push notification scheduled events.")
        # not actually a script, just run this ... for-loop.
        push_notification_scheduledevent_rebuild()


def push_notification_scheduledevent_rebuild():
    for study in Study.objects.all():
        repopulate_all_survey_scheduled_events(study)
    
    # This flag eneables resends. This has to be set _After_ schedules are repopulated because...
    # on servers where the participants have updated the app before the server has been updated will
    # possibly be in a position where they receive a resend of every archived event within that period.
    
    # - repopulating logic may generate events that were missed (because of old bugs)
    settings = GlobalSettings().singleton()
    if settings.push_notification_resend_enabled is None:
        settings.push_notification_resend_enabled = timezone.now()
        settings.save()


#
## Participant data deletion
#
def create_task_participant_data_deletion():
    with SCRIPT_ERROR_SENTRY:
        print("Queueing purge_participant_data.")
        queue_script(celery_participant_data_deletion, HOURLY)


@scripts_celery_app.task(queue=SCRIPTS_QUEUE)
def celery_participant_data_deletion():
    with SCRIPT_ERROR_SENTRY:
        print("running script purge_participant_data.")
        from scripts import purge_participant_data
        purge_participant_data.main()


######################################### Daily ####################################################

#
## Upload the ssh auth log to S3 - this is a very basic security/audit measure, so we just do it.
#
def create_task_upload_logs():
    with SCRIPT_ERROR_SENTRY:
        print("Queueing upload_logs.")
        queue_script(celery_upload_logs, DAILY)


@scripts_celery_app.task(queue=SCRIPTS_QUEUE)
def celery_upload_logs():
    with SCRIPT_ERROR_SENTRY:
        print("running script upload_logs.")
        from scripts import upload_logs
        upload_logs.main()

#
## Purge all data that is from impossible timestamps - we test for this now, but have still seen it.
#
def create_task_purge_invalid_time_data():
    with SCRIPT_ERROR_SENTRY:
        print("Queueing script_that_removes_data_from_invalid_time_sources.")
        queue_script(celery_purge_invalid_time_data, DAILY)


@scripts_celery_app.task(queue=SCRIPTS_QUEUE)
def celery_purge_invalid_time_data():
    with SCRIPT_ERROR_SENTRY:
        print("running script_that_removes_data_from_invalid_time_sources.")
        from scripts import script_that_removes_data_from_invalid_time_sources
        script_that_removes_data_from_invalid_time_sources.main()

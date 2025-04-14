from datetime import timedelta
from typing import Callable

from django.utils import timezone

from constants.celery_constants import SCRIPTS_QUEUE
from libs.celery_control import safe_apply_async, scripts_celery_app
from libs.sentry import script_runner_error_sentry
from scripts import (purge_participant_data, repopulate_push_notifications,
    script_that_removes_data_from_invalid_time_sources, update_forest_version, upload_logs)


"""
To create a "script" task make sure to import it above (do not do a local import), make a function
that is wrapped with the @scripts_celery_app.task decorator, and make the name of the function
start with "six_minutes_", "hourly_", or "daily_".  Include a print statement and a 

@scripts_celery_app.task(queue=SCRIPTS_QUEUE)
def six_minutes_do_cool_new_thing():
    with script_runner_error_sentry():
        print("running script do_cool_new_thing.")
        do_cool_new_thing.main()
"""

####################################### Six Minutely ###############################################

#
## Check the forest version in the update_forest_version script
#
@scripts_celery_app.task(queue=SCRIPTS_QUEUE)
def six_minutes_update_forest_version():
    with script_runner_error_sentry():
        print("running script update_forest_version.")
        update_forest_version.main()


######################################### Hourly ###################################################

#
## Push Notification
#
@scripts_celery_app.task(queue=SCRIPTS_QUEUE)
def hourly_run_push_notification_scheduledevent_rebuild():
    with script_runner_error_sentry():
        print("running script to rebuilld push notification scheduled events.")
        # not actually a script, just run this ... for-loop.
        repopulate_push_notifications.main()

#
## Participant data deletion
#
@scripts_celery_app.task(queue=SCRIPTS_QUEUE)
def hourly_participant_data_deletion():
    with script_runner_error_sentry():
        print("running script purge_participant_data.")
        purge_participant_data.main()

######################################### Daily ####################################################

#
## Upload the ssh auth log to S3 - this is a very basic security/audit measure, so we just do it.
#
@scripts_celery_app.task(queue=SCRIPTS_QUEUE)
def daily_upload_logs():
    with script_runner_error_sentry():
        print("running script upload_logs.")
        upload_logs.main()

#
## Purge all data that is from impossible timestamps - we test for this now, but have still seen it.
#
@scripts_celery_app.task(queue=SCRIPTS_QUEUE)
def daily_purge_invalid_time_data():
    with script_runner_error_sentry():
        print("running script_that_removes_data_from_invalid_time_sources.")
        script_that_removes_data_from_invalid_time_sources.main()


####################################################################################################
############################# Script Runner Logic ##################################################
####################################################################################################


def enqueue_six_minute_scripts_tasks():
    _enqueue_tasks("six_minutes_", SIX_MINUTELY)


def enqueue_hourly_scripts_tasks():
    _enqueue_tasks("hourly_", HOURLY)


def enqueue_daily_script_tasks():
    _enqueue_tasks("daily_", DAILY)


def _enqueue_tasks(prefix: str, expiry_string: str):
    for task_name, task_function in get_prefixes_and_functions(globals(), prefix):
        with script_runner_error_sentry():
            print(f"Enqueueing {task_name} as \"{expiry_string}\" task.")
            queue_script(task_function, expiry_string)


def get_prefixes_and_functions(namespace: dict, prefix: str)-> list[tuple[str, Callable]]:
    return [(name, namespace[name]) for name in namespace if name.startswith(prefix)]


SIX_MINUTELY = "six_minutely"  # SOME DAY we will have better than 6 minute minute celery tasks.
HOURLY = "hourly"
DAILY = "daily"


def queue_script(a_celery_task: Callable, expiry_str: str):
    """ Forces enqueueing with an expiry. """
    
    if expiry_str == SIX_MINUTELY:
        expires = timezone.now() + timedelta(minutes=6)
    elif expiry_str == HOURLY:
        expires = timezone.now() + timedelta(hours=1)
    elif expiry_str == DAILY:
        expires = timezone.now() + timedelta(hours=24)
    else:
        raise ValueError("Expiry must be one of the constants in this file.")
    
    expires = expires.replace(second=0, microsecond=0)  # clear out seconds and microseconds
    
    print(f"Queueing script '{a_celery_task.__name__}', expires at {expiry_str}")
    safe_apply_async(
        a_celery_task,
        max_retries=0,
        expires=expires,
        task_track_started=True,
        task_publish_retry=False,
        retry=False,
    )

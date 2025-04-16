import functools
from datetime import timedelta
from time import sleep
from types import FunctionType
from typing import Any, Callable

from django.utils import timezone

from constants.celery_constants import SCRIPTS_QUEUE
from libs.celery_control import safe_apply_async, scripts_celery_app
from libs.sentry import script_runner_error_sentry, time_warning_script_runner
from scripts import (purge_participant_data, repopulate_push_notifications,
    script_that_removes_data_from_invalid_time_sources, update_forest_version, upload_logs)


class WoopsieError(Exception): pass


"""
To create a "script" task make sure to import it above (do not do a local import), make a function
that is wrapped with the @ScriptQueueWrapper()
start with "six_minutes_", "hourly_", or "daily_".  Include a print statement and an error Sentry.

@ScriptQueueWrapper()
def six_minutes_do_cool_new_thing():
    with script_runner_error_sentry():
        print("running script do_cool_new_thing.")
        do_cool_new_thing.main()
"""


class ScriptQueueWrapper:
    """ Just wrap a function in this class and name it correctly to make it a script queue task. """
    
    SIX_MINUTELY = "Six Minutely"  # SOME DAY we will have better than 6 minute minute celery tasks.
    HOURLY = "Hourly"
    DAILY = "Daily"
    
    WARNING_TIMEOUTS = {
        SIX_MINUTELY: (5*60),
        HOURLY: 55*60,
        DAILY: 23*60*60,
    }
    ERRORS = {
        SIX_MINUTELY: "Script '{}' took more than 5 minutes to run.",
        HOURLY: "Script '{}' took more than 55 minutes to run.",
        DAILY: "Script '{}' took more than 23 hours to run.",
    }
    
    queueable_things = []
    
    def __init__(self, *args):
        # Called as the class is instantiated: global scope. Errors will be raised at import time.
        
        if args and isinstance(args[0], FunctionType):
            raise WoopsieError(
                f"You forgot to include `()` on the @ScriptQueueWrapper for {args[0].__name__}."
            )
        
        self.__class__.queueable_things.append(self)
    
    def __call__(self, the_wrapped_func: Callable) -> Any:
        # Also called at import time! Not task-run-time!
        
        self.wrapper_setup(the_wrapped_func)
        self.sentry_function = self.wrapper_with_sentry_etc()  # wraps task_fusction
        self.timer_function = self.wrapper_function_in_timer_warning()  # wraps sentry_function
        self.celery_task_object = self.wrapper_function_as_celery_task()  # wraps timer_function
        
        # this will be *impossible* to debug if the wrapped functions don't use functools.wraps
        assert self.task_function is the_wrapped_func
        assert self.sentry_function.__name__ == self.original_name
        assert self.timer_function.__name__ == self.original_name
        return self.celery_task_object
    
    def wrapper_setup(self, the_wrapped_func):
        self.original_name = the_wrapped_func.__name__
        self.task_function = the_wrapped_func
        
        if self.original_name.startswith("six_minutes_"):
            self.task_frequency = self.SIX_MINUTELY
        elif self.original_name.startswith("hourly_"):
            self.task_frequency = self.HOURLY
        elif self.original_name.startswith("daily_"):
            self.task_frequency = self.DAILY
        else:
            raise NameError(
                f"Wrapped function '{self.original_name}' must have a name starting with "
                "`six_minutes_` `hourly_`, or `daily_`"
            )
    
    def wrapper_with_sentry_etc(self):
        # add a sentry error catcher, print statements, and a sleep statement.
        
        @functools.wraps(self.task_function)
        def wrapper_func():
            print(f"\nRunning {self.original_name} as {self.task_frequency} task.\n")
            try:
                with script_runner_error_sentry(tags={"task_name": self.original_name}):
                    self.task_function()
            finally:
                print(f"\nTask {self.original_name} is no longer running, pausing for 2 seconds.\n")
                sleep(2)  # Very fast errors may not send to sentry, sleep for 2 seconds to fix
        return wrapper_func
    
    def wrapper_function_in_timer_warning(self):
        timer_wrapper = time_warning_script_runner(
            message=self.ERRORS[self.task_frequency].format(self.original_name),
            timeout_seconds=self.WARNING_TIMEOUTS[self.task_frequency],
        )
        return timer_wrapper(self.sentry_function)  # uses functools.wrap
    
    def wrapper_function_as_celery_task(self):
        return scripts_celery_app.task(queue=SCRIPTS_QUEUE)(self.timer_function)
    
    @classmethod
    def enqueue_tasks(cls, task_type: str):
        SQW: ScriptQueueWrapper
        for SQW in (sqw for sqw in cls.queueable_things if sqw.task_frequency == task_type):
            with script_runner_error_sentry(tags={"task_name": SQW.original_name}):
                print(f"\nEnqueueing {SQW.original_name} as {task_type} task.\n")
                queue_script(SQW.celery_task_object, task_type)


####################################### Six Minutely ###############################################

#
## Check the forest version in the update_forest_version script
#
@ScriptQueueWrapper()
def six_minutes_update_forest_version():
    update_forest_version.main()


######################################### Hourly ###################################################

#
## Push Notification
#
@ScriptQueueWrapper()
def hourly_run_push_notification_scheduledevent_rebuild():
    repopulate_push_notifications.main()

#
## Participant data deletion
#
@ScriptQueueWrapper()
def hourly_participant_data_deletion():
    purge_participant_data.main()

######################################### Daily ####################################################

#
## Upload the ssh auth log to S3 - this is a very basic security/audit measure, so we just do it.
#
@ScriptQueueWrapper()
def daily_upload_logs():
    upload_logs.main()

#
## Purge all data that is from impossible timestamps - we test for this now, but have still seen it.
#
@ScriptQueueWrapper()
def daily_purge_invalid_time_data():
    script_that_removes_data_from_invalid_time_sources.main()


####################################################################################################
############################# Script Runner Logic ##################################################
####################################################################################################


def enqueue_six_minute_scripts_tasks():
    ScriptQueueWrapper.enqueue_tasks(ScriptQueueWrapper.SIX_MINUTELY)


def enqueue_hourly_scripts_tasks():
    ScriptQueueWrapper.enqueue_tasks(ScriptQueueWrapper.HOURLY)


def enqueue_daily_script_tasks():
    ScriptQueueWrapper.enqueue_tasks(ScriptQueueWrapper.DAILY)


# def _enqueue_tasks(prefix: str, expiry_string: str):
#     for task_name, task_function in get_prefixes_and_functions(globals(), prefix):
#         with script_runner_error_sentry(tags={"task_name": task_name}):
#             print(f"Enqueueing {task_name} as \"{expiry_string}\" task.")
#             queue_script(task_function, expiry_string)


def get_prefixes_and_functions(namespace: dict, prefix: str)-> list[tuple[str, Callable]]:
    return [(name, namespace[name]) for name in namespace if name.startswith(prefix)]


def queue_script(a_celery_task: Callable, expiry_str: str):
    """ Forces enqueueing with an expiry. """
    
    if expiry_str == ScriptQueueWrapper.SIX_MINUTELY:
        expires = timezone.now() + timedelta(minutes=6)
    elif expiry_str == ScriptQueueWrapper.HOURLY:
        expires = timezone.now() + timedelta(hours=1)
    elif expiry_str == ScriptQueueWrapper.DAILY:
        expires = timezone.now() + timedelta(hours=24)
    else:
        raise ValueError("Expiry must be one of the constants in this file.")
    
    expires = expires.replace(second=0, microsecond=0)  # clear out seconds and microseconds
    safe_apply_async(
        a_celery_task,
        max_retries=0,
        expires=expires,
        task_track_started=True,
        task_publish_retry=False,
        retry=False,
    )

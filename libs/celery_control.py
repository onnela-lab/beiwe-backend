from __future__ import annotations

import functools
import json
from datetime import timedelta
from pprint import pformat
from time import sleep
from types import FunctionType
from typing import Any, Callable, ClassVar, Self

from celery import Celery, Task
from celery.events.snapshot import Polaroid
from django.utils import timezone
from kombu.exceptions import OperationalError

from constants.celery_constants import (CELERY_CONFIG_LOCATION, DATA_PROCESSING_CELERY_QUEUE,
    DATA_PROCESSING_CELERY_SERVICE, FOREST_SERVICE, PUSH_NOTIFICATION_SEND_SERVICE, SCRIPTS_QUEUE,
    SCRIPTS_SERVICE)
from constants.common_constants import RUNNING_TESTS
from libs.sentry import make_error_sentry, SentryTimerWarning, SentryTypes


FORCE_CELERY_OFF = False  ##  Flag to force use of our DebugCeleryTask.


class OopsiesError(Exception): pass
class CeleryNotRunningException(Exception): pass


class DebugCeleryApp(object):
    """ Class that mimics enough functionality of a Celery app for us to be able to execute
    our celery infrastructure from the shell, single-threaded, without queuing. """
    
    events: Any
    connection: Any
    max_retries: Any
    
    def __init__(self, an_function: Callable):
        """ at instantiation (aka when used as a decorator) stash the function we wrap """
        if not RUNNING_TESTS:
            print(f"Instantiating a DebugCeleryApp for {an_function.__name__}.")
        self.an_function = an_function
    
    @staticmethod
    def task(*args, **kwargs):
        """
        This function executes at-import-time because it is a file-global function declaration with
        a celery_app.task(queue=queue_name) decorator. Our hack is to declare a static "task" method
        that does nothing but returns a FalseCelery app. """
        if not RUNNING_TESTS:
            print(f"task declared, args: {args}, kwargs:{kwargs}")
        return DebugCeleryApp
    
    def apply_async(self, *args, **kwargs):
        """ apply_async is the function we use to queue up tasks.  Our hack is to declare
        our own apply_async function that extracts the "args" parameter.  We pass those
        into our stored function. """
        if not RUNNING_TESTS:
            print(f"apply_async running {self.an_function.__name__}, args:{args}, kwargs:{kwargs}")
        if "args" not in kwargs:
            return self.an_function()
        return self.an_function(*kwargs["args"])


TaskLike = Task|DebugCeleryApp


def safe_apply_async(tasklike_obj: TaskLike, *args, **kwargs):
    """ Enqueuing a new task, for which we always use Celery's most flexible `apply_async` function,
    can fail deep inside amqp/transport.py with an OperationalError.
    
    tasklike_obj is either a "@celery_app.task"-wrapped function, or it is a DebugCeleryApp.
    
    DebugCeleryApps implement a pattern that allows us to test functions wrapped in celery tasks in
    the terminal and in tests without a celery app running.
    
    I'm sure celery has great debugging support somewhere, do not care. """
    
    for i in range(10):
        try:
            return tasklike_obj.apply_async(*args, **kwargs)
        except OperationalError:
            # after 4+ years in production this strategy works perfectly.  Cool.
            if i >= 3:
                raise


#
# Connections to Celery (or DebugCeleryApps if Celery is not present)
#

FORCE_CELERY_OFF = False


def instantiate_celery_app_connection(service_name: str) -> Celery|DebugCeleryApp:
    # this isn't viable because it breaks watch_processing (etc), because the celery.task.inspect
    # call will time out if no Celery object has been instantiated with credentials.
    # if RUNNING_TEST_OR_IN_A_SHELL:
    # return DebugCeleryApp
    
    if FORCE_CELERY_OFF:
        return DebugCeleryApp  # type: ignore[return-value]
    
    # the location of the manager_ip credentials file is in the folder above the project folder.
    try:
        with open(CELERY_CONFIG_LOCATION, 'r') as f:
            manager_ip, password = f.read().splitlines()
    except IOError:
        return DebugCeleryApp  # type: ignore[return-value]
    
    return Celery(
        service_name,
        # note that the 2nd trailing slash here is required, it is some default rabbitmq thing.
        broker=f'pyamqp://beiwe:{password}@{manager_ip}//',  # the pyamqp_endpoint? sure.
        backend='rpc://',
        task_publish_retry=False,
        task_track_started=True,
    )


processing_celery_app = instantiate_celery_app_connection(DATA_PROCESSING_CELERY_SERVICE)
push_send_celery_app = instantiate_celery_app_connection(PUSH_NOTIFICATION_SEND_SERVICE)
forest_celery_app = instantiate_celery_app_connection(FOREST_SERVICE)
scripts_celery_app = instantiate_celery_app_connection(SCRIPTS_SERVICE)


# These functions are helpers for use in a live shell session on a machine running celery.
# All return a list of ids (can be empty), or None if celery isn't currently running.


# which celery app(?) used is completely arbitrary, they are all the same.
def inspect_celery(selery: Celery|DebugCeleryApp):
    """ Inspect is annoyingly unreliable and has a default 1 second timeout.
        Will error if executed while a DebugCeleryApp is in use. """
    
    # this function intentionally breaks if you instantiated a DebugCeleryApp.
    if isinstance(selery, DebugCeleryApp):
        raise CeleryNotRunningException("DebugCeleryApp is in use, this session is not connected to celery.")
    
    now = timezone.now()
    fail_time = now + timedelta(seconds=20)
    
    while now < fail_time:
        try:
            return selery.control.inspect(timeout=0.1)
        except CeleryNotRunningException:
            now = timezone.now()
            continue
    
    raise CeleryNotRunningException()


# these strings are tags on the apps in cluster_management/pushed_files/install_celery_worker.sh
# specific example: --hostname=%%h_processing

# Push Notifications
def get_notification_scheduled_job_ids() -> int|list|None:
    if push_send_celery_app is DebugCeleryApp:
        print("call to get_notification_scheduled_job_ids in DebugCeleryApp, returning []")
        return []
    return _get_job_ids(inspect_celery(push_send_celery_app).scheduled(), "notifications")


def get_notification_reserved_job_ids() -> int|list|None:
    if push_send_celery_app is DebugCeleryApp:
        print("call to get_notification_reserved_job_ids in DebugCeleryApp, returning []")
        return []
    return _get_job_ids(inspect_celery(push_send_celery_app).reserved(), "notifications")


def get_notification_active_job_ids() -> int|list|None:
    if push_send_celery_app is DebugCeleryApp:
        print("call to get_notification_active_job_ids in DebugCeleryApp, returning []")
        return []
    return _get_job_ids(inspect_celery(push_send_celery_app).active(), "notifications")

# Processing
def get_processing_scheduled_job_ids() -> int|list|None:
    if processing_celery_app is DebugCeleryApp:
        print("call to get_processing_scheduled_job_ids in DebugCeleryApp, returning []")
        return []
    return _get_job_ids(inspect_celery(processing_celery_app).scheduled(), "processing")


def get_processing_reserved_job_ids() -> int|list|None:
    if processing_celery_app is DebugCeleryApp:
        print("call to get_processing_reserved_job_ids in DebugCeleryApp, returning []")
        return []
    return _get_job_ids(inspect_celery(processing_celery_app).reserved(), "processing")


def get_processing_active_job_ids() -> int|list|None:
    if processing_celery_app is DebugCeleryApp:
        print("call to get_processing_active_job_ids in DebugCeleryApp, returning []")
        return []
    return _get_job_ids(inspect_celery(processing_celery_app).active(), "processing")


# logic for any of the above functions
def _get_job_ids(celery_query_dict, celery_app_suffix):
    """ This is a utility function for poking live celery apps.
    
    Data structure looks like this, we just want that args component.
    Returns list of ids (can be empty), or None if celery isn't currently running.
    
    {'celery@ip-172-31-75-163_processing': [{'id': 'a391eff1-05ae-4524-843e-f8bdc96d0468',
    'name': 'services.celery_data_processing.celery_process_file_chunks',
    'args': [1559],
    'kwargs': {},
    'type': 'services.celery_data_processing.celery_process_file_chunks',
    'hostname': 'celery@ip-172-31-75-163_processing',
    'time_start': 1710402847.7559981,
    'acknowledged': True,
    'delivery_info': {'exchange': '',
        'routing_key': 'data_processing',
        'priority': None,
        'redelivered': False},
    'worker_pid': 4433},
    {'id': '0a4a3fad-ce10-4265-ae14-a2004f0bbedc',
    'name': 'services.celery_data_processing.celery_process_file_chunks',
    'args': [1557],
    'kwargs': {},
    'type': 'services.celery_data_processing.celery_process_file_chunks',
    'hostname': 'celery@ip-172-31-75-163_processing',
    'time_start': 1710402847.7390666,
    'acknowledged': True,
    'delivery_info': {'exchange': '',
        'routing_key': 'data_processing',
        'priority': None,
        'redelivered': False},
    'worker_pid': 4432}]}
    """
    
    # for when celery isn't running
    if celery_query_dict is None:
        raise CeleryNotRunningException()
    
    # below could be substantially improved. itertools chain....
    all_processing_jobs = []
    for worker_name, list_of_jobs in celery_query_dict.items():
        if worker_name.endswith(celery_app_suffix):
            all_processing_jobs.extend(list_of_jobs)
    
    all_args = []
    for job_arg in [job['args'] for job in all_processing_jobs]:
        # 2020-11-24:: this job_arg value has started to return a list object, not a json string
        #  ... but only on one of 3 newly updated servers. ...  Buh?
        args = job_arg if isinstance(job_arg, list) else json.loads(job_arg)
        # safety/sanity check, assert that there is only 1 integer id in a list and that it is a list.
        assert isinstance(args, list)
        assert len(args) == 1
        assert isinstance(args[0], int)
        all_args.append(args[0])
    
    return all_args

"""
Documenting the inspect functionality because it is quite obscure.

active - a list of the following form, should be lists of tasks.
    {'celery@ip-172-31-75-163_notifications': [],
     'celery@ip-172-31-75-163_processing': [],
     'celery@ip-172-31-75-163_forest': [],
     'celery@ip-172-31-75-163_scripts': []}

revoked - same format
scheduled - same format

active_queues - more detail than you could ever want about the queues (dict of list of dict).
{'celery@ip-172-31-75-163_processing': [{'name': 'data_processing',
   'exchange': {'name': 'data_processing',
    'type': 'direct',
    'arguments': None,
    'durable': True,
    'passive': False,
    'auto_delete': False,
    'delivery_mode': None,
    'no_declare': False},
   'routing_key': 'data_processing',
   'queue_arguments': None,
   'binding_arguments': None,
   'consumer_arguments': None,
   'durable': True,
   'exclusive': False,
   'auto_delete': False,
   'no_ack': False,
   'alias': None,
   'bindings': [],
   'no_declare': None,
   'expires': None,
   'message_ttl': None,
   'max_length': None,
   'max_length_bytes': None,
   'max_priority': None}], ...

registered
    {'celery@ip-172-31-75-163_processing': ['services.celery_data_processing.celery_process_file_chunks'],
    'celery@ip-172-31-75-163_forest': ['services.celery_forest.celery_run_forest'],
    'celery@ip-172-31-75-163_notifications': ['services.celery_push_notifications.celery_heartbeat_send_push_notification',
     'services.celery_push_notifications.celery_send_survey_push_notification'],
    'celery@ip-172-31-75-163_scripts': ['services.scripts_runner.celery_participant_data_deletion',
     'services.scripts_runner.celery_process_ios_no_decryption_key',
     'services.scripts_runner.celery_purge_invalid_time_data',
     'services.scripts_runner.celery_update_forest_version',
     'services.scripts_runner.celery_upload_logs']}

registered_tasks - looks identical to registered...
reserved - looks identical to registered...

stats - it's own thing.
"""

class SnapShot(Polaroid):
    clear_after = True  # clear after flush (incl, state.event_count).
    
    def on_shutter(self, state):
        if not state.event_count:
            # No new events since last snapshot.
            print('No new events...\n')
        print('Workers: {0}'.format(pformat(state.workers, indent=4)))
        print('Tasks: {0}'.format(pformat(state.tasks, indent=4)))
        print('Total: {0.event_count} events, {0.task_count} tasks'.format(state))
        print()


def watch_celery():
    """ it doesn't matter which processing_celery_app we use, they are all the same.  """
    state = processing_celery_app.events.State()
    freq = 1.0  # seconds
    with processing_celery_app.connection() as connection:
        recv = processing_celery_app.events.Receiver(connection, handlers={'*': state.event})
        with SnapShot(state, freq=freq):
            recv.capture(limit=None, timeout=None)


####################################################################################################
#################################### Celery Task Wrappers ##########################################
####################################################################################################

# Keep these legible
SIX_MINUTELY = "Six Minutely"  # SOME DAY we will have better than 6 minute minute celery tasks.
HOURLY = "Hourly"
DAILY = "Daily"

class AbstractQueueWrapper:
    """
    IT EVENTUALLY BECAME APPARENT THAT BEING ABLE TO CREATE NEW TASKS VERY EASILY WAS SUPER IMPORTANT.
    
    ALSO, IF THERE IS AN ERROR REALLY EARLY IN A CELERY TASK'S RUNTIME, OR POSSIBLY ANOTHER UNKNOWN
    SCENARIO IT CAN FAIL BEFORE SENDING AN ERROR REPORT, AND/OR CAUSE SOME WEIRD ~SPINLOCK CONDITION
    THAT CAUSES RABBITMQ TO GET STUCK AND SPEW LOGGING ERRORS UNTIL THE SERVER GOES DOWN.
    
    Just wrap a function in A POPULATED SUBCLASS, name it correctly, and it should work better.
    There are tests to import all celery tasks, which executes everything but the enqueuing code.
    
    Timer values are based on the name of the wrapped function.
    """
    
    queueable_things: ClassVar[list[Self]]  # just make a list
    celery_app: ClassVar[Celery|DebugCeleryApp]
    target_queue: ClassVar[str]
    sentry_type: ClassVar[str]
    ERRORS: ClassVar[dict[str, str]]
    
    WARNING_TIMEOUTS = {
        SIX_MINUTELY: (5*60),
        HOURLY: 55*60,
        DAILY: 23*60*60,
    }
    
    def __init__(self, *args): # executes at-import-time
        # Called as the class is instantiated: global scope. Errors will be raised at import time.
        
        if args and isinstance(args[0], FunctionType):
            raise OopsiesError(
                f"You forgot to include `()` on the @ScriptQueueWrapper for {args[0].__name__}."
            )
        
        self.__class__.queueable_things.append(self)
    
    def __call__(self, the_wrapped_func: Callable) -> Task | DebugCeleryApp:  # executes at-IMPORT-time!
        
        self.wrapper_setup(the_wrapped_func)
        self.sentry_function = self.wrapper_with_sentry_etc()  # wraps task_fusction
        self.timer_function = self.wrapper_function_in_timer_warning()  # wraps sentry_function
        self.celery_task_object = self.wrapper_function_as_celery_task()  # wraps timer_function
        
        # Due to the number of wrappers here we have to either ensure functools.wraps is used
        # down-the-line, or not used at all.  Using it may make task traces literally wrong, but not
        # using it will make the messages on Sentry literally wrong.
        assert self.task_function is the_wrapped_func  # ok but this assertion is important
        assert self.sentry_function.__name__ == self.original_name
        assert self.timer_function.__name__ == self.original_name
        return self.celery_task_object
    
    def wrapper_setup(self, the_wrapped_func):  # executes at-IMPORT-time!
        self.original_name = the_wrapped_func.__name__
        self.task_function = the_wrapped_func
        startswith = self.original_name.startswith  # (holy CRAP this makes it way easier to read...)
        
        if startswith("six_minutes_"):
            self.task_frequency = SIX_MINUTELY
        elif startswith("hourly_"):
            self.task_frequency = HOURLY
        elif startswith("daily_"):
            self.task_frequency = DAILY
        else:
            raise NameError(
                f"Wrapped function '{self.original_name}' must have a name starting with "
                "`six_minutes_` `hourly_`, or `daily_`"
            )
    
    def wrapper_with_sentry_etc(self) -> Callable:  # executes at-IMPORT-time!
        # add an error sentry, print statements, and a post-execution sleep
        
        @functools.wraps(self.task_function)
        def wrapper_func():
            print(f"\nRunning {self.original_name} as {self.task_frequency} task.\n")
            try:
                with make_error_sentry(self.sentry_type, tags={"task_name": self.original_name}):
                    self.task_function()
            finally:
                print(f"\nTask {self.original_name} is no longer running, pausing for 2 seconds.\n")
                sleep(2)  # Very fast errors may not send to sentry, sleep for 2 seconds to fix
        
        return wrapper_func
    
    def wrapper_function_in_timer_warning(self) -> Callable:  # executes at-IMPORT-time!
        timer_wrapper = SentryTimerWarning(
            self.sentry_type,
            message=self.ERRORS[self.task_frequency].format(self.original_name),
            timeout_seconds=self.WARNING_TIMEOUTS[self.task_frequency],
            tags={"task_name": self.original_name}
        )
        return timer_wrapper(self.sentry_function)  # uses functools.wrap
    
    def wrapper_function_as_celery_task(self) -> TaskLike:  # executes at-IMPORT-time!
        the_task = self.celery_app.task(queue=self.target_queue)(self.timer_function)
        the_task.max_retries = 0  # We never, ever, EVER, _EV-UR_ want to automatically retry a task
        return the_task  # type: ignore[return-value]
    
    # (you don't have to use these easy enqueue-all functions, if you do the timer still respects
    # the function naming structure.)
    
    @classmethod
    def enqueue_tasks(cls, task_type: str):  # executes at-RUN-time of task queueuing.
        self: Self
        for self in (self for self in cls.queueable_things if self.task_frequency == task_type):
            with make_error_sentry(self.sentry_type, tags={"task_name": self.original_name}):
                print(f"\nEnqueueing {self.original_name} as {task_type} task.\n")
                cls.enqueue_task(self.celery_task_object, task_type)
    
    @classmethod
    def enqueue_task(cls, task: TaskLike, expiry_str: str, *args, **kwargs):  # executes at-RUN-time of task enqueuement
        now = timezone.now().replace(second=0, microsecond=0)
        
        if expiry_str == SIX_MINUTELY:
            expires = now + timedelta(minutes=6)
        elif expiry_str == HOURLY:
            expires = now + timedelta(hours=1)
        elif expiry_str == DAILY:
            expires = now + timedelta(hours=24)
        else:
            raise ValueError("Expiry must be one of SIX_MINUTELY, HOURLY, or DAILY.")
        
        expires = expires.replace(second=0, microsecond=0)  # clear out seconds and microseconds
        safe_apply_async(
            task,
            args=args,
            kwargs=kwargs,
            max_retries=0,
            expires=expires,
            task_track_started=True,
            task_publish_retry=False,
            retry=False,
        )


class CeleryScriptTask(AbstractQueueWrapper):
    queueable_things = []
    celery_app = scripts_celery_app
    target_queue = SCRIPTS_QUEUE
    sentry_type = SentryTypes.script_runner
    ERRORS = {
        SIX_MINUTELY: "Script '{}' took more than 5 minutes to run.",
        HOURLY: "Script '{}' took more than 55 minutes to run.",
        DAILY: "Script '{}' took more than 23 hours to run.",
    }


class CeleryDataProcessingTask(AbstractQueueWrapper):
    queueable_things = []
    celery_app = processing_celery_app
    target_queue = DATA_PROCESSING_CELERY_QUEUE
    sentry_type = SentryTypes.data_processing
    ERRORS = {
        SIX_MINUTELY: "Data Processing task '{}' took more than 5 minutes to run.",  # dumb, whatver
        HOURLY: "Data Processing task '{}' took more than 55 minutes to run.",
        DAILY: "Data Processing task '{}' took more than 23 hours to run.",
    }

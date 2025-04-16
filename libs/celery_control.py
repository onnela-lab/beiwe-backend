import json
from datetime import timedelta
from pprint import pformat
from typing import Union

from celery import Celery
from celery.events.snapshot import Polaroid
from django.utils import timezone
from kombu.exceptions import OperationalError

from constants.celery_constants import (CELERY_CONFIG_LOCATION, DATA_PROCESSING_CELERY_SERVICE,
    FOREST_SERVICE, PUSH_NOTIFICATION_SEND_SERVICE, SCRIPTS_SERVICE)
from constants.common_constants import RUNNING_TESTS


def safe_apply_async(a_task_for_a_celery_queue, *args, **kwargs):
    """ Enqueuing a new task, for which we use Celery's most flexible `apply_async` function,
    can fail deep inside amqp/transport.py with an OperationalError. Use this common wrapper to
    handle that case.

    An "a_task_for_a_celery_queue" is either a "@celery_app.task"-wrapped function' or it is a
    FalseCeleryApp. FalseCeleryApps implement an apply_async passthrough function that allows us to
    test "celery code" in the terminal. In the terminal everything is perfectly sequential and easy,
    without and devs don't need an active celery instance. """
    for i in range(10):
        try:
            return a_task_for_a_celery_queue.apply_async(*args, **kwargs)
        except OperationalError:
            # after 4+ years in production this strategy works perfectly.  Cool.
            if i >= 3:
                raise


#
# Helper classes
#

class FalseCeleryAppError(Exception): pass
class CeleryNotRunningException(Exception): pass


class FalseCeleryApp:
    """ Class that mimics enough functionality of a Celery app for us to be able to execute
    our celery infrastructure from the shell, single-threaded, without queuing. """
    
    def __init__(self, an_function: callable):
        """ at instantiation (aka when used as a decorator) stash the function we wrap """
        if not RUNNING_TESTS:
            print(f"Instantiating a FalseCeleryApp for {an_function.__name__}.")
        self.an_function = an_function
    
    @staticmethod
    def task(*args, **kwargs):
        """ Our pattern is that we wrap our celery functions in the task decorator.
        This function executes at-import-time because it is a file-global function declaration with
        a celery_app.task(queue=queue_name) decorator. Our hack is to declare a static "task" method
        that does nothing but returns a FalseCelery app. """
        if not RUNNING_TESTS:
            print(f"task declared, args: {args}, kwargs:{kwargs}")
        return FalseCeleryApp
    
    def apply_async(self, *args, **kwargs):
        """ apply_async is the function we use to queue up tasks.  Our hack is to declare
        our own apply_async function that extracts the "args" parameter.  We pass those
        into our stored function. """
        if not RUNNING_TESTS:
            print(f"apply_async running {self.an_function.__name__}, args:{args}, kwargs:{kwargs}")
        if "args" not in kwargs:
            return self.an_function()
        return self.an_function(*kwargs["args"])


#
# Connections to Celery (or FalseCeleryApps if Celery is not present)
#

FORCE_CELERY_OFF = False


def instantiate_celery_app_connection(service_name: str) -> Union[Celery, FalseCeleryApp]:
    # this isn't viable because it breaks watch_processing (etc), because the celery.task.inspect
    # call will time out if no Celery object has been instantiated with credentials.
    # if RUNNING_TEST_OR_IN_A_SHELL:
    # return FalseCeleryApp
    
    if FORCE_CELERY_OFF:
        return FalseCeleryApp
    
    # the location of the manager_ip credentials file is in the folder above the project folder.
    try:
        with open(CELERY_CONFIG_LOCATION, 'r') as f:
            manager_ip, password = f.read().splitlines()
    except IOError:
        return FalseCeleryApp
    
    return Celery(
        service_name,
        # note that the 2nd trailing slash here is required, it is some default rabbitmq thing.
        broker=f'pyamqp://beiwe:{password}@{manager_ip}//',  # the pyamqp_endpoint.
        backend='rpc://',
        task_publish_retry=False,
        task_track_started=True,
    )


# if None then there is no celery app.
processing_celery_app = instantiate_celery_app_connection(DATA_PROCESSING_CELERY_SERVICE)
push_send_celery_app = instantiate_celery_app_connection(PUSH_NOTIFICATION_SEND_SERVICE)
forest_celery_app = instantiate_celery_app_connection(FOREST_SERVICE)
scripts_celery_app = instantiate_celery_app_connection(SCRIPTS_SERVICE)

#
# The remaining functions are helpers for use in a live shell session on a machine running celery.
# All return a list of ids (can be empty), or None if celery isn't currently running.
#

# which celery app(?) used is completely arbitrary, they are all the same.
def inspect(selery: Celery):
    """ Inspect is annoyingly unreliable and has a default 1 second timeout.
        Will error if executed while a FalseCeleryApp is in use. """
    
    # this function intentionally breaks if you every instantiated a false celery app
    if (
        processing_celery_app is FalseCeleryApp
        or push_send_celery_app is FalseCeleryApp
        or forest_celery_app is FalseCeleryApp
    ):
        raise CeleryNotRunningException("FalseCeleryApp is in use, this session is not connected to celery.")
    
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
def get_notification_scheduled_job_ids() -> Union[int, None]:
    if push_send_celery_app is FalseCeleryApp:
        print("call to get_notification_scheduled_job_ids in FalseCeleryApp, returning []")
        return []
    return _get_job_ids(inspect(push_send_celery_app).scheduled(), "notifications")


def get_notification_reserved_job_ids() -> Union[int, None]:
    if push_send_celery_app is FalseCeleryApp:
        print("call to get_notification_reserved_job_ids in FalseCeleryApp, returning []")
        return []
    return _get_job_ids(inspect(push_send_celery_app).reserved(), "notifications")


def get_notification_active_job_ids() -> Union[int, None]:
    if push_send_celery_app is FalseCeleryApp:
        print("call to get_notification_active_job_ids in FalseCeleryApp, returning []")
        return []
    return _get_job_ids(inspect(push_send_celery_app).active(), "notifications")

# Processing
def get_processing_scheduled_job_ids() -> Union[int, None]:
    if processing_celery_app is FalseCeleryApp:
        print("call to get_processing_scheduled_job_ids in FalseCeleryApp, returning []")
        return []
    return _get_job_ids(inspect(processing_celery_app).scheduled(), "processing")


def get_processing_reserved_job_ids() -> Union[int, None]:
    if processing_celery_app is FalseCeleryApp:
        print("call to get_processing_reserved_job_ids in FalseCeleryApp, returning []")
        return []
    return _get_job_ids(inspect(processing_celery_app).reserved(), "processing")


def get_processing_active_job_ids() -> Union[int, None]:
    if processing_celery_app is FalseCeleryApp:
        print("call to get_processing_active_job_ids in FalseCeleryApp, returning []")
        return []
    return _get_job_ids(inspect(processing_celery_app).active(), "processing")


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


# class off of documentation to watch celery events. Not super useful right now but could be.
class DumpCam(Polaroid):
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
        with DumpCam(state, freq=freq):
            recv.capture(limit=None, timeout=None)

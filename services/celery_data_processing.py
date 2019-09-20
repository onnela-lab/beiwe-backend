# We need to execute this file directly, so we always run the import hack
import imp as _imp
from os.path import abspath as _abspath

_current_folder_init = _abspath(__file__).rsplit('/', 1)[0] + "/__init__.py"
_imp.load_source("__init__", _current_folder_init)

from config import load_django

from kombu.exceptions import OperationalError
from celery import Celery, states
from celery.states import SUCCESS

STARTED_OR_WAITING = [states.PENDING, states.RECEIVED, states.STARTED]

FAILED = [states.REVOKED, states.RETRY, states.FAILURE]

try:
    with open("/home/ubuntu/manager_ip", 'r') as f:
        manager_info = f.read()
    manager_ip, password = manager_info.splitlines()
    celery_app = Celery(
            "data_processing_tasks",
            # note that the 2nd trailing slash here is actually required and
            broker='pyamqp://beiwe:%s@%s//' % (password, manager_ip),
            backend='rpc://',
            task_publish_retry=False,
            task_track_started=True
    )

except IOError:
    print "could not load the manager ip file, defaulting to 127.0.0.1 in guest mode."
    celery_app = Celery(
            "data_processing_tasks",
            broker='pyamqp://guest@127.0.0.1//',
            backend='rpc://',
            task_publish_retry=False,
            task_track_started=True
    )

# Load Django

################################################################################
############################# Data Processing ##################################
################################################################################
from time import sleep
from datetime import datetime, timedelta

from config.constants import FILE_PROCESS_PAGE_SIZE, CELERY_EXPIRY_MINUTES, CELERY_ERROR_REPORT_TIMEOUT_SECONDS
from database.data_access_models import FileProcessLock
from database.user_models import Participant
from libs.file_processing import ProcessingOverlapError, do_process_user_file_chunks
from libs.logging import email_system_administrators
from libs.sentry import make_error_sentry


@celery_app.task
def queue_user(participant):
    return celery_process_file_chunks(participant)

#Fixme: does this work?
queue_user.max_retries = 0


def safe_queue_user(*args, **kwargs):
    """
    Queue the given user's file processing with the given keyword arguments. This should
    return immediately and leave the processing to be done in the background via celery.
    In case there is an error with enqueuing the process, retry it several times until
    it works.
    """
    for i in range(10):
        try:
            return queue_user.apply_async(*args, **kwargs)
        except OperationalError:
            # Enqueuing can fail deep inside amqp/transport.py with an OperationalError. We
            # wrap it in some retry logic when this occurs.
            if i < 3:
                pass
            else:
                raise


def create_file_processing_tasks():
    # The entire code is wrapped in an ErrorSentry, which catches any errors
    # and sends them to Sentry.
    with make_error_sentry('data') as error_sentry:
        print(error_sentry.sentry_client.is_enabled())
        if FileProcessLock.islocked():
            # This is really a safety check to ensure that no code executes
            # if file processing is locked.
            report_file_processing_locked_and_exit()
            # report_file_processing_locked should raise an error; this should be unreachable
            exit(0)
        else:
            FileProcessLock.lock()
            
        print("starting.")
        now = datetime.now()
        expiry = now + timedelta(minutes=CELERY_EXPIRY_MINUTES)
        participant_set = Participant.objects.filter(files_to_process__isnull=False).distinct().values_list("id", flat=True)
        running = []
        
        for participant_id in participant_set:
            # Queue all users' file processing, and generate a list of currently running jobs
            # to use to detect when all jobs are finished running.
            running.append(safe_queue_user(
                args=[participant_id],
                max_retries=0,
                expires=expiry,
                task_track_started=True,
                task_publish_retry=False,
                retry=False
            ))
            
        print("tasks:", running)
        
        # If there are any Celery tasks still running, check their state and update the running
        # list accordingly. Do this every 5 seconds.
        while running:
            new_running = []
            failed = []
            successful = []
            for future in running:
                ####################################################################################
                # This variable can mutate on a separate thread.  We need the value as it was at
                # this snapshot in time, so we store it.  (The object is a string, passed by value.)
                ####################################################################################
                state = future.state
                if state == SUCCESS:
                    successful.append(future)
                if state in FAILED:
                    failed.append(future)
                if state in STARTED_OR_WAITING:
                    new_running.append(future)
                
            running = new_running
            print("tasks:", running)
            if running:
                sleep(5)
                
        print("Finished, unlocking.")
        # The unlocking MUST be **inside** the with statement.
        FileProcessLock.unlock()


def report_file_processing_locked_and_exit():
    """ Creates a useful error report with information about the run time. """
    timedelta_since_last_run = FileProcessLock.get_time_since_locked()
    print("timedelta %s" % timedelta_since_last_run.total_seconds())
    if timedelta_since_last_run.total_seconds() > CELERY_ERROR_REPORT_TIMEOUT_SECONDS:
        error_msg = (
            "Data processing has overlapped with a prior data index run that started more than "
            "%s minutes ago.\nThat prior run has been going for %s hour(s), %s minute(s)"
        )
        error_msg = error_msg % (CELERY_ERROR_REPORT_TIMEOUT_SECONDS / 60,
                                 str(int(timedelta_since_last_run.total_seconds() / 60 / 60)),
                                 str(int(timedelta_since_last_run.total_seconds() / 60 % 60)))
        
        if timedelta_since_last_run.total_seconds() > CELERY_ERROR_REPORT_TIMEOUT_SECONDS * 4:
            error_msg = "DATA PROCESSING OVERLOADED, CHECK SERVER.\n" + error_msg
            email_system_administrators(error_msg, "DATA PROCESSING OVERLOADED, CHECK SERVER")
        raise ProcessingOverlapError(error_msg)


# we are not really using the this class for much anymore, but it is there if we need it in future
class LogList(list):
    def append(self, p_object):
        super(LogList, self).append(p_object)
        print(p_object)
        
    def extend(self, iterable):
        print (str(i) for i in iterable)
        super(LogList, self).extend(iterable)
        
        
def celery_process_file_chunks(participant_id):
    """
    This is the function that is called from celery.  It runs through all new files that have
    been uploaded and 'chunks' them. Handles logic for skipping bad files, raising errors
    appropriately.
    This runs automatically and periodically as a Celery task.
    """
    participant = Participant.objects.get(id=participant_id)
    log = LogList()
    number_bad_files = 0
    tags = {'user_id': participant.patient_id}
    error_sentry = make_error_sentry('data', tags=tags)
    log.append("processing files for %s" % participant.patient_id)
    
    while True:
        previous_number_bad_files = number_bad_files
        starting_length = participant.files_to_process.exclude(deleted=True).count()
        
        log.append("%s processing %s, %s files remaining" % (datetime.now(), participant.patient_id, starting_length))
        number_bad_files += do_process_user_file_chunks(
                count=FILE_PROCESS_PAGE_SIZE,
                error_handler=error_sentry,
                skip_count=number_bad_files,
                participant=participant,
        )
        
        # If no files were processed, quit processing
        if participant.files_to_process.exclude(deleted=True).count() == starting_length:
            if previous_number_bad_files == number_bad_files:
                # Cases:
                #   every file broke, blow up. (would cause infinite loop otherwise)
                #   no new files.
                break
            else:
                continue
                
    with make_error_sentry('data', tags=tags):
        error_sentry.raise_errors()
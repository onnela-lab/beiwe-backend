from datetime import timedelta

from django.utils import timezone

from database.user_models_participant import Participant
from libs.celery_control import (CeleryDataProcessingTask, get_processing_active_job_ids,
    safe_apply_async)
from libs.file_processing.file_processing_core import easy_run
from libs.sentry import make_error_sentry, SentryTypes


from libs.celery_control import processing_celery_app;  # required in the file namespace for celery to work.

################################################################################
############################# Data Processing ##################################
################################################################################


def create_file_processing_tasks():
    """ Generates tasks to enqueue.  This is called every 6 minutes, and tasks have a lifetime
    of 6 minutes.  Note that tasks are not removed from the queue by RabbitMQ, but by Celery.
    inspecting the queue will continue to display the tasks that have not been sent to Celery until
    the most recent job is finished.
    
    Also, for some reason 5 minutes is the smallest value that .... works.  At all. No clue why. """
    
    # set the tasks to expire at the 5 minutes and thirty seconds mark after the most recent 6
    # minutely cron task. This way all tasks will be revoked at the same, and well-known, instant.
    expiry = (timezone.now() + timedelta(minutes=5)).replace(second=30, microsecond=0)
    
    with make_error_sentry(sentry_type=SentryTypes.data_processing):
        participant_set = set(
            Participant.objects.filter(files_to_process__isnull=False)
                .distinct()
                # .order_by("id")  # For debugging, forces overlap conflicts.
                .order_by("?")     # don't want a single user blocking everyone because they are at the front.
                .values_list("id", flat=True)
        )
        
        # sometimes celery just fails to exist, set should be redundant.
        active_set = set(get_processing_active_job_ids())
        
        participants_to_process = participant_set - active_set
        print("Queueing these participants:", ",".join(str(p) for p in participants_to_process))
        
        for participant_id in participants_to_process:
            # Queue all users' file processing, and generate a list of currently running jobs to use
            # to detect when all jobs are finished running.
            safe_apply_async(
                daily_celery_process_file_chunks,
                args=[participant_id],
                max_retries=0,
                expires=expiry,
                task_track_started=True,
                task_publish_retry=False,
                retry=False
            )
        print(f"{len(participants_to_process)} users queued for processing")


## uh, this use of "daily" is just to make the timer warning useful for this process.
@CeleryDataProcessingTask()
def daily_celery_process_file_chunks(participant_id):
    """ Task caller that runs through all new uploads from a specific user and 'chunks' them.
    Handles logic for skipping bad files, raising errors ~nicer. """
    
    # All iteration logic has been moved into celery_processing_core
    participant = Participant.objects.get(id=participant_id)
    easy_run(participant)

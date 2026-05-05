from datetime import timedelta

from django.utils import timezone

from database.models import FileToProcess, Participant
from libs.celery_control import (CeleryDataProcessingTask, get_processing_active_job_ids,
    safe_apply_async)
from libs.file_processing.file_processing_core import easy_run
from libs.sentry import send_sentry_warning, SentryUtils


from libs.celery_control import processing_celery_app;  # noqa - required in the file namespace for celery to work.

################################################################################
############################# Data Processing ##################################
################################################################################


def create_file_processing_tasks():
    """ Generates tasks to enqueue.  This is called every 6 minutes, and tasks have a lifetime
    of 6 minutes.  Note that tasks are not removed from the queue by RabbitMQ, but by Celery.
    inspecting the queue will continue to display the tasks that have not been sent to Celery until
    the most recent job is finished.
    
    Also, for some reason 6 minutes is the smallest value that .... works. No clue why. """
    
    real_start = timezone.now()
    interval_at_start = get_next_6_minute_interval()
    
    with SentryUtils.report_data_processing():
        participant_set = set(
            FileToProcess.objects.values_list("participant_id", flat=True).distinct()
        )
        
        # sometimes celery just fails to exist, set should be redundant.
        active_set = set(get_processing_active_job_ids())
        
        participants_to_process = participant_set - active_set
        print("Queueing these participants:", ",".join(str(p) for p in participants_to_process))
        
        next_interval = get_next_6_minute_interval()
        
        if interval_at_start != next_interval:
            send_sentry_warning(
                "Dispatching data processing tasks took more than 6 minutes. Start: "
                f"{real_start.isoformat()}, interval at start: {interval_at_start.isoformat()}, "
                f"final interval: {next_interval.isoformat()}"
            )
        
        for participant_id in participants_to_process:
            # Queue all users' file processing, and generate a list of currently running jobs to use
            # to detect when all jobs are finished running.
            safe_apply_async(
                daily_celery_process_file_chunks,
                args=[participant_id],
                max_retries=0,
                expires=next_interval,
                task_track_started=True,
                task_publish_retry=False,
                retry=False
            )
        
        print(f"{len(participants_to_process)} users queued for processing")


def get_next_6_minute_interval():
    """ Returns the next 6-minute interval from the current time, minus 5 seconds for a tiny buffer. """
    # Get the current time, calculate the next multiple of 6 time (this is dispatched every 6 minutes)
    # then subtract 5 seconds for a tiny buffer.
    
    # (x // 6) * 6 = start of current 6-minute block
    # Account for complex hour-rollover math by making this
    now = timezone.now()
    minutes_rounded = (now.minute // 6) * 6
    rounded_now = now.replace(minute=minutes_rounded, second=0, microsecond=0)
    return rounded_now + timedelta(minutes=6) - timedelta(seconds=5)


## uh, this use of "daily" is just to make the timer warning useful for this process.
@CeleryDataProcessingTask()
def daily_celery_process_file_chunks(participant_id):
    """ Task caller that runs through all new uploads from a specific user and 'chunks' them.
    Handles logic for skipping bad files, raising errors ~nicer. """
    
    # All iteration logic has been moved into celery_processing_core
    participant = Participant.objects.get(id=participant_id)
    easy_run(participant)

from datetime import datetime, timedelta

from config.constants import DATA_PROCESSING_CELERY_QUEUE
from config.settings import FILE_PROCESS_PAGE_SIZE
from database.user_models import Participant
from libs.celery_control import (get_processing_active_job_ids, processing_celery_app,
    safe_apply_async)
from libs.file_processing.file_processing_core import do_process_user_file_chunks
from libs.sentry import make_error_sentry, SentryTypes


################################################################################
############################# Data Processing ##################################
################################################################################


def create_file_processing_tasks():
    """ Generates tasks to enqueue.  This is called every 6 minutes, and tasks have a lifetime
    of 6 minutes.  Note that tasks are not removed from the queue by RabbitMQ, but by Celery.
    inspecting the queue will continue to display the tasks that have not been sent to Celery
    until the most recent job is finished.

    Also, for some reason 5 minutes is the smallest value that .... works.  At all.
    No clue why.
    """

    # set the tasks to expire at the 5 minutes and thirty seconds mark after the most recent
    # 6 minutely cron task. This way all tasks will be revoked at the same, and well-known, instant.
    expiry = (datetime.utcnow() + timedelta(minutes=5)).replace(second=30, microsecond=0)

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
            # Queue all users' file processing, and generate a list of currently running jobs
            # to use to detect when all jobs are finished running.
            safe_apply_async(
                celery_process_file_chunks,
                args=[participant_id],
                max_retries=0,
                expires=expiry,
                task_track_started=True,
                task_publish_retry=False,
                retry=False
            )
        print(f"{len(participants_to_process)} users queued for processing")


@processing_celery_app.task(queue=DATA_PROCESSING_CELERY_QUEUE)
def celery_process_file_chunks(participant_id):
    """ This is the function is queued up, it runs through all new uploads from a specific user and
    'chunks' them. Handles logic for skipping bad files, raising errors. """

    # celery doesn't clean up after itself very well, either memory or open network connections.
    # this probably has something to do with the fact that celery forks, so possibly picking
    # a different mode would impact this.  Or we can just exit the python process.
    try:
        time_start = datetime.now()
        participant = Participant.objects.get(id=participant_id)

        number_bad_files = 0
        tags = {'user_id': participant.patient_id}
        error_sentry = make_error_sentry(sentry_type=SentryTypes.data_processing)
        print("processing files for %s" % participant.patient_id)

        while True:
            previous_number_bad_files = number_bad_files
            starting_length = participant.files_to_process.exclude(deleted=True).count()

            print("%s processing %s, %s files remaining" % (datetime.now(), participant.patient_id, starting_length))
            number_bad_files += do_process_user_file_chunks(
                    page_size=FILE_PROCESS_PAGE_SIZE,
                    error_handler=error_sentry,
                    position=number_bad_files,
                    participant=participant,
            )
            # If no files were processed, quit processing
            if participant.files_to_process.exclude(deleted=True).count() == starting_length:
                if previous_number_bad_files == number_bad_files:
                    # 2 Cases:
                    #   1) every file broke, blow up. (would cause infinite loop otherwise).
                    #   2) no new files.
                    break
                else:
                    continue

            # put maximum time limit per user
            if (time_start - datetime.now()).total_seconds() > 60*60*3:
                    break

    finally:
        print(
            "IGNORE 'ConnectionResetError: [Errno 104] Connection reset by peer'\n"
            "WE EXIT IN ORDER TO FIX A MEMORY LEAK THAT SO FAR DEFIES ANALYSIS. CELERY COMPLAINS."
        )
        exit(0)


# and mark it to not retry!
celery_process_file_chunks.max_retries = 0

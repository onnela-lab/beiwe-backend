import logging
from datetime import timedelta
from typing import List

from dateutil.tz import gettz
from django.utils import timezone

from constants.celery_constants import PUSH_NOTIFICATION_SEND_QUEUE
from constants.common_constants import RUNNING_TESTS
from libs.celery_control import push_send_celery_app, safe_apply_async
from libs.firebase_config import check_firebase_instance
from libs.sentry import make_error_sentry, SentryTypes, time_warning_data_processing
from services.heartbeat_push_notifications import (celery_heartbeat_send_push_notification_task,
    heartbeat_query)
from services.resend_push_notifications import restore_scheduledevents_logic
from services.survey_push_notifications import (get_surveys_and_schedules,
    send_scheduled_event_survey_push_notification_logic)


logger = logging.getLogger("push_notifications")
if RUNNING_TESTS:
    logger.setLevel(logging.ERROR)
else:
    logger.setLevel(logging.INFO)

log = logger.info
logw = logger.warning
loge = logger.error
logd = logger.debug

UTC = gettz("UTC")


####################################################################################################
################################### SURVEY PUSH NOTIFICATIONS ######################################
####################################################################################################


@time_warning_data_processing("global push notification queuing task took over 3 minutes", 3*60)
def create_survey_push_notification_tasks():
    # we reuse the high level strategy from data processing celery tasks, see that documentation.
    # (this used datetime.utcnow().... I hope nothing breaks?)
    expiry = (timezone.now().astimezone(UTC) + timedelta(minutes=5)).replace(second=30, microsecond=0)
    
    # though complex this query logic really should be on the order of a few seconds max. Running it
    # before every push notification task means we might actually hit the 30 minute timeout.
    restore_scheduledevents_logic()
    
    now = timezone.now()
    surveys, schedules, patient_ids = get_surveys_and_schedules(now)
    log("Surveys:", surveys)
    log("Schedules:", schedules)
    log("Patient_ids:", patient_ids)
    
    with make_error_sentry(sentry_type=SentryTypes.data_processing):
        if not check_firebase_instance():
            loge("Firebase is not configured, cannot queue notifications.")
            return
        
        # surveys and schedules are guaranteed to have the same keys, assembling the data structures
        # is a pain, so it is factored out. sorry, but not sorry. it was a mess.
        for fcm_token in surveys.keys():
            log(f"Queueing up push notification for user {patient_ids[fcm_token]} for {surveys[fcm_token]}")
            safe_apply_async(
                celery_send_survey_push_notification,
                args=[fcm_token, surveys[fcm_token], schedules[fcm_token]],
                max_retries=0,
                expires=expiry,
                task_track_started=True,
                task_publish_retry=False,
                retry=False,
            )


@push_send_celery_app.task(queue=PUSH_NOTIFICATION_SEND_QUEUE)
def celery_send_survey_push_notification(
    fcm_token: str, survey_obj_ids: List[str], schedule_pks: List[int]
):
    """ Passthrough for the survey push notification function, just a wrapper for celery. """
    send_scheduled_event_survey_push_notification_logic(
        fcm_token,
        survey_obj_ids,
        schedule_pks,
        make_error_sentry(sentry_type=SentryTypes.data_processing),
    )


####################################################################################################
######################################## HEARTBEAT #################################################
####################################################################################################


def create_heartbeat_tasks():
    if not check_firebase_instance():
        loge("Heartbeat - Firebase credentials are not configured.")
        return
    
    # gonna try timezone.now() and see what happens.
    expiry = (timezone.now() + timedelta(minutes=5)).replace(second=30, microsecond=0)
    # to reduce database operations in celery_heartbeat_send_push_notification, which may have
    # A LOT of participants that it hits, we run the complex query here and do a single database
    # query in celery_heartbeat_send_push_notification.
    push_notification_data = heartbeat_query()
    log(f"Sending heartbeats to {len(push_notification_data)} "
        "participants considered active in the past week.")
    
    # dispatch the push notifications celery tasks
    for participant_id, fcm_token, os_type, message in heartbeat_query():
        safe_apply_async(
            celery_heartbeat_send_push_notification,
            args=[participant_id, fcm_token, os_type, message],
            max_retries=0,
            expires=expiry,
            task_track_started=True,
            task_publish_retry=False,
            retry=False,
        )


@push_send_celery_app.task(queue=PUSH_NOTIFICATION_SEND_QUEUE)
def celery_heartbeat_send_push_notification(participant_id: int, fcm_token: str, os_type, message: str):
    # just a passthrough for the heartbeat push notification function, just a wrapper for celery.
    celery_heartbeat_send_push_notification_task(participant_id, fcm_token, os_type, message)


# can't be factored out easily because it requires the celerytask function object.
# 2024-1-13 - it's not clear anymore if this is required .
celery_send_survey_push_notification.max_retries = 0

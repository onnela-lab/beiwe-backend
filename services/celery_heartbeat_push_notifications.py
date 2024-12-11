import logging
from datetime import timedelta
from typing import List, Tuple

from django.utils import timezone

from constants import action_log_messages
from constants.celery_constants import PUSH_NOTIFICATION_SEND_QUEUE
from constants.common_constants import RUNNING_TESTS
from database.user_models_participant import Participant, ParticipantActionLog
from libs.celery_control import push_send_celery_app, safe_apply_async
from libs.firebase_config import check_firebase_instance
from libs.push_notification_helpers import (fcm_for_pushable_participants,
    send_custom_notification_safely)
from libs.sentry import make_error_sentry, SentryTypes


logger = logging.getLogger("push_notifications")
if RUNNING_TESTS:
    logger.setLevel(logging.ERROR)
else:
    logger.setLevel(logging.INFO)

log = logger.info
logw = logger.warning
loge = logger.error
logd = logger.debug

####################################################################################################
######################################## HEARTBEAT #################################################
####################################################################################################
# There are two senses in which the term "heartbeat" is used in this codebase. One is with respect
# to the push notification that this celery task pushes to the app, the other is with respect to
# the periodic checkin that the app makes to the backend.  The periodic checkin is app-code, it hits
# the mobile_endpoints.mobile_heartbeat endpoint.

def heartbeat_query() -> List[Tuple[int, str, str, str]]:
    """ Handles logic of finding all active participants and providing the information required to
    send them all the "heartbeat" push notification to keep them up and running. """
    now = timezone.now()
    
    query = fcm_for_pushable_participants(now - timedelta(days=7)).values_list(
        "participant_id",
        "token",
        "participant__os_type",
        "participant__study__device_settings__heartbeat_message",
        "participant__study__device_settings__heartbeat_timer_minutes",
        # only send one notification per participant per heartbeat period.
        "participant__last_heartbeat_notification",
        # These are the ACTIVE_PARTICIPANT_FIELDS in query form
        'participant__last_upload',
        'participant__last_get_latest_surveys',
        'participant__last_set_password',
        'participant__last_set_fcm_token',
        'participant__last_get_latest_device_settings',
        'participant__last_register_user',
        "participant__last_heartbeat_checkin",
    )
    
    # We used to use the AppHeartbeats table inside a clever query, but when we added customizable
    # per-study heartbeat timers that query became too complex. Now we filter out participants that
    # have ACTIVE_PARTICIPANT_FIELDS that are too recent manually in python. All of the information
    # is contained within a single query, which is much more performant than running extra queries
    # in the push notification celery task. This performance should be adequate up to thousands of
    # participants taking seconds, not minutes.
    
    # check if the time to send the next notification has passed, if so, add to the return list.
    # t1 - t8 are all of the fields we check for activity by getting the most recent one.
    ret = []
    for participant_id, token, os_type, message, heartbeat_minutes, t1, t2, t3, t4, t5, t6, t7, t8 in query:
        # need to filter out Nones
        most_recent_time_field = max(t for t in (t1, t2, t3, t4, t5, t6, t7, t8) if t)
        
        # We offset by one minute due to periodicity of the task, this should fix off-by-six-minutes bugs.
        point_at_which_to_send_next_notification = \
            most_recent_time_field + timedelta(minutes=heartbeat_minutes - 1)
        # debugging code
        # log("heartbeat_minutes:", heartbeat_minutes)
        # log("last_heartbeat_notification:", t1)
        # log("last_upload:", t2)
        # log("last_get_latest_surveys:", t3)
        # log("last_set_password:", t4)
        # log("last_set_fcm_token:", t5)
        # log("last_get_latest_device_settings:", t6)
        # log("last_register_user:", t7)
        # log("last_heartbeat_checkin:", t8)
        # log("most_recent_time_field:", most_recent_time_field)
        # log("point_at_which_to_send_next_notification:", point_at_which_to_send_next_notification)
        if now > point_at_which_to_send_next_notification:
            ret.append((participant_id, token, os_type, message))
    
    return ret


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


# fixme: override the nonce value so it doesn't back up many notifications? need to test behavior if the participant has dismissed the notification before implementing.
@push_send_celery_app.task(queue=PUSH_NOTIFICATION_SEND_QUEUE)
def celery_heartbeat_send_push_notification(participant_id: int, fcm_token: str, os_type, message: str):
    with make_error_sentry(sentry_type=SentryTypes.data_processing):
        now = timezone.now()
        if not check_firebase_instance():
            loge("Heartbeat - Firebase credentials are not configured.")
            return
        
        if send_custom_notification_safely(fcm_token, os_type, "Heartbeat", message):
            # update the last heartbeat time using minimal database operations, create log entry.
            Participant.objects.filter(pk=participant_id).update(last_heartbeat_notification=now)
            ParticipantActionLog.objects.create(
                participant_id=participant_id,
                action=action_log_messages.HEARTBEAT_PUSH_NOTIFICATION_SENT,
                timestamp=now
            )


# can't be factored out easily because it requires the celerytask function object.
# 2024-1-13 - it's not clear anymore if this is required .
celery_heartbeat_send_push_notification.max_retries = 0

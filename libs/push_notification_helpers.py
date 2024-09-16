import logging
import random
from datetime import datetime
from typing import List

from cronutils import null_error_handler
from dateutil.tz import gettz
from django.utils import timezone
from firebase_admin.messaging import (AndroidConfig, Message, Notification, QuotaExceededError,
    send as send_notification, SenderIdMismatchError, ThirdPartyAuthError, UnregisteredError)

from constants.common_constants import RUNNING_TESTS
from constants.message_strings import MESSAGE_SEND_SUCCESS
from constants.security_constants import OBJECT_ID_ALLOWED_CHARS
from constants.user_constants import ANDROID_API
from database.schedule_models import ArchivedEvent
from database.study_models import Study
from database.survey_models import Survey
from database.user_models_participant import Participant, ParticipantFCMHistory
from libs.utils.date_utils import date_is_in_the_past


# same logger as in celery_push_notifications
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

#
## Somewhat common code (regular SURVEY notifications have extra logic)
#

def send_custom_notification_safely(fcm_token:str, os_type: str, logging_tag: str, message: str) -> bool:
    """ Our wrapper around the firebase send_notification function. Returns True if successful,
    False if unsuccessful, and may raise errors that have been seen over time.  Any errors raised
    SHOULD be raised and reported because they are unknown failure modes. This code is taken and
    modified from the Survey Push Notification logic, which has special cases because those
    notifications recur on known schedules, this function is more for one-off type of notifications.
    (Though we do log the events outside of the scopes of this function.) """
    # for full documentation of these errors see celery_send_survey_push_notification.
    try:
        send_custom_notification_raw(fcm_token, os_type, message)
        return True
    except UnregisteredError:
        # this is the only "real" error we handle here because we may as well update the fcm
        # token as invalid as soon as we know.  DON'T raise the error, this is normal behavior.
        log(f"\n{logging_tag} - UnregisteredError\n")
        ParticipantFCMHistory.objects.filter(token=fcm_token).update(unregistered=timezone.now())
        return False
    
    except ThirdPartyAuthError as e:
        logw(f"\n{logging_tag} - ThirdPartyAuthError\n")
        if str(e) != "Auth error from APNS or Web Push Service":
            raise
        return False
    
    except ValueError as e:
        logw(f"\n{logging_tag} - ValueError\n")
        if "The default Firebase app does not exist" not in str(e):
            raise
        return False
    
    except (SenderIdMismatchError, QuotaExceededError):
        return False


def send_custom_notification_raw(fcm_token: str, os_type: str, message: str):
    """ Our wrapper around the firebase send_notification function. """
    # we need a nonce because duplicate notifications won't be delivered.
    data_kwargs = {
        # trunk-ignore(bandit/B311)
        'nonce': ''.join(random.choice(OBJECT_ID_ALLOWED_CHARS) for _ in range(32)),
    }
    # os requires different setup
    if os_type == ANDROID_API:
        data_kwargs['type'] = 'message'
        data_kwargs['message'] = message
        message = Message(android=AndroidConfig(data=data_kwargs, priority='high'), token=fcm_token)
    else:
        message = Message(
            data=data_kwargs, token=fcm_token, notification=Notification(title="Beiwe", body=message)
        )
    send_notification(message)



def get_stopped_study_ids() -> List[int]:
    """ Returns a list of study ids that are stopped or deleted (and should not have *stuff* happen.)"""
    bad_study_ids = []
    
    # we don't really care about performance, there are AT MOST hundreds of studies.
    query = Study.objects.values_list("id", "deleted", "manually_stopped", "end_date", "timezone_name")
    for study_id, deleted, manually_stopped, end_date, timezone_name in query:
        if deleted or manually_stopped:
            bad_study_ids.append(study_id)
            continue
        if end_date:
            if date_is_in_the_past(end_date, timezone_name):
                bad_study_ids.append(study_id)
    
    return bad_study_ids


#
## Some Debugging code for use in a terminal
#

# TODO: update these with new non-scheduled-event paradigm

def debug_send_valid_survey_push_notification(participant: Participant, now: datetime = None):
    """ Runs the REAL LOGIC for sending push notifications based on the time passed in, but without
    the ErrorSentry. """
    
    from services.celery_push_notifications import (check_firebase_instance,
        get_surveys_and_schedules, send_scheduled_event_survey_push_notification_logic)
    
    if not now:
        now = timezone.now()
    # get_surveys_and_schedules for one participant, extra args are query filters on ScheduledEvents.
    surveys, schedules, _ = get_surveys_and_schedules(now, participant=participant)
    
    if len(surveys) == 0:
        print(f"There are no surveys to send push notifications for {participant}.")
        return
    if len(surveys) > 1:
        print("There are multiple participants to send push notifications for...")
        return
    
    # it is exactly one participant, get the number of surveys in item one
    survey_object_ids = list(surveys.values())[0]
    print(f"sending {len(survey_object_ids)} notifications to", participant)
    for survey in Survey.objects.filter(object_id__in=survey_object_ids):
        print(f"Sending notification for survey '{survey.name if survey.name else survey.object_id}'")
    
    if not check_firebase_instance():
        print("Firebase is not configured, cannot queue notifications.")
        return
    
    for fcm_token in surveys.keys():
        send_scheduled_event_survey_push_notification_logic(
            fcm_token, surveys[fcm_token], schedules[fcm_token], null_error_handler
        )


def debug_send_all_survey_push_notification(participant: Participant):
    """ Debugging function that sends a survey notification for all surveys on a study. """
    
    from services.celery_push_notifications import send_scheduled_event_survey_push_notification_logic
    
    fcm_token = participant.get_valid_fcm_token().token
    if not fcm_token:
        print("no valid token")
        return
    
    surveys: List[Survey] = list(participant.study.surveys.filter(deleted=False))
    if not surveys:
        print(f"There are no surveys to send push notifications for {participant}.")
        return
    
    print(f"Sending {len(surveys)} notifications to", participant)
    for survey in surveys:
        print(f"Sending notification for survey '{survey.name if survey.name else survey.object_id}'")
    
    survey_obj_ids = [survey.object_id for survey in surveys]
    print(survey_obj_ids)
    send_scheduled_event_survey_push_notification_logic(fcm_token, survey_obj_ids, None, null_error_handler, debug=True)
    
    # and create some fake archived events
    timezone.now()
    for survey in surveys:
        ArchivedEvent(
            survey_archive=survey.most_recent_archive(),
            participant=participant,
            schedule_type="DEBUG",
            scheduled_time=None,
            status=MESSAGE_SEND_SUCCESS,
            uuid=None,
        ).save()

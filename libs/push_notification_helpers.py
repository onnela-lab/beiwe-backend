from __future__ import annotations

import logging
import operator
import random
from datetime import datetime
from functools import reduce
from threading import Lock
from time import perf_counter

from cronutils import ErrorSentry, null_error_handler
from dateutil.tz import gettz
from django.db.models import Q, QuerySet
from django.utils import timezone
from firebase_admin.messaging import (AndroidConfig, Message, Notification, QuotaExceededError,
    send as send_notification, SenderIdMismatchError, ThirdPartyAuthError, UnregisteredError)

# do not import from libs.schedules
from constants.common_constants import RUNNING_TESTS
from constants.message_strings import MESSAGE_SEND_SUCCESS
from constants.security_constants import OBJECT_ID_ALLOWED_CHARS
from constants.user_constants import ACTIVE_PARTICIPANT_FIELDS, ANDROID_API, IOS_API
from database.schedule_models import ArchivedEvent
from database.study_models import Study
from database.survey_models import Survey
from database.user_models_participant import Participant, ParticipantFCMHistory
from libs.celery_control import make_error_sentry
from libs.sentry import SentryTypes


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


class ParticipantCache:
    """ Push notifications can cause a lot of database queries, so we cache the participants. """
    
    fcm_to_participant_id: dict[str, int] = {}
    participant_id_to_participant: dict[int, Participant] = {}
    lock_time: float | None = None  # None on first call
    timer_timeout = 10 * 60  # 10 minutes in seconds
    lock = Lock()
    
    @classmethod
    def check_populate(cls):
        print("yo checking if we need to populate")
        print("yo locking")
        # "now" is a higher number than the lock time, subtraction yields seconds since lock time.
        # (this check must execute once here and once after the lock)
        if cls.lock_time is None or (perf_counter() - cls.lock_time) > cls.timer_timeout:
            pass  # proceed to populate
        else:
            print("did not need to populate")
            return
        
        # NOW lock, check if still need to populate again after lock unlocks.
        with cls.lock:
            # (this check must occur once here and once within the lock)
            assert cls.lock_time is not None, "participant cache failed"
            if (perf_counter() - cls.lock_time) > cls.timer_timeout:
                print("did not need to populate")
                return
            
            # condition to populate is met, repopulate the cache.
            print("yo populating")
            cls.participant_id_to_participant = {p.pk: p for p in Participant.fltr()}
            cls.fcm_to_participant_id = ParticipantFCMHistory.make_lookup_dict("token", "participant_id")
            cls.lock_time = perf_counter()
    
    @classmethod
    def get_participant_by_fcm(cls, token: str) -> Participant:
        cls.check_populate()
        
        if token not in cls.fcm_to_participant_id:
            p = Participant.obj_get(fcm_tokens__token=token)
            cls.participant_id_to_participant[p.pk] = p
            cls.fcm_to_participant_id[token] = p.pk
        
        return cls.participant_id_to_participant[cls.fcm_to_participant_id[token]]


class ErrorSentryCache:
    """ ErrorSentry objects cause IO on creatiion, so we cache them for 10 minutes. """
    
    lock_time: float | None = None  # None on first call
    timer_timeout = 10 * 60  # 10 minutes in seconds
    lock = Lock()
    error_sentry: ErrorSentry|None = None  # None on first call
    
    @classmethod
    def check_populate(cls):
        # check, lock, check again after unlock, 
        if cls.lock_time is None or (perf_counter() - cls.lock_time) > cls.timer_timeout:
            with cls.lock:
                if cls.lock_time is None or (perf_counter() - cls.lock_time) > cls.timer_timeout:
                    cls.error_sentry = SentryTypes.error_handler_push_notifications()
                    cls.lock_time = perf_counter()
    
    @classmethod
    def get_sentry_processing(cls) -> ErrorSentry:
        cls.check_populate()
        assert cls.error_sentry is not None, "ErrorSentryCache failed."
        return cls.error_sentry
#
## Somewhat common code (regular SURVEY notifications have extra logic)
#

def send_custom_notification_safely(fcm_token: str, os_type: str, logging_tag: str, message: str) -> bool:
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


def slowly_get_stopped_study_ids() -> list[int]:
    """ Returns a list of study ids that are stopped or deleted. """
    # testing for a stopped study is hard enough that we should engineer this to depend on the Study
    # objects. There are at-most hundreds of studies, so performance is good enough.  Me can use 
    # only() to reduce the number of fields we pull from the database.
    bad_study_ids = []
    for study in Study.objects.all().only("deleted", "manually_stopped", "end_date", "timezone_name"):
        if study.study_is_stopped:
            bad_study_ids.append(study.id)
    
    return bad_study_ids


def generate_active_participant_Q_object(last_activity_cutoff: datetime) -> Q:
    """ Generates a Q object that is compatible with any database table that has a foreign key
    relation to Participant. Add the Q object to .filter to get participants that have been active
    in the past week.  Does not filter by the permanently_retired field. """
    # (e.g. filter out participants that have not been active in the past week.)
    activity_qs = [
        # Need to do string interpolation to get the field name, using a **{} inline dict unpacking.
        # Creates a Q object like: Q(participant__last_upload__gte=one_week_ago)
        Q(**{f"participant__{field_name}__gte": last_activity_cutoff}) for field_name in
        ACTIVE_PARTICIPANT_FIELDS if field_name != "permanently_retired"
    ]
    
    # uses operator.or_ (note the underscore) to combine all those Q objects as an any match query.
    # (operator.or_ is the same as |, it is the bitwise or operator. Reduce applies it to all items.)
    any_activity_field_gte_cutoff = reduce(operator.or_, activity_qs) 
    return any_activity_field_gte_cutoff


def fcm_for_pushable_participants(last_activity_cutoff: datetime) -> QuerySet[ParticipantFCMHistory]:
    """
    Filter on fcm tokens  for only participants with ACTIVE_PARTICIPANT_FIELDS that were updated in
    the last week, exclude deleted and permanently_retired participants, exclude participants that
    do not have heartbeat enabled, and only where there is a valid FCM token (unregistered=None).
    
    This query could theoretically return multiple fcm tokens per participant, which is not ideal,
    but we haven't had obvious problems in the normal push notification logic ever, and it would
    require a race condition in the endpoint where fcm tokens are set, and ... its just a push
    notification.
    """
    any_activity_field_gte_cutoff = generate_active_participant_Q_object(last_activity_cutoff)
    
    return ParticipantFCMHistory.objects.filter(
        any_activity_field_gte_cutoff,             # participants active in the past "week"
        
        participant__deleted=False,                # no deleted participants
        participant__permanently_retired=False,    # not redundant with deleted.
        unregistered=None,                         # this is fcm-speak for "has non-retired fcm token"
        participant__os_type__in=[ANDROID_API, IOS_API],  # participants need to _have an OS_.
    ).exclude(
        participant__study_id__in=slowly_get_stopped_study_ids()  # no stopped studies
    )

#
## Some Debugging code for use in a terminal
#

# TODO: update these with new non-scheduled-event paradigm

def debug_send_valid_survey_push_notification(participant: Participant, now: datetime = None):
    """ Runs the REAL LOGIC for sending push notifications based on the time passed in, but without
    the ErrorSentry. """
    
    from libs.firebase_config import BackendFirebaseAppState
    from services.celery_push_notifications import (get_surveys_and_schedules,
        send_scheduled_event_survey_push_notification_logic)
    
    if now is None:
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
    
    if not BackendFirebaseAppState.check():
        print("Firebase is not configured, cannot queue notifications.")
        return
    
    for fcm_token in surveys.keys():
        send_scheduled_event_survey_push_notification_logic(
            fcm_token, surveys[fcm_token], schedules[fcm_token], null_error_handler
        )


def debug_send_all_survey_push_notification(participant: Participant):
    """ Debugging function that sends a survey notification for all surveys on a study. """
    
    from services.celery_push_notifications import (
        send_scheduled_event_survey_push_notification_logic)
    
    fcm_token = participant.get_valid_fcm_token().token
    if not fcm_token:
        print("no valid token")
        return
    
    surveys: list[Survey] = list(participant.study.surveys.filter(deleted=False))
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

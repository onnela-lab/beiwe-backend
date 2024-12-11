import logging
import random
import uuid
from collections import defaultdict
from datetime import datetime, timedelta
from typing import List, Tuple

import orjson
from cronutils.error_handler import ErrorSentry
from dateutil.tz import gettz
from django.utils import timezone
from firebase_admin.messaging import (AndroidConfig, Message, Notification, QuotaExceededError,
    send as send_notification, SenderIdMismatchError, ThirdPartyAuthError, UnregisteredError)

from config.settings import BLOCK_QUOTA_EXCEEDED_ERROR, PUSH_NOTIFICATION_ATTEMPT_COUNT
from constants import action_log_messages
from constants.celery_constants import PUSH_NOTIFICATION_SEND_QUEUE
from constants.common_constants import API_TIME_FORMAT, RUNNING_TESTS
from constants.message_strings import (ACCOUNT_NOT_FOUND, CONNECTION_ABORTED,
    FAILED_TO_ESTABLISH_CONNECTION, MESSAGE_SEND_SUCCESS, UNEXPECTED_SERVICE_RESPONSE,
    UNKNOWN_REMOTE_ERROR)
from constants.security_constants import OBJECT_ID_ALLOWED_CHARS
from constants.user_constants import ANDROID_API
from database.schedule_models import ScheduledEvent
from database.survey_models import Survey
from database.user_models_participant import (Participant, ParticipantActionLog,
    ParticipantFCMHistory, PushNotificationDisabledEvent)
from libs.celery_control import push_send_celery_app, safe_apply_async
from libs.firebase_config import check_firebase_instance
from libs.internal_types import DictOfStrStr, DictOfStrToListOfStr
from libs.push_notification_helpers import (fcm_for_pushable_participants,
    send_custom_notification_safely, slowly_get_stopped_study_ids)
from libs.sentry import make_error_sentry, SentryTypes
from services.resend_push_notifications import undelete_events_based_on_lost_notification_checkin


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


def get_or_mock_schedules(schedule_pks: List[str], debug: bool) -> List[ScheduledEvent]:
    """ In order to have debug functions and certain tests run we need to be able to mock a schedule
    object. In all other cases we query the database. """
    if not debug:
        return list(ScheduledEvent.objects.filter(pk__in=schedule_pks))
    else:
        # object needs a scheduled_time attribute, and a falsey uuid attribute.
        class mock_reference_schedule:
            scheduled_time = timezone.now()
            uuid = uuid.uuid4()
        return [mock_reference_schedule]


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


####################################################################################################
################################### SURVEY PUSH NOTIFICATIONS ######################################
####################################################################################################

def get_surveys_and_schedules(now: datetime, **filter_kwargs) -> Tuple[DictOfStrToListOfStr, DictOfStrToListOfStr, DictOfStrStr]:
    """ Mostly this function exists to reduce mess. returns:
    a mapping of fcm tokens to list of survey object ids
    a mapping of fcm tokens to list of schedule ids
    a mapping of fcm tokens to patient ids """
    log(f"\nChecking for scheduled events that are in the past (before {now})")
    
    # we need to find all possible events and convert them on a per-participant-timezone basis.
    # The largest timezone offset is +14?, but we will do one whole day and manually filter.
    tomorrow = now + timedelta(days=1)
    
    # oct 2024: turns out we get TENS OF THOUSANDS of hits, so we need to filter better.
    # (humorously, this came from trying to debug something, not from slowness.)
    # We can filter out participants that have been marked as unreachable:
    valid_participant_ids = ParticipantFCMHistory.objects.filter(unregistered__isnull=True)\
        .values_list("participant_id", flat=True).distinct()
    
    # get: schedule time is in the past for participants that have fcm tokens.
    # need to filter out unregistered fcms, database schema sucks for that, do it in python. its fine.
    query = ScheduledEvent.objects.filter(
        participant_id__in=valid_participant_ids,
        # core
        scheduled_time__lte=tomorrow,
        participant__fcm_tokens__isnull=False,
        # safety
        participant__deleted=False,
        participant__permanently_retired=False,
        survey__deleted=False,
        # Shouldn't be necessary, placeholder containing correct lte count.
        # participant__push_notification_unreachable_count__lte=PUSH_NOTIFICATION_ATTEMPT_COUNT
        deleted=False, # added august 2022, part of checkins
    ) \
    .filter(**filter_kwargs) \
    .exclude(
        survey__study_id__in=slowly_get_stopped_study_ids(),  # no stopped studies
    ) \
    .values_list(
        "scheduled_time",
        "survey__object_id",
        "survey__study__timezone_name",
        "participant__fcm_tokens__token",
        "pk",
        "participant__patient_id",
        "participant__fcm_tokens__unregistered",
        "participant__timezone_name",
        "participant__unknown_timezone",
    )
    
    # we need a mapping of fcm tokens (a proxy for participants) to surveys and schedule ids (pks)
    surveys = defaultdict(list)
    schedules = defaultdict(list)
    patient_ids = {}
    
    # unregistered means that the FCM push notification token has been marked as unregistered, which
    # is fcm-speak for invalid push notification token. It's probably possible to update the query
    # to bad fcm tokens, but it becomes complex. The filtering is fast enough in Python.
    unregistered: bool
    fcm: str  # fcm token
    patient_id: str
    survey_obj_id: str
    scheduled_time: datetime  # in UTC
    schedule_id: int
    study_tz_name: str
    participant_tz_name: str
    participant_has_bad_tz: bool
    for scheduled_time, survey_obj_id, study_tz_name, fcm, schedule_id, patient_id, unregistered, participant_tz_name, participant_has_bad_tz in query:
        logd("\nchecking scheduled event:")
        logd("unregistered:", unregistered)
        logd("fcm:", fcm)
        logd("patient_id:", patient_id)
        logd("survey_obj_id:", survey_obj_id)
        logd("scheduled_time:", scheduled_time)
        logd("schedule_id:", schedule_id)
        logd("study_tz_name:", study_tz_name)
        logd("participant_tz_name:", participant_tz_name)
        logd("participant_has_bad_tz:", participant_has_bad_tz)
        
        # case: this instance has an outdated FCM credential, skip it.
        if unregistered:
            logd("nope, unregistered fcm token")
            continue
        
        # The participant and study timezones REALLY SHOULD be valid timezone names. If they aren't
        # valid then gettz's behavior is to return None; if gettz receives None or the empty string
        # then it returns UTC. In order to at-least-be-consistent we will coerce no timezone to UTC.
        # (At least gettz caches, so performance should be fine without adding complexity.)
        participant_tz = gettz(study_tz_name) if participant_has_bad_tz else gettz(participant_tz_name)
        participant_tz = participant_tz or UTC
        study_tz = gettz(study_tz_name) or UTC
        
        # ScheduledEvents are created in the study's timezone, and in the database they are
        # normalized to UTC. Convert it to the study timezone time - we'll call that canonical time
        # - which will be the time of day assigned on the survey page. Then time-shift that into the
        # participant's timezone, and check if That value is in the past.
        canonical_time = scheduled_time.astimezone(study_tz)
        participant_time = canonical_time.replace(tzinfo=participant_tz)
        logd("canonical_time:", canonical_time)
        logd("participant_time:", participant_time)
        if participant_time > now:
            logd("nope, participant time is considered in the future")
            logd(f"{now} > {participant_time}")
            continue
        
        logd("yup, participant time is considered in the past")
        logd(f"{now} <= {participant_time}")
        surveys[fcm].append(survey_obj_id)
        schedules[fcm].append(schedule_id)
        patient_ids[fcm] = patient_id
    
    return dict(surveys), dict(schedules), patient_ids


def create_survey_push_notification_tasks():
    # we reuse the high level strategy from data processing celery tasks, see that documentation.
    # (this used datetime.utcnow().... I hope nothing breaks?)
    expiry = (timezone.now().astimezone(UTC) + timedelta(minutes=5)).replace(second=30, microsecond=0)
    
    # though complex this query logic really should be on the order of a few seconds max. Running it
    # before every push notification task means we might actually hit the 30 minute timeout.
    undelete_events_based_on_lost_notification_checkin()
    
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


def send_scheduled_event_survey_push_notification_logic(
    fcm_token: str,
    survey_obj_ids: List[str],
    schedule_pks: List[int],
    error_handler: ErrorSentry,
    debug: bool = False
):
    """ Sends push notifications. Note that this list of pks may contain duplicates. """
    
    # We need the patient_id is so that we can debug anything on Sentry. Worth a database call?
    patient_id = ParticipantFCMHistory.objects.filter(token=fcm_token) \
        .values_list("participant__patient_id", flat=True).get()
    
    with error_handler:
        if not check_firebase_instance():
            loge("Surveys - Firebase credentials are not configured.")
            return
        
        participant = Participant.objects.get(patient_id=patient_id)
        survey_obj_ids = list(set(survey_obj_ids))  # Dedupe-dedupe
        log(f"Sending push notification to {patient_id} for {survey_obj_ids}...")
        
        # we need to mock the reference_schedule object in debug mode... it is stupid.
        scheduled_events = get_or_mock_schedules(schedule_pks, debug)
        
        try:
            inner_send_survey_push_notification(participant, scheduled_events, fcm_token)
        # error types are documented at firebase.google.com/docs/reference/fcm/rest/v1/ErrorCode
        except UnregisteredError:
            log("\nUnregisteredError\n")
            # Is an internal 404 http response, it means the token that was used has been disabled.
            # Mark the fcm history as out of date, return early.
            ParticipantFCMHistory.objects.filter(token=fcm_token).update(unregistered=timezone.now())
            return
        
        except QuotaExceededError as e:
            # Limits are very high, this should be impossible. Reraise because this requires
            # sysadmin attention and probably new development to allow multiple firebase
            # credentials. Read comments in settings.py if toggling.
            if BLOCK_QUOTA_EXCEEDED_ERROR:
                failed_send_survey_handler(participant, fcm_token, str(e), scheduled_events, debug)
                return
            else:
                raise
        
        except ThirdPartyAuthError as e:
            loge("\nThirdPartyAuthError\n")
            failed_send_survey_handler(participant, fcm_token, str(e), scheduled_events, debug)
            # This means the credentials used were wrong for the target app instance.  This can occur
            # both with bad server credentials, and with bad device credentials.
            # We have only seen this error statement, error name is generic so there may be others.
            if str(e) != "Auth error from APNS or Web Push Service":
                raise
            return
        
        except SenderIdMismatchError as e:
            # In order to enhance this section we will need exact text of error messages to handle
            # similar error cases. (but behavior shouldn't be broken anymore, failed_send_handler
            # executes.)
            loge("\nSenderIdMismatchError:\n")
            loge(e)
            failed_send_survey_handler(participant, fcm_token, str(e), scheduled_events, debug)
            return
        
        except ValueError as e:
            loge("\nValueError\n")
            # This case occurs ever? is tested for in check_firebase_instance... weird race
            # condition? Error should be transient, and like all other cases we enqueue the next
            # weekly surveys regardless.
            if "The default Firebase app does not exist" in str(e):
                return
            else:
                raise
        
        except Exception as e:
            failed_send_survey_handler(participant, fcm_token, str(e), scheduled_events, debug)
            raise
        
        success_send_survey_handler(participant, fcm_token, scheduled_events)


def inner_send_survey_push_notification(
    participant: Participant, scheduled_events: List[ScheduledEvent], fcm_token: str
):
    # There can be multiple instances of the same survey for which we need to deduplicate object
    #   ids, but appropriately map all object ids to schedule uuids.
    survey_pks_fltr = list(set(scheduled_event.survey_id for scheduled_event in scheduled_events))
    survey_obj_ids_by_pk = dict(Survey.fltr(pk__in=survey_pks_fltr).values_list("pk", "object_id"))
    
    survey_obj_ids_to_uuids = defaultdict(list)
    for scheduled_event in scheduled_events:
        if scheduled_event.uuid is None:
            continue  # probably unreachable
        survey_obj_ids_to_uuids[survey_obj_ids_by_pk[scheduled_event.survey_id]].append(scheduled_event.uuid)
    
    # looks like ["object_id": "uuid", "object_id": "uuid,uuid"], used to inform backend of receipt
    uuids_json_dict_string = orjson.dumps(dict(survey_obj_ids_to_uuids)).decode()
    # the survey ids for the app to display
    survey_obj_ids = orjson.dumps(list(survey_obj_ids_by_pk.values())).decode()
    # used in the app to sort surveys
    earliest_schedule = min(scheduled_events, key=lambda x: x.scheduled_time)
    
    # Include a nonce to bypass notification deduplication.
    data_kwargs = {
        # trunk-ignore(bandit/B311): this is a nonce, not a password.
        'nonce': ''.join(random.choice(OBJECT_ID_ALLOWED_CHARS) for _ in range(32)),
        'sent_time': earliest_schedule.scheduled_time.strftime(API_TIME_FORMAT),
        'type': 'survey',
        'survey_ids': survey_obj_ids,
        'survey_uuids_dict': uuids_json_dict_string,
    }
    
    if participant.os_type == ANDROID_API:
        message = Message(android=AndroidConfig(data=data_kwargs, priority='high'), token=fcm_token)
    else:
        display_message = \
            "You have a survey to take." if len(survey_obj_ids) == 1 else "You have surveys to take."
        message = Message(
            data=data_kwargs,
            token=fcm_token,
            notification=Notification(title="Beiwe", body=display_message),
        )
    send_notification(message)


def success_send_survey_handler(participant: Participant, fcm_token: str, events: List[ScheduledEvent]):
    # If the query was successful archive the schedules.  Clear the fcm unregistered flag
    # if it was set (this shouldn't happen. ever. but in case we hook in a ui element we need it.)
    log(f"Survey push notification send succeeded for {participant.patient_id}.")
    
    # this condition shouldn't occur.  Leave in, this case would be super stupid to diagnose.
    fcm_hist: ParticipantFCMHistory = ParticipantFCMHistory.objects.get(token=fcm_token)
    if fcm_hist.unregistered is not None:
        fcm_hist.unregistered = None
        fcm_hist.save()
    
    participant.push_notification_unreachable_count = 0
    participant.save()
    
    create_archived_events(events, participant, status=MESSAGE_SEND_SUCCESS)


def failed_send_survey_handler(
    participant: Participant,
    fcm_token: str,
    error_message: str,
    schedules: List[ScheduledEvent],
    debug: bool,
):
    """ Contains body of code for unregistering a participants push notification behavior.
        Participants get reenabled when they next touch the app checkin endpoint. """
    
    # we have encountered some really weird error behavior, we need to normalize the error messages,
    # see TestFailedSendHandler, see migration 128 for some cleanup.
    if "DOCTYPE" in error_message:
        error_message = UNEXPECTED_SERVICE_RESPONSE  # this one is like a 502 proxy error?
    elif "Unknown error while making a remote service call:" in error_message:
        error_message = UNKNOWN_REMOTE_ERROR
    elif "Failed to establish a connection" in error_message:
        error_message = FAILED_TO_ESTABLISH_CONNECTION
    elif "Connection aborted." in error_message:
        error_message = CONNECTION_ABORTED
    elif "invalid_grant" in error_message:
        error_message = ACCOUNT_NOT_FOUND
    
    if participant.push_notification_unreachable_count >= PUSH_NOTIFICATION_ATTEMPT_COUNT:
        now = timezone.now()
        fcm_hist: ParticipantFCMHistory = ParticipantFCMHistory.objects.get(token=fcm_token)
        fcm_hist.unregistered = now
        fcm_hist.save()
        
        PushNotificationDisabledEvent(
            participant=participant, timestamp=now,
            count=participant.push_notification_unreachable_count
        ).save()
        
        # disable the credential
        participant.push_notification_unreachable_count = 0
        participant.save()
        
        logd(f"Participant {participant.patient_id} has had push notifications "
              f"disabled after {PUSH_NOTIFICATION_ATTEMPT_COUNT} failed attempts to send.")
    
    else:
        now = None
        participant.save()
        participant.push_notification_unreachable_count += 1
        logd(f"Participant {participant.patient_id} has had push notifications failures "
              f"incremented to {participant.push_notification_unreachable_count}.")
    
    # don't do the new archive events if this is running in debug mode, raise uncatchable exception
    if debug:
        raise BaseException("debug mode, not archiving events.")
    
    create_archived_events(schedules, participant, status=error_message)


def create_archived_events(events: List[ScheduledEvent], participant: Participant, status: str):
    """ Populates event history, does not mark ScheduledEvents as deleted. """
    for scheduled_event in events:
        scheduled_event.archive(participant, status=status)


# can't be factored out easily because it requires the celerytask function object.
# 2024-1-13 - it's not clear anymore if this is required .
celery_send_survey_push_notification.max_retries = 0
celery_heartbeat_send_push_notification.max_retries = 0

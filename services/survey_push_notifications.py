import logging
import random
import uuid
from collections import defaultdict
from datetime import datetime, timedelta

import orjson
from cronutils.error_handler import ErrorSentry
from dateutil.tz import gettz
from django.utils import timezone
from firebase_admin.messaging import (AndroidConfig, Message, Notification, QuotaExceededError,
    send as send_notification, SenderIdMismatchError, ThirdPartyAuthError, UnregisteredError)

from authentication.data_access_authentication import SentryTypes
from config.settings import BLOCK_QUOTA_EXCEEDED_ERROR, PUSH_NOTIFICATION_ATTEMPT_COUNT
from constants.common_constants import API_TIME_FORMAT, RUNNING_TESTS
from constants.message_strings import (ACCOUNT_NOT_FOUND, CONNECTION_ABORTED,
    FAILED_TO_ESTABLISH_CONNECTION, MESSAGE_SEND_SUCCESS, UNEXPECTED_SERVICE_RESPONSE,
    UNKNOWN_REMOTE_ERROR)
from constants.security_constants import OBJECT_ID_ALLOWED_CHARS
from constants.user_constants import ANDROID_API
from database.schedule_models import ScheduledEvent
from database.survey_models import Survey
from database.user_models_participant import (Participant, ParticipantFCMHistory,
    PushNotificationDisabledEvent)
from libs.firebase_config import BackendFirebaseAppState
from libs.push_notification_helpers import ParticipantCache, slowly_get_stopped_study_ids
from libs.sentry import SentryTypes
from services.resend_push_notifications import (
    get_all_unconfirmed_notification_schedules_for_bundling)


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

def get_or_mock_schedules(event_pks: list[int], debug: bool) -> list[ScheduledEvent]:
    """ In order to have debug functions and certain tests run we need to be able to mock a schedule
    object. In all other cases we query the database. """
    if not debug:
        return list(ScheduledEvent.objects.filter(pk__in=event_pks))
    else:
        # object needs a scheduled_time attribute, and a falsey uuid attribute.
        class mock_reference_schedule:
            scheduled_time = timezone.now()
            uuid = uuid.uuid4()
        return [mock_reference_schedule]  # type: ignore


####################################################################################################
################################### SURVEY PUSH NOTIFICATIONS ######################################
####################################################################################################

SurveyReturn = dict[str, list[str]]     # a dictionary of fcm tokens to lists of survey object ids
SchedulesReturn = dict[str, list[int]]  # a dictionary of fcm tokens to lists of schedule pks
PatientsReturn = dict[str, str]         # a dictionary of fcm tokens to (individual) patient ids

@SentryTypes.timer_warning_push_notifications("push notification query logic took over 30 seconds", 30)
def get_surveys_and_schedules(now: datetime, **filter_kwargs) -> tuple[SurveyReturn, SchedulesReturn, PatientsReturn]:
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
    surveys: defaultdict[str, list[str]] = defaultdict(list)
    schedules: defaultdict[str, list[int]] = defaultdict(list)
    patient_ids = {}
    
    # unregistered means that the FCM push notification token has been marked as unregistered, which
    # is fcm-speak for invalid push notification token. It's probably possible to update the query
    # to bad fcm tokens, but it becomes complex. The filtering is fast enough in Python.
    unregistered: bool
    fcm: str  # fcm token
    patient_id: str
    survey_obj_id: str
    scheduled_time: datetime  # in UTC
    schedule_pk: int
    study_tz_name: str
    participant_tz_name: str
    participant_has_bad_tz: bool
    for scheduled_time, survey_obj_id, study_tz_name, fcm, schedule_pk, patient_id, unregistered, participant_tz_name, participant_has_bad_tz in query:
        logd("\nchecking scheduled event:")
        logd("unregistered:", unregistered)
        logd("fcm:", fcm)
        logd("patient_id:", patient_id)
        logd("survey_obj_id:", survey_obj_id)
        logd("scheduled_time:", scheduled_time)
        logd("schedule_id:", schedule_pk)
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
        schedules[fcm].append(schedule_pk)
        patient_ids[fcm] = patient_id
    
    return  dict(surveys), dict(schedules), patient_ids


def send_scheduled_event_survey_push_notification_logic(
    fcm_token: str,
    survey_obj_ids: list[str],
    schedule_pks: list[int],
    error_handler: ErrorSentry,
    debug: bool = False
):
    """ Sends push notifications. Note that this list of pks may contain duplicates. """
    
    # We need the patient_id is so that we can debug anything on Sentry. Worth a database call?
    # patient_id = ParticipantFCMHistory.objects.filter(token=fcm_token) \
    #     .values_list("participant__patient_id", flat=True).get()
    
    cached_participant = ParticipantCache.get_participant_by_fcm(fcm_token)
    
    with error_handler:
        if not BackendFirebaseAppState.check():
            loge("Surveys - Firebase credentials are not configured.")
            return
        
        survey_obj_ids = list(set(survey_obj_ids))  # Dedupe-dedupe
        log(f"Sending push notification to {cached_participant.patient_id} for {survey_obj_ids}...")
        
        # we need to mock the reference_schedule object in debug mode... it is stupid.
        scheduled_events = get_or_mock_schedules(schedule_pks, debug)
        scheduled_events.extend(get_all_unconfirmed_notification_schedules_for_bundling(cached_participant, schedule_pks))
        
        try:
            inner_send_survey_push_notification(cached_participant, scheduled_events, fcm_token)
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
                failed_send_survey_handler(cached_participant, fcm_token, str(e), scheduled_events, debug)
                return
            else:
                raise
        
        except ThirdPartyAuthError as e:
            loge("\nThirdPartyAuthError\n")
            failed_send_survey_handler(cached_participant, fcm_token, str(e), scheduled_events, debug)
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
            failed_send_survey_handler(cached_participant, fcm_token, str(e), scheduled_events, debug)
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
            failed_send_survey_handler(cached_participant, fcm_token, str(e), scheduled_events, debug)
            raise
        
        success_send_survey_handler(cached_participant, fcm_token, scheduled_events)


def inner_send_survey_push_notification(
    cached_participant: Participant, scheduled_events: list[ScheduledEvent], fcm_token: str
):
    # There can be multiple instances of the same survey for which we need to deduplicate object
    #   ids, but appropriately map all object ids to schedule uuids.
    survey_pks_fltr = list({scheduled_event.survey_id for scheduled_event in scheduled_events})
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
    
    if cached_participant.os_type == ANDROID_API:
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


def success_send_survey_handler(participant: Participant, fcm_token: str, events: list[ScheduledEvent]):
    # If the query was successful archive the schedules.  Clear the fcm unregistered flag
    # if it was set (this shouldn't happen. ever. but in case we hook in a ui element we need it.)
    log(f"Survey push notification send succeeded for {participant.patient_id}.")
    
    # this condition shouldn't occur.  Leave in, this case would be super stupid to diagnose.
    fcm_hist: ParticipantFCMHistory = ParticipantFCMHistory.objects.get(token=fcm_token)
    if fcm_hist.unregistered is not None:
        fcm_hist.unregistered = None
        fcm_hist.save()
    
    participant.update_only(push_notification_unreachable_count=0)
    
    create_archived_events(events, participant, status=MESSAGE_SEND_SUCCESS)


def failed_send_survey_handler(
    participant: Participant,
    fcm_token: str,
    error_message: str,
    schedules: list[ScheduledEvent],
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
        # disable the credential
        now = timezone.now()
        fcm_hist = ParticipantFCMHistory.objects.get(token=fcm_token)
        fcm_hist.unregistered = now
        fcm_hist.save()
        
        PushNotificationDisabledEvent(
            participant=participant, timestamp=now,
            count=participant.push_notification_unreachable_count
        ).save()
        
        logd(f"Participant {participant.patient_id} has had push notifications "
              f"disabled after {PUSH_NOTIFICATION_ATTEMPT_COUNT} failed attempts to send.")
    
    else:
        participant.update_only(
            push_notification_unreachable_count=participant.push_notification_unreachable_count + 1
        )
        logd(f"Participant {participant.patient_id} has had push notifications failures "
              f"incremented to {participant.push_notification_unreachable_count}.")
    
    # don't do the new archive events if this is running in debug mode, raise uncatchable exception
    if debug:
        raise BaseException("debug mode, not archiving events.")
    
    create_archived_events(schedules, participant, status=error_message)


def create_archived_events(events: list[ScheduledEvent], participant: Participant, status: str):
    """ Populates event history, does not mark ScheduledEvents as deleted. """
    for scheduled_event in events:
        scheduled_event.archive(participant, status=status)

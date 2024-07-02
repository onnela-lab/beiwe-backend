import json
import logging
import operator
import random
from collections import defaultdict
from datetime import datetime, timedelta
from functools import reduce
from typing import List, Tuple

from cronutils import null_error_handler
from cronutils.error_handler import ErrorSentry
from dateutil.tz import gettz
from django.db.models import Q
from django.utils import timezone
from firebase_admin.messaging import (AndroidConfig, Message, Notification, QuotaExceededError,
    send as send_notification, SenderIdMismatchError, ThirdPartyAuthError, UnregisteredError)

from config.settings import BLOCK_QUOTA_EXCEEDED_ERROR, PUSH_NOTIFICATION_ATTEMPT_COUNT
from constants import action_log_messages
from constants.celery_constants import PUSH_NOTIFICATION_SEND_QUEUE
from constants.common_constants import API_TIME_FORMAT, RUNNING_TESTS
from constants.message_strings import MESSAGE_SEND_SUCCESS
from constants.schedule_constants import ScheduleTypes
from constants.security_constants import OBJECT_ID_ALLOWED_CHARS
from constants.user_constants import ACTIVE_PARTICIPANT_FIELDS, ANDROID_API, IOS_API
from database.schedule_models import ArchivedEvent, ScheduledEvent
from database.survey_models import Survey
from database.user_models_participant import (Participant, ParticipantActionLog,
    ParticipantFCMHistory, PushNotificationDisabledEvent)
from libs.celery_control import push_send_celery_app, safe_apply_async
from libs.firebase_config import check_firebase_instance
from libs.internal_types import DictOfStrStr, DictOfStrToListOfStr
from libs.schedules import set_next_weekly
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

UTC = gettz("UTC")

#
## Somewhat common code (regular survey notifications have extra logic)
#

def send_notification_safely(fcm_token:str, os_type: str, logging_tag: str, message: str) -> bool:
    """ Our wrapper around the firebase send_notification function. Returns True if successful,
    False if unsuccessful, and may raise errors that have been seen over time.  Any errors raised
    SHOIULD be raised and reported because they are unknown failure modes. This code is taken and
    modified from the Survey Push Notifcation logic, which has special cases because those
    notifications recur on known schedules, this function is more for one-off type of notifications.
    (Though we do log the events outside of the scopes of this function.) """
    # for full documentation of these errors see celery_send_survey_push_notification.
    try:
        _send_notification(fcm_token, os_type, message)
        return True
    except UnregisteredError:
        # this is the only "real" error we handle here because we may as well update the fcm
        # token as invalid as soon as we know.
        log(f"\n{logging_tag} - UnregisteredError\n")
        ParticipantFCMHistory.objects.filter(token=fcm_token).update(unregistered=timezone.now())
        # DON'T raise the error, this is normal behavior.
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


def _send_notification(fcm_token: str, os_type: str, message: str):
    """ our wrapper around the firebase send_notification function. Raises errors. """
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



# This feature is both not named well and not enabled or even tested.
####################################################################################################
###################################### RESURRECTION ###############################################
####################################################################################################

# def create_hard_exit_check_tasks() -> List[Tuple[int, str]]:
#     # the safety check for not having multiple hard exits in the database should be in the endpoint
#     # the app hits when it does that.
#     expiry = (timezone.now() + timedelta(minutes=5)).replace(second=30, microsecond=0)
#     # The big query we would have to make here gets the most recent heartbeat for each participant
#     # and compares it to the most recent hard exit for each participant. That's very hard, so we are
#     # pushing it into celery_resurrection_notification even though that may cause database load
#     # because the the number of concurrent participants with this occurring is very low.
#     pks = IOSHardExits.objects.filter(handled=None).values_list("participant_id", flat=True)
#     for participant_id in pks:
#         safe_apply_async(
#             celery_resurrection_notification,
#             args=participant_id,
#             max_retries=0,
#             expires=expiry,
#             task_track_started=True,
#             task_publish_retry=False,
#             retry=False,
#         )


# @push_send_celery_app.task(queue=PUSH_NOTIFICATION_SEND_QUEUE)
# def celery_resurrection_notification(participant_id: int):
#     if not check_firebase_instance():
#         loge("Resurrection - Surveys - Firebase credentials are not configured.")
#         return
#     with make_error_sentry(sentry_type=SentryTypes.data_processing):
#         resurrection_notification(participant_id)

#TODO: add ParticipantActionLog
# def resurrection_notification(participant_id: int):
#     # check the most recent heartbeat and the most recent hard exit, if the hard exit is newer
#     # send the notifications. We only need the most recent timestamp
#     hard_exit_timestamp = (
#         IOSHardExits.objects.filter(participant_id=participant_id, handled=None)
#         .order_by("-timestamp")
#         .values_list("timestamp", flat=True)
#         .first()
#     )
#    
#     # exit early if there are no hard exits because the participant hit the heartbeat endpoint
#     # between the original query and now.
#     if hard_exit_timestamp is None:
#         return
#    
#     # exit early if there is a later heartbeat - this is potentially expensive? the index Should be
#     # a timestamp ordering index, but it may not be?
#     there_is_a_later_heartbeat = AppHeartbeats.objects.filter(
#         participant_id=participant_id, timestamp__gt=hard_exit_timestamp).exists()
#    
#     if there_is_a_later_heartbeat:
#         log(f"Participant {participant_id} already restarted app.")
#         IOSHardExits.objects.filter(participant_id=participant_id, handled=None).update(handled=timezone.now())
#         return
#    
#     # get the fcm token and send them the notification to reopen the app:
#     fcm_token = (
#         ParticipantFCMHistory.objects
#         .filter(participant_id=participant_id, unregistered=None)
#         .values_list("token", flat=True)
#         .first()
#     )
#    
#     # just give up if they don't have a token and mark as handled because otherwise they are
#     # impossible to get rid of.
#     if not fcm_token:
#         IOSHardExits.objects.filter(participant_id=participant_id, handled=None).update(handled=timezone.now())
#    
#     send_notification_safely(fcm_token, IOS_API, "Resurrection")


####################################################################################################
######################################## HEARTBEAT #################################################
####################################################################################################
# There are two senses in which the term "heartbeat" is used in this codebase. One is with respect
# to the push notification that this celery task pushes to the app, the other is with respect to
# the periodic checkin that the app makes to the backend.  The periodic checkin is app-code, it hits
# the moblile_api.mobile_heartbeat endpoint.

def heartbeat_query() -> List[Tuple[int, str, str, str]]:
    """ Handles logic of finding all active participants and providing the information required to
    send them all the "heartbeat" push notification to keep them up and running. """
    # active is premised on all of the active participant fields being within the last week
    now = timezone.now()
    one_week_ago = now - timedelta(days=7)
    
    # get all participants that have an activity field more recent that the past week.
    # (e.g. filter out participants that have not been active in the past week.)
    activity_qs = [
        # Need to do string interpolation to get the field name, using a **{} inline dict unpacking.
        # Creates a Q object like: Q(participant__last_upload__gte=one_week_ago)
        Q(**{f"participant__{field_name}__gte": one_week_ago}) for field_name in ACTIVE_PARTICIPANT_FIELDS
            if field_name != "permanently_retired"  # handled in main query below.
    ]
    
    # uses operator.or_ (note the underscore) to combine all those Q objects as an any match query.
    # (operator.or_ is the same as |, it is the bitwise or operator) (reduce just applies it)
    any_activity_field_gt_one_week_ago = reduce(operator.or_, activity_qs)
    
    # Get fcm tokens and participant pk for all participants, filter for only participants with
    # ACTIVE_PARTICIPANT_FIELDS that were updated in the last week, exclude deleted and
    # permanently_retired participants, exclude partipcants that do not have heartbeat enabled,
    # and only where there is a valid FCM token (unregistered=None).
    # This query could theoretically return multiple fcm tokens per participant, which is not ideal,
    # but we haven't had obvious problems in the normal push notification logic ever, and it would
    # require a race condition in the endpoint where fcm tokens are set, and ... its just a push
    # notification.
    query = ParticipantFCMHistory.objects.filter(
            any_activity_field_gt_one_week_ago,
            
            participant__deleted=False,                # no deleted participants
            participant__permanently_retired=False,    # should be rendundant with deleted.
            unregistered=None,                         # this is fcm-speak for non-retired fcm token
            participant__os_type__in=[ANDROID_API, IOS_API],  # participants need to _have an OS_.
        )\
        .values_list(
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
        )\
        .order_by("?")  # cover for some slowness by at least not making it predictable... (dumb)
        # .exclude(participant__id__in=participant_ids_with_recent_heartbeats)
    
    # We used to use the AppHeartbeats table inside a clever query, but when we added customizeable
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
        
        if send_notification_safely(fcm_token, os_type, "Heartbeat", message):
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
    
    # get: schedule time is in the past for participants that have fcm tokens.
    # need to filter out unregistered fcms, database schema sucks for that, do it in python. its fine.
    query = ScheduledEvent.objects.filter(
        # core
        scheduled_time__lte=tomorrow,
        participant__fcm_tokens__isnull=False,
        # safety
        participant__deleted=False,
        participant__permanently_retired=False,
        survey__deleted=False,
        # Shouldn't be necessary, placeholder containing correct lte count.
        # participant__push_notification_unreachable_count__lte=PUSH_NOTIFICATION_ATTEMPT_COUNT
        # added august 2022, part of checkins
        deleted=False,
    ) \
    .filter(**filter_kwargs) \
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
    expiry = (datetime.utcnow() + timedelta(minutes=5)).replace(second=30, microsecond=0)
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


def debug_send_valid_survey_push_notification(participant: Participant, now: datetime = None):
    """ Runs the REAL LOGIC for sending push notifications based on the time passed in, but without
    the ErrorSentry. """
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
        do_send_survey_push_notification(
            fcm_token, surveys[fcm_token], schedules[fcm_token], null_error_handler
        )


def debug_send_all_survey_push_notification(participant: Participant):
    """ Debugging function that sends a survey notification for all surveys on a study. """
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
    do_send_survey_push_notification(fcm_token, survey_obj_ids, None, null_error_handler, debug=True)
    
    # and create some fake archived events
    timezone.now()
    for survey in surveys:
        ArchivedEvent(
            survey_archive=survey.most_recent_archive(),
            participant=participant,
            schedule_type="DEBUG",
            scheduled_time=None,
            status="success",
            uuid=None,
        ).save()


@push_send_celery_app.task(queue=PUSH_NOTIFICATION_SEND_QUEUE)
def celery_send_survey_push_notification(
    fcm_token: str, survey_obj_ids: List[str], schedule_pks: List[int]
):
    """ Passthrough for the survey push notification function, just a wrapper for celery. """
    do_send_survey_push_notification(
        fcm_token,
        survey_obj_ids,
        schedule_pks,
        make_error_sentry(sentry_type=SentryTypes.data_processing),
    )


def do_send_survey_push_notification(
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
        reference_schedule, schedules = get_or_mock_schedule(schedule_pks, debug)
        try:
            send_survey_push_notification(
                participant, reference_schedule, survey_obj_ids, fcm_token
            )
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
                failed_send_handler(participant, fcm_token, str(e), schedules, debug)
                return
            else:
                raise
        
        except ThirdPartyAuthError as e:
            loge("\nThirdPartyAuthError\n")
            failed_send_handler(participant, fcm_token, str(e), schedules, debug)
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
            failed_send_handler(participant, fcm_token, str(e), schedules, debug)
            return
        
        except ValueError as e:
            loge("\nValueError\n")
            # This case occurs ever? is tested for in check_firebase_instance... weird race
            # condition? Error should be transient, and like all other cases we enqueue the next
            # weekly surveys regardless.
            if "The default Firebase app does not exist" in str(e):
                enqueue_weekly_surveys(participant, schedules)
                return
            else:
                raise
        
        except Exception as e:
            failed_send_handler(participant, fcm_token, str(e), schedules, debug)
            raise
        
        success_send_handler(participant, fcm_token, schedules)


def get_or_mock_schedule(schedule_pks: List[str], debug: bool) -> Tuple[ScheduledEvent, List[ScheduledEvent]]:
    # in debug mode we need to mock a schedule object, in production we need to get the earliest one.
    if not debug:
        # use the earliest timed schedule as our reference for the sent_time parameter.
        schedules = ScheduledEvent.objects.filter(pk__in=schedule_pks)
        reference_schedule = schedules.order_by("scheduled_time").first()
        return reference_schedule, schedules
    else:
        # object needs a scheduled_time attribute, and a falsey uuid attribute.
        class mock_reference_schedule:
            scheduled_time = timezone.now()
            uuid = ""
        return mock_reference_schedule, []


def send_survey_push_notification(
    participant: Participant, reference_schedule: ScheduledEvent, survey_obj_ids: List[str],
    fcm_token: str
) -> str:
    """ Contains the body of the code to send a notification  """
    # we include a nonce in case of notification deduplication, and a schedule_uuid to for the
    #  checkin after the push notification is sent.
    data_kwargs = {
        'nonce': ''.join(random.choice(OBJECT_ID_ALLOWED_CHARS) for _ in range(32)),
        'sent_time': reference_schedule.scheduled_time.strftime(API_TIME_FORMAT),
        'type': 'survey',
        'survey_ids': json.dumps(survey_obj_ids),
        'schedule_uuid': (reference_schedule.uuid if reference_schedule else "") or ""
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


def success_send_handler(participant: Participant, fcm_token: str, schedules: List[ScheduledEvent]):
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
    
    create_archived_events(schedules, status=MESSAGE_SEND_SUCCESS)
    enqueue_weekly_surveys(participant, schedules)


def failed_send_handler(
    participant: Participant,
    fcm_token: str,
    error_message: str,
    schedules: List[ScheduledEvent],
    debug: bool,
):
    """ Contains body of code for unregistering a participants push notification behavior.
        Participants get reenabled when they next touch the app checkin endpoint. """
    
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
    
    create_archived_events(schedules, status=error_message, created_on=now)
    enqueue_weekly_surveys(participant, schedules)


def create_archived_events(schedules: List[ScheduledEvent], status: str, created_on: datetime = None):
    # """ Populates event history, does not mark ScheduledEvents as deleted. """
    # TODO: We are currently blindly deleting after sending, this will be changed after the app is
    #  updated to provide uuid checkins on the download surveys endpoint. (maybe)
    mark_as_deleted = status == MESSAGE_SEND_SUCCESS
    for scheduled_event in schedules:
        scheduled_event.archive(self_delete=mark_as_deleted, status=status, created_on=created_on)


def enqueue_weekly_surveys(participant: Participant, schedules: List[ScheduledEvent]):
    # set_next_weekly is idempotent until the next weekly event passes.
    # its perfectly safe (commit time) to have many of the same weekly survey be scheduled at once.
    for schedule in schedules:
        if schedule.get_schedule_type() == ScheduleTypes.weekly:
            set_next_weekly(participant, schedule.survey)


# can't be factored out easily because it requires the celerytask function object.
# 2024-1-13 - it's not clear anymore if this is required .
celery_send_survey_push_notification.max_retries = 0
celery_heartbeat_send_push_notification.max_retries = 0

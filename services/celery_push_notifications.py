import json
import random
from collections import defaultdict
from datetime import datetime, timedelta
from typing import List

from django.utils import timezone
from firebase_admin.messaging import (AndroidConfig, Message, Notification, QuotaExceededError,
    send as send_notification, ThirdPartyAuthError, UnregisteredError)

from config.constants import API_TIME_FORMAT, PUSH_NOTIFICATION_SEND_QUEUE, ScheduleTypes
from config.settings import PUSH_NOTIFICATION_ATTEMPT_COUNT
from config.study_constants import OBJECT_ID_ALLOWED_CHARS
from database.schedule_models import ArchivedEvent, ScheduledEvent
from database.user_models import Participant, ParticipantFCMHistory, PushNotificationDisabledEvent
from libs.celery_control import push_send_celery_app, safe_apply_async
from libs.firebase_config import check_firebase_instance
from libs.push_notification_helpers import set_next_weekly
from libs.sentry import make_error_sentry, SentryTypes


################################################################E###############
############################# PUSH NOTIFICATIONS ###############################
################################################################################

def get_surveys_and_schedules(now):
    """ Mostly this function exists to reduce namespace clutter. """
    # get: schedule time is in the past for participants that have fcm tokens.
    # need to filter out unregistered fcms, database schema sucks for that, do it in python. its fine.
    query = ScheduledEvent.objects.filter(
        # core
        scheduled_time__lte=now, participant__fcm_tokens__isnull=False,
        # safety
        participant__deleted=False, survey__deleted=False,
        # Shouldn't be necessary, placeholder containing correct lte count.
        # participant__push_notification_unreachable_count__lte=PUSH_NOTIFICATION_ATTEMPT_COUNT
    ).values_list(
        "survey__object_id",
        "participant__fcm_tokens__token",
        "pk",
        "participant__patient_id",
        "participant__fcm_tokens__unregistered",
    )

    # defaultdicts = clean code, convert to dicts at end.
    # we need a mapping of fcm tokens (a proxy for participants) to surveys and schedule ids (pks)
    surveys = defaultdict(list)
    schedules = defaultdict(list)
    patient_ids = {}
    for survey_obj_id, fcm, schedule_id, patient_id, unregistered in query:
        if unregistered:
            continue
        surveys[fcm].append(survey_obj_id)
        schedules[fcm].append(schedule_id)
        patient_ids[fcm] = patient_id

    return dict(surveys), dict(schedules), patient_ids


def create_push_notification_tasks():
    # we reuse the high level strategy from data processing celery tasks, see that documentation.
    expiry = (datetime.utcnow() + timedelta(minutes=5)).replace(second=30, microsecond=0)
    now = timezone.now()
    surveys, schedules, patient_ids = get_surveys_and_schedules(now)
    print("Surveys:", surveys, sep="\n\t")
    print("Schedules:", schedules, sep="\n\t")
    print("Patient_ids:", patient_ids, sep="\n\t")

    with make_error_sentry(sentry_type=SentryTypes.data_processing):
        if not check_firebase_instance():
            print("Firebase is not configured, cannot queue notifications.")
            return

        # surveys and schedules are guaranteed to have the same keys, assembling the data structures
        # is a pain, so it is factored out. sorry, but not sorry. it was a mess.
        for fcm_token in surveys.keys():
            print(f"Queueing up push notification for user {patient_ids[fcm_token]} for {surveys[fcm_token]}")
            safe_apply_async(
                celery_send_push_notification,
                args=[fcm_token, surveys[fcm_token], schedules[fcm_token]],
                max_retries=0,
                expires=expiry,
                task_track_started=True,
                task_publish_retry=False,
                retry=False,
            )


@push_send_celery_app.task(queue=PUSH_NOTIFICATION_SEND_QUEUE)
def celery_send_push_notification(fcm_token: str, survey_obj_ids: List[str], schedule_pks: List[int]):
    ''' Celery task that sends push notifications.   Note that this list of pks may contain duplicates.'''
    patient_id = ParticipantFCMHistory.objects.filter(token=fcm_token) \
        .values_list("participant__patient_id", flat=True).get()

    with make_error_sentry(sentry_type=SentryTypes.data_processing):
        if not check_firebase_instance():
            print("Firebase credentials are not configured.")
            return

        # use the earliest timed schedule as our reference for the sent_time parameter.  (why?)
        participant = Participant.objects.get(patient_id=patient_id)
        schedules = ScheduledEvent.objects.filter(pk__in=schedule_pks)
        reference_schedule = schedules.order_by("scheduled_time").first()
        survey_obj_ids = list(set(survey_obj_ids))  # already deduped; whatever.

        print(f"Sending push notification to {patient_id} for {survey_obj_ids}...")
        try:
            send_push_notification(participant, reference_schedule, survey_obj_ids, fcm_token)
        # error types are documented at firebase.google.com/docs/reference/fcm/rest/v1/ErrorCode
        except UnregisteredError as e:
            # is an internal 404 http response, it means the token used was wrong.
            # mark the fcm history as out of date.
            return

        except QuotaExceededError:
            # limits are very high, this is effectively impossible, but it is possible, so we catch it.
            raise

        except ThirdPartyAuthError as e:
            failed_send_handler(participant, fcm_token, str(e), schedules)
            # This means the credentials used were wrong for the target app instance.  This can occur
            # both with bad server credentials, and with bad device credentials.
            # We have only seen this error statement, error name is generic so there may be others.
            if str(e) != "Auth error from APNS or Web Push Service":
                raise
            return

        except ValueError as e:
            # This case occurs ever? is tested for in check_firebase_instance... weird race condition?
            # Error should be transient, and like all other cases we enqueue the next weekly surveys regardless.
            if "The default Firebase app does not exist" in str(e):
                enqueue_weekly_surveys(participant, schedules)
                return
            else:
                raise

        success_send_handler(participant, fcm_token, schedules)


def send_push_notification(
        participant: Participant, reference_schedule: ScheduledEvent, survey_obj_ids: List[str],
        fcm_token: str
):
    """ Contains the body of the code to send a notification  """
    # we include a nonce in case of notification deduplication.
    data_kwargs = {
        'nonce': ''.join(random.choice(OBJECT_ID_ALLOWED_CHARS) for _ in range(32)),
        'sent_time': reference_schedule.scheduled_time.strftime(API_TIME_FORMAT),
        'type': 'survey',
        'survey_ids': json.dumps(list(set(survey_obj_ids))),  # Dedupe.
    }

    if participant.os_type == Participant.ANDROID_API:
        message = Message(
            android=AndroidConfig(data=data_kwargs, priority='high'), token=fcm_token,
        )
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
    print(f"Push notification send succeeded for {participant.patient_id}.")

    # this condition shouldn't occur.  Leave in, this case would be super stupid to diagnose.
    fcm_hist = ParticipantFCMHistory.objects.get(token=fcm_token)
    if fcm_hist.unregistered is not None:
        fcm_hist.unregistered = None
        fcm_hist.save()

    participant.push_notification_unreachable_count = 0
    participant.save()

    create_archived_events(schedules, success=True, status=ArchivedEvent.SUCCESS)
    enqueue_weekly_surveys(participant, schedules)


def failed_send_handler(
        participant: Participant, fcm_token: str, error_message: str, schedules: List[ScheduledEvent]
):
    """ Contains body of code for unregistering a participants push notification behavior.
        Participants get reenabled when they next touch the app checkin endpoint. """

    if participant.push_notification_unreachable_count >= PUSH_NOTIFICATION_ATTEMPT_COUNT:
        now = timezone.now()
        fcm_hist = ParticipantFCMHistory.objects.get(token=fcm_token)
        fcm_hist.unregistered = now
        fcm_hist.save()

        PushNotificationDisabledEvent(
            participant=participant, timestamp=now, count=participant.push_notification_unreachable_count
        ).save()

        # disable the credential
        participant.push_notification_unreachable_count = 0
        participant.save()

        print(f"Participant {participant.patient_id} has had push notifications "
              f"disabled after {PUSH_NOTIFICATION_ATTEMPT_COUNT} failed attempts to send.")

    else:
        now = None
        participant.push_notification_unreachable_count += 1
        participant.save()
        print(f"Participant {participant.patient_id} has had push notifications failures "
              f"incremented to {participant.push_notification_unreachable_count}.")

    create_archived_events(schedules, success=False, created_on=now, status=error_message)
    enqueue_weekly_surveys(participant, schedules)


def create_archived_events(
        schedules: List[ScheduledEvent], success: bool, status: str, created_on: datetime = None,
    ):
    """ Populates event history, successes will delete source ScheduledEvents. """
    for schedule in schedules:
        schedule.archive(self_delete=success, status=status, created_on=created_on)


def enqueue_weekly_surveys(participant: Participant, schedules: List[ScheduledEvent]):
    # set_next_weekly is idempotent until the next weekly event passes.
    # its perfectly safe (commit time) to have many of the same weekly survey be scheduled at once.
    for schedule in schedules:
        if schedule.get_schedule_type() == ScheduleTypes.weekly:
            set_next_weekly(participant, schedule.survey)


celery_send_push_notification.max_retries = 0  # requires the celerytask function object.

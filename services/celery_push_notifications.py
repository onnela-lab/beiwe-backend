import json
import random
from collections import defaultdict
from datetime import datetime, timedelta
from django.db.models import Q
from django.utils import timezone
from firebase_admin.messaging import (AndroidConfig, Message, Notification, QuotaExceededError,
    send as send_notification, SenderIdMismatchError, ThirdPartyAuthError, UnregisteredError)
from typing import Dict, List

from config.settings import BLOCK_QUOTA_EXCEEDED_ERROR, PUSH_NOTIFICATION_ATTEMPT_COUNT
from constants.celery_constants import PUSH_NOTIFICATION_SEND_QUEUE, ScheduleTypes
from constants.common_constants import API_TIME_FORMAT
from constants.participant_constants import ANDROID_API
from constants.security_constants import OBJECT_ID_ALLOWED_CHARS
from database.schedule_models import ArchivedEvent, ParticipantMessage, ParticipantMessageStatus, ParticipantMessageScheduleType, ScheduledEvent
from database.user_models import Participant, ParticipantFCMHistory, PushNotificationDisabledEvent
from libs.celery_control import push_send_celery_app, safe_apply_async
from libs.firebase_config import check_firebase_instance
from libs.push_notification_helpers import set_next_weekly
from libs.sentry import make_error_sentry, SentryTypes


def create_push_notification_tasks():
    # we reuse the high level strategy from data processing celery tasks, see that documentation.
    now = timezone.now()
    with make_error_sentry(sentry_type=SentryTypes.data_processing):
        if not check_firebase_instance():
            print("Firebase is not configured, cannot queue notifications.")
            return
        queue_survey_tasks(now)
        queue_message_tasks(now)


def queue_message_tasks(now):
    asap_filter = Q(schedule_type=ParticipantMessageScheduleType.asap)
    absolute_filter = (
        Q(schedule_type=ParticipantMessageScheduleType.absolute) &
        Q(scheduled_send_datetime__lte=now)
    )
    participant_message_ids = ParticipantMessage.objects.filter(
        asap_filter | absolute_filter,
        status=ParticipantMessageStatus.scheduled,
    ).values_list("id", flat=True)
    for participant_message_id in participant_message_ids:
        queue_celery_task(
            celery_send_message_push_notification,
            args=[participant_message_id]
        )


def queue_survey_tasks(now):
    # get: schedule time is in the past for participants that have fcm tokens.
    query = ScheduledEvent.objects.filter(
        # core
        participant__fcm_tokens__isnull=False,
        participant__fcm_tokens__unregistered=None,  # TODO: should this be here?
        scheduled_time__lte=now,
        scheduled_time__gte=now - timedelta(weeks=7),  # If it's older than 1 week, don't send it
        # safety
        participant__deleted=False,
        survey__deleted=False,
    ).values_list(
        "participant_id",
        "id",
    )

    participants_and_scheduled_events = defaultdict(list)
    for participant_id, schedule_id in query:
        participants_and_scheduled_events[participant_id].append(schedule_id)

    for participant_id, schedule_ids in participants_and_scheduled_events.items():
        print(
            f"Queuing up survey push notification for participant {participant_id} for schedules "
            f"{schedule_ids}"
        )
        queue_celery_task(
            celery_send_survey_push_notification,
            args=[participant_id, schedule_ids]
        )


def queue_celery_task(func, *args, **kwargs):
    default_kwargs = {
        "max_retries": 0,
        "expires": (datetime.utcnow() + timedelta(minutes=5)).replace(second=30, microsecond=0),
        "task_track_started": True,
        "task_publish_retry": False,
        "retry": False,
    }
    combined_kwargs = {**default_kwargs, **kwargs}
    return safe_apply_async(func, *args, **combined_kwargs)


@push_send_celery_app.task(queue=PUSH_NOTIFICATION_SEND_QUEUE)
def celery_send_message_push_notification(participant_message_id: int):
    print("Here's this")
    with make_error_sentry(sentry_type=SentryTypes.data_processing):
        participant_message = ParticipantMessage.objects.get(pk=participant_message_id)
        data_kwargs = {
            'message': participant_message.message,
            'type': 'message',
        }
        send_push_notification(
            participant_message.participant,
            data_kwargs,
            display_message="You have a new message."
        )
        participant_message.record_successful_send()


@push_send_celery_app.task(queue=PUSH_NOTIFICATION_SEND_QUEUE)
def celery_send_survey_push_notification(participant_id: int, schedule_pks: List[int]):
    participant = Participant.objects.get(pk=participant_id)
    schedules = participant.scheduled_events.filter(id__in=schedule_pks).prefetch_related('survey')
    patient_id = participant.patient_id  # patient_id helps with debugging
    reference_schedule = schedules.order_by("scheduled_time").first()
    survey_obj_ids = list(schedules.values_list('survey__object_id', flat=True).distinct())

    # we include a nonce in case of notification deduplication.
    data_kwargs = {
        'nonce': ''.join(random.choice(OBJECT_ID_ALLOWED_CHARS) for _ in range(32)),
        'sent_time': reference_schedule.scheduled_time.strftime(API_TIME_FORMAT),
        'type': 'survey',
        'survey_ids': json.dumps(list(set(survey_obj_ids))),  # Dedupe.
    }
    display_message = "You have a survey to take." if len(survey_obj_ids) == 1 else "You have surveys to take."
    print(f"Sending push notification to {patient_id} for {survey_obj_ids}...")
    send_push_notification(participant, data_kwargs, display_message, schedule_pks)


def send_push_notification(
        participant: Participant,
        notification_data: Dict,
        display_message: str,
        schedule_pks: List[int] = None,
):
    ''' Celery task that sends push notifications. Note that this list of pks may contain duplicates.'''
    with make_error_sentry(sentry_type=SentryTypes.data_processing):
        if not check_firebase_instance():
            print("Firebase credentials are not configured.")
            return
        
        # use the earliest timed schedule as our reference for the sent_time parameter.  (why?)
        fcm_token = participant.get_valid_fcm_token().token
        if schedule_pks is not None:
            schedules = participant.scheduled_events.filter(id__in=schedule_pks).prefetch_related('survey')

        try:
            if participant.os_type == ANDROID_API:
                message = Message(
                    android=AndroidConfig(data=notification_data, priority='high'), token=fcm_token,
                )
            else:
                display_message = display_message
                message = Message(
                    data=notification_data,
                    token=fcm_token,
                    notification=Notification(title="Beiwe", body=display_message),
                )
            send_notification(message)
        # error types are documented at firebase.google.com/docs/reference/fcm/rest/v1/ErrorCode
        except UnregisteredError:
            print("\nUnregisteredError\n")
            # Is an internal 404 http response, it means the token that was used has been disabled.
            # Mark the fcm history as out of date, return early.
            ParticipantFCMHistory.objects.filter(token=fcm_token).update(unregistered=timezone.now())
            return
        
        except QuotaExceededError as e:
            # Limits are very high, this should be impossible. Reraise because this requires
            # sysadmin attention and probably new development to allow multiple firebase
            # credentials. Read comments in settings.py if toggling.
            if BLOCK_QUOTA_EXCEEDED_ERROR:
                if schedule_pks:
                    failed_send_handler(participant, fcm_token, str(e), schedules)
                return
            else:
                raise
        
        except ThirdPartyAuthError as e:
            print("\nThirdPartyAuthError\n")
            if schedule_pks:
                failed_send_handler(participant, fcm_token, str(e), schedules)
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
            print("\nSenderIdMismatchError:\n")
            print(e)
            if schedule_pks:
                failed_send_handler(participant, fcm_token, str(e), schedules)
            return
        
        except ValueError as e:
            print("\nValueError\n")
            print(e)
            # This case occurs ever? is tested for in check_firebase_instance... weird race condition?
            # Error should be transient, and like all other cases we enqueue the next weekly surveys regardless.
            if "The default Firebase app does not exist" in str(e):
                if schedule_pks:
                    enqueue_weekly_surveys(participant, schedules)
                return
            else:
                raise
        
        except Exception as e:
            if schedule_pks:
                failed_send_handler(participant, fcm_token, str(e), schedules)
            return
        if schedule_pks:
            success_send_handler(participant, fcm_token, schedules)


def success_send_handler(participant: Participant, fcm_token: str, schedules: List[ScheduledEvent]):
    # If the query was successful archive the schedules.  Clear the fcm unregistered flag
    # if it was set (this shouldn't happen. ever. but in case we hook in a ui element we need it.)
    print(f"Push notification send succeeded for {participant.patient_id}.")
    
    # this condition shouldn't occur.  Leave in, this case would be super stupid to diagnose.
    fcm_hist: ParticipantFCMHistory = ParticipantFCMHistory.objects.get(token=fcm_token)
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
        fcm_hist: ParticipantFCMHistory = ParticipantFCMHistory.objects.get(token=fcm_token)
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


celery_send_survey_push_notification.max_retries = 0  # requires the celerytask function object.
celery_send_message_push_notification.max_retries = 0  # requires the celerytask function object.

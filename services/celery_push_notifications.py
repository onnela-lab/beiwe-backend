import random
from os.path import abspath
from sys import path
path.insert(0, abspath(__file__).rsplit('/', 2)[0])

# add the root of the project into the path to allow cd-ing into this folder and running the script.
from config.study_constants import OBJECT_ID_ALLOWED_CHARS
from libs.sentry import make_error_sentry


import json
from collections import defaultdict
from datetime import datetime, timedelta
from typing import List

import pytz
from django.utils import timezone
from django.utils.timezone import make_aware
from firebase_admin.messaging import (AndroidConfig, AndroidNotification, Message, Notification,
                                      QuotaExceededError, send, ThirdPartyAuthError,
                                      UnregisteredError)
from kombu.exceptions import OperationalError

from config.constants import API_TIME_FORMAT, PUSH_NOTIFICATION_SEND_QUEUE, ScheduleTypes
from database.schedule_models import ScheduledEvent
from database.user_models import Participant, ParticipantFCMHistory
from libs.celery_control import push_send_celery_app
from libs.push_notifications import (check_firebase_instance, FirebaseMisconfigured, set_next_weekly)


################################################################E###############
############################# PUSH NOTIFICATIONS ###############################
################################################################################

def get_surveys_and_schedules(now):
    """ Mostly this function exists to reduce namespace clutter. """
    # schedule time is in the past, with participants that have fcm tokens.
    # need to filter out unregistered fcms, database schema sucks for that, do it in python. its fine.
    query = ScheduledEvent.objects.filter(
        scheduled_time__lte=now, participant__fcm_tokens__isnull=False,
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
    now = make_aware(datetime.utcnow(), timezone=pytz.utc)
    surveys, schedules, patient_ids = get_surveys_and_schedules(now)
    print(surveys)
    print(schedules)
    print(patient_ids)
    with make_error_sentry('data'):
        if not check_firebase_instance():
            raise FirebaseMisconfigured("Firebase is not configured, cannot queue notifications.")

        # surveys and schedules are guaranteed to have the same keys, assembling the data structures
        # is a pain, so it is factored out. sorry, but not sorry. it was a mess.
        for fcm_token in surveys.keys():
            print(
                f"Queueing up push notification for user {patient_ids[fcm_token]} for {surveys[fcm_token]}")
            safe_queue_push(
                args=[fcm_token, surveys[fcm_token], schedules[fcm_token]],
                max_retries=0,
                expires=expiry,
                task_track_started=True,
                task_publish_retry=False,
                retry=False,
            )


@push_send_celery_app.task(queue=PUSH_NOTIFICATION_SEND_QUEUE)
def celery_send_push_notification(fcm_token: str, survey_obj_ids: List[str],
                                  schedule_pks: List[int]):
    ''' Celery task that sends push notifications.   Note that this list of pks may contain duplicates.'''
    success = False
    patient_id = ParticipantFCMHistory.objects.filter(token=fcm_token).values_list(
        "participant__patient_id", flat=True).get()

    with make_error_sentry("data"):
        if not check_firebase_instance():
            raise FirebaseMisconfigured(
                "You have not provided credentials for Firebase, notifications cannot be sent."
            )

        # use the earliest timed schedule as our reference for the sent_time parameter.  (why?)
        schedules = ScheduledEvent.objects.filter(pk__in=schedule_pks)
        reference_schedule = schedules.order_by("scheduled_time").first()
        survey_obj_ids = list(set(survey_obj_ids))

        # There is debugging code that looks for this variable, don't delete it...
        print(f"Sending push notification to {patient_id} for {survey_obj_ids}.")
        try:
            if Participant.objects.get(patient_id=patient_id).os_type == Participant.ANDROID_API:
                message = Message(
                    android=AndroidConfig(
                        data={
                            'type': 'survey',
                            'survey_ids': json.dumps(list(set(survey_obj_ids))),  # Dedupe.
                            'sent_time': reference_schedule.scheduled_time.strftime(API_TIME_FORMAT),
                            'nonce': ''.join(random.choice(OBJECT_ID_ALLOWED_CHARS) for _ in range(32))
                        },
                        priority='high',
                    ),
                    token=fcm_token,
                )
            else:
                message = Message(
                    data={
                        'type': 'survey',
                        'survey_ids': json.dumps(list(set(survey_obj_ids))),  # Dedupe.
                        'sent_time': reference_schedule.scheduled_time.strftime(API_TIME_FORMAT),
                        'nonce': ''.join(random.choice(OBJECT_ID_ALLOWED_CHARS) for _ in range(32))
                    },
                    notification=AndroidNotification(
                        title="Beiwe",
                        body=
                        "You have a survey to take." if len(survey_obj_ids) == 1 else
                        "You have surveys to take.",
                    ),
                    token=fcm_token,
                )
            response = send(message)
            #
            # response = send(Message(
            #     # data={
            #     #     'type': 'survey',
            #     #     'survey_ids': json.dumps(list(set(survey_obj_ids))),  # Dedupe.
            #     #     'sent_time': reference_schedule.scheduled_time.strftime(API_TIME_FORMAT),
            #     #     'nonce': ''.join(random.choice(OBJECT_ID_ALLOWED_CHARS) for _ in range(32))
            #     # },
            #     token=fcm_token,
            # ))
            success = True
        except UnregisteredError:
            # mark the fcm history as out of date.
            fcm_hist = ParticipantFCMHistory.objects.get(token=fcm_token)
            if fcm_hist.unregistered is None:
                fcm_hist.unregistered = timezone.now()
                fcm_hist.save()

        except QuotaExceededError:
            # limits are very high, this is effectively impossible, but it is possible.
            raise

        except ThirdPartyAuthError:
            # occurs when the platform (Android or iOS) is not configured appropriately.
            raise Exception(
                "There is a misconfiguration in your firebase push notification setup.  "
                "Please see Beiwe's documentation for setup of FCM push notifications.  "
                "If the configuration is correct but you continue to see this error "
                "please post a bug report."
                "\n issues: https://github.com/onnela-lab/beiwe-backend/issues"
                "\n documentation: https://firebase.google.com/docs/admin/setup#initialize-sdk"
            )

        # DEBUG uncomment to print
        # from pprint import pprint
        # if err_sentry.errors:
        #     err_sentry.raise_errors()

        # if "response" in vars():
        #     print(response)
        #     pprint(vars(response))

    # NOTE: code has exited the ErrorHandler.
    # If the query was successful archive the schedules.  Clear the fcm unregistered flag
    # if it was set (this shouldn't happen. ever. but in case we hook in a ui element we need it.)
    if success:
        print("Push notification send succeeded")
        fcm_hist = ParticipantFCMHistory.objects.get(token=fcm_token)
        if fcm_hist.unregistered is not None:
            fcm_hist.unregistered = None
            fcm_hist.save()

        for schedule in schedules:
            schedule.archive()
            if schedule.get_schedule_type() == ScheduleTypes.weekly:
                set_next_weekly(fcm_hist.participant, schedule.survey)
    else:
        print("Push notification send failed")


celery_send_push_notification.max_retries = 0


def safe_queue_push(*args, **kwargs):
    for i in range(10):
        try:
            return celery_send_push_notification.apply_async(*args, **kwargs)
        except OperationalError:
            if i < 3:
                pass
            else:
                raise

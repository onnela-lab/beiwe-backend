from os.path import abspath
from sys import path
# add the root of the project into the path to allow cd-ing into this folder and running the script.

path.insert(0, abspath(__file__).rsplit('/', 2)[0])

import json
from collections import defaultdict
from datetime import datetime, timedelta
from typing import List

import pytz
from django.utils import timezone
from django.utils.timezone import make_aware
from firebase_admin.messaging import Message, QuotaExceededError, send, UnregisteredError
from kombu.exceptions import OperationalError

from config.constants import API_TIME_FORMAT, PUSH_NOTIFICATION_SEND_QUEUE, ScheduleTypes
from database.schedule_models import ScheduledEvent
from database.user_models import ParticipantFCMHistory
from libs.celery_control import push_send_celery_app
from libs.push_notifications import (firebase_app, FirebaseNotCredentialed, set_next_weekly)
from libs.sentry import make_error_sentry


################################################################################
############################# Data Processing ##################################
################################################################################

def get_surveys_and_schedules(now):
    """ Mostly this function exists to reduce namespace clutter. """
    # schedule time is in the past, with participants that have fcm tokens.
    query = ScheduledEvent.objects.filter(
            scheduled_time__lte=now, participant__fcm_tokens__isnull=False
        ).values_list(
            "survey__object_id", "participant__fcm_tokens__token", "pk"
        )

    # defaultdicts = clean code, convert to dicts at end.
    # we need a mapping of fcm tokens (a proxy for participants) to surveys and schedule ids (pks)
    surveys = defaultdict(list)
    schedules = defaultdict(list)
    for survey_obj_id, fcm, schedule_id in query:
        surveys[fcm].append(survey_obj_id)
        schedules[fcm].append(schedule_id)

    return dict(surveys), dict(schedules)


def create_push_notification_tasks():
    # we reuse the high level strategy from data processing celery tasks, see that documentation.
    expiry = (datetime.now() + timedelta(minutes=5)).replace(second=30, microsecond=0)
    now = make_aware(datetime.utcnow(), timezone=pytz.utc)
    surveys, schedules = get_surveys_and_schedules(now)

    with make_error_sentry('data'):
        if not firebase_app:
            raise FirebaseNotCredentialed("Firebase is not configured, cannot queue notifications.")

        # surveys and schedules are guaranteed to have the same keys, assembling the data structures
        # is a pain, so it is factored out. sorry, but not sorry. it was a mess.
        for fcm_token in surveys.keys():
            safe_queue_push(
                args=[fcm_token, surveys[fcm_token], schedules[fcm_token]],
                max_retries=0,
                expires=expiry,
                task_track_started=True,
                task_publish_retry=False,
                retry=False,
            )


@push_send_celery_app.task(queue=PUSH_NOTIFICATION_SEND_QUEUE)
def celery_send_push_notification(fcm_token: str, survey_obj_ids: List[str], schedule_pks: List[int]):
    ''' Celery task that sends push notifications. '''

    success = False
    with make_error_sentry("data"):
        if not firebase_app:
            raise FirebaseNotCredentialed(
                "You have not provided credentials for Firebase, notifications cannot be sent."
            )

        # use the earliest timed schedule as our reference for the sent_time parameter.  (why?)
        schedules = ScheduledEvent.objects.filter(pk__in=schedule_pks)
        reference_schedule = schedules.order_by("scheduled_time").first()

        try:
            # There is debugging code that looks for this variable, don't delete it...
            response = send(Message(
                data={
                    'type': 'survey',
                    'survey_ids': json.dumps(survey_obj_ids),
                    'sent_time': reference_schedule.scheduled_time.strftime(API_TIME_FORMAT),
                },
                token=fcm_token,
            ))
            success = True
        except UnregisteredError:
            # mark the fcm history as out of date.
            fcm_hist = ParticipantFCMHistory.objects.get(fcm_token=fcm_token)
            if fcm_hist.unregistered is None:
                fcm_hist.unregistered = timezone.now()
                fcm_hist.save()

        except QuotaExceededError:
            # limits are very high, this is effectively impossible, but it is possible.
            raise

        # DEBUG uncomment to print
        # from pprint import pprint
        # if err_sentry.errors:
        #     err_sentry.raise_errors()
        #
        # if "response" in vars():
        #     print(response)
        #     pprint(vars(response))

    # NOTE: code has exited the ErrorHandler.
    # If the query was successful archive the schedules.  Clear the fcm unregistered flag
    # if it was set (this shouldn't happen. ever. but in case we hook in a ui element we need it.)
    if success:
        fcm_hist = ParticipantFCMHistory.objects.get(token=fcm_token)
        if fcm_hist.unregistered is not None:
            fcm_hist.unregistered = None
            fcm_hist.save()

        for schedule in schedules:
            schedule.archive()
            if schedule.get_schedule_type() == ScheduleTypes.weekly:
                set_next_weekly(fcm_hist.participant, schedule.survey)


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


# Running this file will enqueue users
if __name__ == "__main__":
    create_push_notification_tasks()

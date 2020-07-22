from datetime import datetime, time
from os.path import exists

import pytz
from django.utils.timezone import make_aware
from firebase_admin import credentials, initialize_app as initialize_firebase_app

from config.constants import FIREBASE_CREDENTIAL_LOCATION
from database.schedule_models import ScheduledEvent, WeeklySchedule
from database.survey_models import Survey
from database.user_models import Participant

# setup firebase
if exists(FIREBASE_CREDENTIAL_LOCATION):
    firebase_app = initialize_firebase_app(credentials.Certificate(FIREBASE_CREDENTIAL_LOCATION))
else:
    firebase_app = None


class FirebaseNotCredentialed(Exception): pass
class NoSchedulesException(Exception): pass


def set_next_weekly(participant: Participant, survey: Survey) -> None:
    ''' Create a next ScheduledEvent for a survey for a particular participant. '''
    schedule_date, schedule = get_next_weekly_event(survey)

    # this handles the case where the schedule was deleted. This is a corner case that shouldn't happen
    if schedule_date is not None and schedule is not None:
        ScheduledEvent.objects.create(
            survey=survey,
            participant=participant,
            weekly_schedule=schedule,
            relative_schedule=None,
            absolute_schedule=None,
            scheduled_time=schedule_date,
        )


def repopulate_weekly_survey_schedule_events(survey: Survey) -> None:
    """ Clear existing schedules, get participants, bulk create schedules """
    survey.scheduled_events.filter(relative_schedule=None, absolute_schedule=None).delete()

    try:
        schedule_date, schedule = get_next_weekly_event(survey)
    except NoSchedulesException:
        return

    participants = survey.study.participants.all()
    new_events = []
    for participant in participants:
        new_events.append(ScheduledEvent(
            survey=survey,
            participant=participant,
            weekly_schedule=schedule,
            relative_schedule=None,
            absolute_schedule=None,
            scheduled_time=schedule_date,
        ))

    ScheduledEvent.objects.bulk_create(new_events)


def repopulate_absolute_survey_schedule_events(survey: Survey) -> None:
    """
    Creates new ScheduledEvents for the survey's AbsoluteSchedules while deleting the old
    ScheduledEvents related to the survey
    """
    # if the event is from an absolute schedule, relative and weekly schedules will be None
    survey.scheduled_events.filter(relative_schedule=None, weekly_schedule=None).delete()
    new_events = []
    for schedule in survey.absolute_schedules.all():
        for participant in survey.study.participants.all():
            new_events.append(ScheduledEvent(
                survey=survey,
                participant=participant,
                weekly_schedule=None,
                relative_schedule=None,
                absolute_schedule=schedule,
                scheduled_time=schedule.scheduled_date,
            ))

    ScheduledEvent.objects.bulk_create(new_events)


def repopulate_relative_survey_schedule_events(survey: Survey) -> None:
    """
    Creates new ScheduledEvents for the survey's RelativeSchedules while deleting the old
    ScheduledEvents related to the survey
    """
    # if the event is from an relative schedule, absolute and weekly schedules will be None
    survey.scheduled_events.filter(absolute_schedule=None, weekly_schedule=None).delete()
    new_events = []

    for schedule in survey.relative_schedules.all():
        for participant in survey.study.participants.all():
            intervention_date = schedule.intervention.intervention_dates.get(participant=participant).date
            if intervention_date:
                new_events.append(ScheduledEvent(
                    survey=survey,
                    participant=participant,
                    weekly_schedule=None,
                    relative_schedule=schedule,
                    absolute_schedule=None,
                    scheduled_time=datetime.combine(intervention_date, time(schedule.hour, schedule.minute)),
                ))

    ScheduledEvent.objects.bulk_create(new_events)


def get_next_weekly_event(survey: Survey) -> (datetime, WeeklySchedule):
    """ Determines the next time for a particular survey, provides the relevant weekly schedule. """
    now = make_aware(datetime.utcnow(), timezone=pytz.utc)
    timing_list = []
    for weekly_schedule in survey.weekly_schedules.all():
        this_week, next_week = weekly_schedule.get_prior_and_next_event_times(now)
        timing_list.append((this_week if now < this_week else next_week, weekly_schedule))

    # handle case where there are no scheduled events
    if not timing_list:
        raise NoSchedulesException

    timing_list.sort(key=lambda date_and_schedule: date_and_schedule[0])
    schedule_date, schedule = timing_list[0]
    return schedule_date, schedule

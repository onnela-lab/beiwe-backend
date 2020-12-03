from datetime import datetime

import pytz
from django.utils.timezone import is_aware, is_naive, make_aware
from firebase_admin import credentials, initialize_app as initialize_firebase_app

from config.settings import PUSH_NOTIFICATIONS_ENABLED
from database.schedule_models import (AbsoluteSchedule, ArchivedEvent, ScheduledEvent,
    WeeklySchedule)
from database.survey_models import Survey
from database.user_models import Participant

# setup firebase
if PUSH_NOTIFICATIONS_ENABLED:
    # todo: new push notification credential details and populate arguments to certificate
    firebase_app = initialize_firebase_app(credentials.Certificate(None))
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


def repopulate_weekly_survey_schedule_events(survey: Survey, participant: Participant = None) -> None:
    """ Clear existing schedules, get participants, bulk create schedules Weekly events are
    calculated in a way that we don't bother checking for survey archives, because they only
    exist in the future. """
    event_filters = dict(relative_schedule=None, absolute_schedule=None)

    if participant is not None:
        survey.scheduled_events.filter(**event_filters, participant=participant).delete()
        participant_ids = [participant.pk]
    else:
        participant_ids = survey.study.participants.values_list("pk", flat=True)
        survey.scheduled_events.filter(**event_filters).delete()

    try:
        # forces tz-aware schedule_date
        schedule_date, schedule = get_next_weekly_event(survey)
    except NoSchedulesException:
        return

    ScheduledEvent.objects.bulk_create(
        [
            ScheduledEvent(
                survey=survey,
                participant_id=participant_id,
                weekly_schedule=schedule,
                relative_schedule=None,
                absolute_schedule=None,
                scheduled_time=schedule_date,
            ) for participant_id in participant_ids
        ]
    )


def repopulate_absolute_survey_schedule_events(survey: Survey, participant: Participant = None) -> None:
    """
    Creates new ScheduledEvents for the survey's AbsoluteSchedules while deleting the old
    ScheduledEvents related to the survey
    """
    # if the event is from an absolute schedule, relative and weekly schedules will be None
    event_filters = dict(relative_schedule=None, weekly_schedule=None)

    if participant is not None:
        event_filters['participant'] = participant

    survey.scheduled_events.filter(**event_filters).delete()

    new_events = []
    for schedule_pk, scheduled_time in survey.absolute_schedules.values_list("pk", "scheduled_date"):
        # if the schedule is somehow not tz-aware, force update it.
        if is_naive(scheduled_time):
            scheduled_time = make_aware(scheduled_time, survey.study.timezone)
            AbsoluteSchedule.objects.filter(pk=schedule_pk).update(scheduled_time=scheduled_time)

        event_params = dict(
            survey=survey,
            weekly_schedule=None,
            relative_schedule=None,
            absolute_schedule_id=schedule_pk,
            scheduled_time=scheduled_time,
        )

        # if one participant
        if participant is not None:
            if not ArchivedEvent.objects.filter(
                    survey_archive__survey=survey,
                    scheduled_time=scheduled_time,
                    participant_id=participant.pk
            ).exists():
                event_params["participant_id"] = participant.pk
                new_events.append(ScheduledEvent(**event_params))
            continue

        # don't create events for already sent notifications
        irrelevant_participants = ArchivedEvent.objects.filter(
            survey_archive__survey=survey, scheduled_time=scheduled_time,
        ).values_list("participant_id", flat=True)
        relevant_participants = survey.study.participants.exclude(
            pk__in=irrelevant_participants
        ).values_list("pk", flat=True)

        # all participants
        for participant_id in relevant_participants:
            event_params["participant_id"] = participant_id
            new_events.append(ScheduledEvent(**event_params))

    ScheduledEvent.objects.bulk_create(new_events)


def repopulate_relative_survey_schedule_events(survey: Survey, participant: Participant = None) -> None:
    """ Creates new ScheduledEvents for the survey's RelativeSchedules while deleting the old
    ScheduledEvents related to the survey. """

    # if the event is from an relative schedule, absolute and weekly schedules will be None
    event_filters = dict(absolute_schedule=None, weekly_schedule=None)

    if participant is not None:
        event_filters["participant"] = participant
    survey.scheduled_events.filter(**event_filters).delete()

    # This is per schedule, and a participant can't have more than one intervention date per
    # intervention per schedule.  It is also per survey and all we really care about is
    # whether an event ever triggered on that survey.
    new_events = []
    for relative_schedule in survey.relative_schedules.all():

        # Get participants with intervention dates, handle single and multiple users
        if participant is None:
            participant_intervention_dates = relative_schedule.intervention.intervention_dates \
                .exclude(date=None).values_list("participant_id", "date")
        else:
            participant_intervention_dates = relative_schedule.intervention.intervention_dates \
                .exclude(date=None).filter(participant=participant).values_list("participant_id", "date")

        # optimizations: should be able to pull out that .exists() call into a white or black list.
        for participant_id, d in participant_intervention_dates:
            schedule_time = relative_schedule.scheduled_time(d)

            # skip if there is an archived event matching participant
            if ArchivedEvent.objects.filter(
                participant_id=participant_id,
                survey_archive__survey_id=survey.id,
                scheduled_time=schedule_time,
            ).exists():
                continue

            new_events.append(ScheduledEvent(
                survey=survey,
                participant_id=participant_id,
                weekly_schedule=None,
                relative_schedule=relative_schedule,
                absolute_schedule=None,
                scheduled_time=schedule_time,
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
    if not is_aware(schedule_date):
        schedule_date = make_aware(schedule_date, survey.study.timezone)
    return schedule_date, schedule

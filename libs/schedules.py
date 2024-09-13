from collections import defaultdict
from datetime import date, datetime, timedelta
from typing import List, Optional, Tuple

from dateutil.tz import gettz
from django.db.models import Q

from constants.schedule_constants import EMPTY_WEEKLY_SURVEY_TIMINGS, ScheduleTypes
from database.schedule_models import (AbsoluteSchedule, ArchivedEvent, RelativeSchedule,
    ScheduledEvent, WeeklySchedule)
from database.study_models import Study
from database.survey_models import Survey
from database.user_models_participant import Participant
from libs.utils.date_utils import date_to_end_of_day, date_to_start_of_day


class NoSchedulesException(Exception): pass
class UnknownScheduleScenario(Exception): pass


ENABLE_SCHEDULE_LOGGING = False

def log(*args, **kwargs):
    if ENABLE_SCHEDULE_LOGGING:
        print(*args, **kwargs)


# Weekly timings lists code
def get_start_and_end_of_java_timings_week(now: datetime) -> Tuple[datetime, datetime]:
    """ study timezone aware week start and end """
    if now.tzinfo is None:
        raise TypeError("missing required timezone-aware datetime")
    now_date: date = now.date()
    date_sunday_start_of_week = now_date - timedelta(days=now.weekday() + 1)
    date_saturday_end_of_week = now_date + timedelta(days=5 - now.weekday())  # TODO: Test
    dt_sunday_start_of_week = date_to_start_of_day(date_sunday_start_of_week, now.tzinfo)
    dt_saturday_end_of_week = date_to_end_of_day(date_saturday_end_of_week, now.tzinfo)
    return dt_sunday_start_of_week, dt_saturday_end_of_week


def decompose_datetime_to_timings(dt: datetime) -> Tuple[int, int]:
    """ returns day-index, seconds into day. """
    # have to convert to sunday-zero-indexed
    return (dt.weekday() + 1) % 7, dt.hour * 60 * 60 + dt.minute * 60

#
## Determining exclusion criteria is just complex, I've found no great way to factor it.
#

DELETED_TRUE = Q(deleted=True)
PERMANENTLY_RETIRED_TRUE = Q(permanently_retired=True)
RELATED_DELETED_TRUE = Q(participant__deleted=True)
RELATED_PERMANENTLY_RETIRED_TRUE = Q(participant__permanently_retired=True)


def participant_allowed_surveys(participant: Participant) -> bool:
    """ Returns whether we should bother to send a participant survey push notifications. """
    PUSH_NOTIFICATION_EXCLUSION = [
        ("deleted", True),
        ("permanently_retired", True),
    ]
    for field_name, truthiness_value in PUSH_NOTIFICATION_EXCLUSION:
        if getattr(participant, field_name) == truthiness_value:
            return False
    return True

#
## Event scheduling
#

def set_next_weekly(participant: Participant, survey: Survey) -> Tuple[ScheduledEvent, int]:
    """ Create a next ScheduledEvent for a survey for a particular participant. Uses get_or_create. """
    schedule_date, schedule = get_next_weekly_event_and_schedule(survey)
    
    # this handles the case where the schedule was deleted. This is a corner case that shouldn't happen
    if schedule_date is not None and schedule is not None:
        # Return so we can write tests easier, its fine
        return ScheduledEvent.objects.get_or_create(
            survey=survey,
            participant=participant,
            weekly_schedule=schedule,
            relative_schedule=None,
            absolute_schedule=None,
            scheduled_time=schedule_date,
        )
    else:
        raise UnknownScheduleScenario(
            f"unknown condition reached. schedule_date was {schedule_date}, schedule was {schedule}"
        )


def repopulate_all_survey_scheduled_events(study: Study, participant: Participant = None):
    """ Runs all the survey scheduled event generations on the provided entities. """
    log("repopulate_all_survey_scheduled_events")
    
    duplicate_schedule_events_merged = False
    for survey in study.surveys.all():
        # remove any scheduled events on surveys that have been deleted.
        if survey.deleted or study.deleted or study.manually_stopped or study.end_date_is_in_the_past:
            survey.scheduled_events.all().delete()
            continue
        
        # log(f"repopulating all for survey {survey.id}")
        if repopulate_weekly_survey_schedule_events(survey, participant):
            duplicate_schedule_events_merged = True
        if repopulate_absolute_survey_schedule_events(survey, participant):
            duplicate_schedule_events_merged = True
        # there are some cases where we can logically exclude relative surveys.
        # Don't. Do. That. Just. Run. Everything. Always.
        if repopulate_relative_survey_schedule_events(survey, participant):
            duplicate_schedule_events_merged = True
    
    return duplicate_schedule_events_merged


#TODO: this will need to be rewritten to examine existing weekly schedules
def repopulate_weekly_survey_schedule_events(survey: Survey, single_participant: Optional[Participant] = None) -> None:
    """ Clear existing schedules, get participants, bulk create schedules Weekly events are
    calculated in a way that we don't bother checking for survey archives, because they only exist
    in the future. """
    log("weekly schedule events")
    events = survey.scheduled_events.filter(relative_schedule=None, absolute_schedule=None)
    if single_participant:
        events = events.filter(participant=single_participant)
        participant_ids = [single_participant.pk]
    else:
        participant_ids = survey.study.participants.exclude(DELETED_TRUE | PERMANENTLY_RETIRED_TRUE).values_list("pk", flat=True)
    
    events.delete()
    if single_participant and not participant_allowed_surveys(single_participant):
        log("weekly bad participant")
        return
    
    try:
        # get_next_weekly_event forces tz-aware schedule_date datetime object
        schedule_date, schedule = get_next_weekly_event_and_schedule(survey)
    except NoSchedulesException:
        log("weekly no schedules configured")
        return
    
    info = ScheduledEvent.objects.bulk_create(
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
    log(f"weekly schedule events created {info}")
    return bool(len(info))


#TODO: this will need to be rewritten to examine existing absolute schedules
def repopulate_absolute_survey_schedule_events(
        survey: Survey, single_participant: Optional[Participant] = None) -> None:
    """ Creates new ScheduledEvents for the survey's AbsoluteSchedules while deleting the old
    ScheduledEvents related to the survey """
    log("absolute schedule events")
    # if the event is from an absolute schedule, relative and weekly schedules will be None
    events = survey.scheduled_events.filter(relative_schedule=None, weekly_schedule=None)
    if single_participant:
        events = events.filter(participant=single_participant)
    events.delete()
    
    if single_participant and not participant_allowed_surveys(single_participant):
        log("absolute bad participant")
        return
    
    new_events = []
    abs_sched: AbsoluteSchedule
    # for each absolute schedule on the survey create a new scheduled event for each participant.
    for abs_sched in survey.absolute_schedules.all():
        scheduled_time = abs_sched.event_time
        # if one participant
        if single_participant:
            archive_exists = ArchivedEvent.objects.filter(
                survey_archive__survey=survey,
                scheduled_time=scheduled_time,
                participant_id=single_participant.pk
            ).exists()
            relevant_participants = [] if archive_exists else [single_participant.pk]
        
        # if many participants
        else:
            # don't create events for already sent notifications, or deleted participants
            irrelevant_participants = ArchivedEvent.objects.filter(
                survey_archive__survey=survey, scheduled_time=scheduled_time,
            ).values_list(
                "participant_id", flat=True
            )
            
            relevant_participants = survey.study.participants.exclude(
                pk__in=irrelevant_participants
            ).exclude(
                DELETED_TRUE | PERMANENTLY_RETIRED_TRUE
            ).values_list(
                "pk", flat=True
            )
        
        # populate the new events
        for participant_id in relevant_participants:
            new_events.append(ScheduledEvent(
                survey=survey,
                weekly_schedule=None,
                relative_schedule=None,
                absolute_schedule_id=abs_sched.pk,
                scheduled_time=scheduled_time,
                participant_id=participant_id
            ))
    # save to database
    info = ScheduledEvent.objects.bulk_create(new_events)
    log(f"absolute schedule events created {info}")
    return bool(len(info))


def repopulate_relative_survey_schedule_events(
        survey: Survey, single_participant: Optional[Participant] = None) -> None:
    """ Creates new ScheduledEvents for the survey's RelativeSchedules while deleting the old
    ScheduledEvents related to the survey. """
    log("relative schedule events")
    # Clear out existing events.
    events = survey.scheduled_events.filter(absolute_schedule=None, weekly_schedule=None)
    if single_participant:
        events = events.filter(participant=single_participant)
    events.delete()
    
    if single_participant and not participant_allowed_surveys(single_participant):
        log("relative bad participant")
        return
    
    # our query for participants is off of a related field... but as an includes.... (see below)
    # includes = {"participant__" + field: truthiness_value for field, truthiness_value in EXCLUDE.items()}
    
    # This is per schedule, and a participant can't have more than one intervention date per
    # intervention per schedule.  It is also per survey and all we really care about is
    # whether an event ever triggered on that survey.
    new_events = []
    study_timezone = survey.study.timezone  # might as well cache this...
    for relative_schedule in survey.relative_schedules.all():
        # Only interventions that have been marked (have a date), for participants that are not
        # deleted, restrict on the single user case, get data points.
        intervention_dates_query = relative_schedule.intervention.intervention_dates.filter(
            date__isnull=False,
            # Subtle [Django?] behavior that I don't understand: you can't exclude null database values.
            # This query in a .exclude returns instances where date is None, same for `date=None`:
            # intervention_dates_query.exclude(date__isnull=True, ...)
        ).exclude(RELATED_DELETED_TRUE | RELATED_PERMANENTLY_RETIRED_TRUE)
        
        if single_participant:
            intervention_dates_query = intervention_dates_query.filter(participant=single_participant)
        intervention_dates_query = intervention_dates_query.values_list("participant_id", "date")
        
        for participant_id, intervention_date in intervention_dates_query:
            # + below is correct, 'days_after' is negative or 0 for days before and day of.
            # bug: somehow got a Nonetype error even though intervention_date cannot be None... how?
            # "unsupported operand type(s) for +: 'NoneType' and 'datetime.timedelta'"
            # (the order of items in the error statements reflects the code, so intervention_date was None.)
            scheduled_date = intervention_date + timedelta(days=relative_schedule.days_after)
            schedule_time = relative_schedule.notification_time_from_intervention_date_and_timezone(scheduled_date, study_timezone)
            # skip if already sent (archived event matching participant, survey, and schedule time)
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
    
    info = ScheduledEvent.objects.bulk_create(new_events)
    log(f"relative schedule events created {info}")
    return bool(len(info))


def get_next_weekly_event_and_schedule(survey: Survey) -> Tuple[datetime, WeeklySchedule]:
    """ Determines the next time for a particular survey, provides the relevant weekly schedule. """
    now = survey.study.now()
    timings_list = []
    # our possible next weekly event may be this week, or next week; get this week if it hasn't
    # happened, next week if it has.  A survey can have many weekly schedules, grab them all.
    
    for weekly_schedule in survey.weekly_schedules.all():
        this_week, next_week = weekly_schedule.get_prior_and_next_event_times(now)
        timings_list.append((this_week if now < this_week else next_week, weekly_schedule))
    
    if not timings_list:
        raise NoSchedulesException()
    
    # get the earliest next schedule_date
    timings_list.sort(key=lambda date_and_schedule: date_and_schedule[0])
    schedule_datetime, schedule = timings_list[0]
    return schedule_datetime, schedule


def export_weekly_survey_timings(survey: Survey) -> List[List[int]]:
    """Returns a json formatted list of weekly timings for use on the frontend"""
    # this weird sort order results in correctly ordered output.
    fields_ordered = ("hour", "minute", "day_of_week")
    timings = EMPTY_WEEKLY_SURVEY_TIMINGS()
    schedule_components = WeeklySchedule.objects. \
        filter(survey=survey).order_by(*fields_ordered).values_list(*fields_ordered)
    
    # get, calculate, append, dump.
    for hour, minute, day in schedule_components:
        timings[day].append((hour * 60 * 60) + (minute * 60))
    return timings


#
## new code that compares against archived events
#  This code was developed as part of a review of push notifications. The review found no
#  Issues, but thi code may be advantageous in the future.


def get_participant_ids_with_absolute_notification_history(schedule: AbsoluteSchedule) -> List[int]:
    """ Notes:
    1) ScheduledEvents for absolute schedules are created with the "canonical time" indicated by
       the date and time on the AbsoluteSchedule object as calculated by the "event_time"
       property, which returns a timezone aware datetime in the study's timezone at that time of
       day.
    2) When the Celery task checks for scheduled events it does the calculation for a full day
       ahead of the current time, and skips participants for whom that participant-tz time has not
       yet passed.
    3) But, the creation of an ArchivedEvent uses the ScheduledEvent time as the target, eg. the
       "canonical time", so we should always be able to find that events scheduled canonical time
    4) ArchivedEvents point at a survey archives, so we need to get a list of survey archive db
       ids and filter on that.
    5) we already don't really support live studies shifting timezones mid study, so we can ignore
       that case.
    survey = schedule.survey
    valid_survey_archive_ids = schedule.survey.archives.values_list("id", flat=True)
    study = survey.study
    study_timezone = study.timezone
    scheduled_time = schedule.event_time
    """
    return list(
        ArchivedEvent.objects.filter(
            scheduled_time=schedule.event_time,
            survey_archive_id__in=valid_survey_archive_ids,
            schedule_type=ScheduleTypes.absolute,
        ).exclude(
            RELATED_DELETED_TRUE | RELATED_PERMANENTLY_RETIRED_TRUE
        ).values_list("participant_id", flat=True)
    )


def get_participant_ids_with_relative_notification_history(schedule: RelativeSchedule) -> List[int]:
    """ Returns a list of participant database pks that need to have a notification sent base on a
    relative schedule. 
    Notes:
    1) ScheduledEvents for relative schedules have the same logical constraints as absolute
       schedules, e.g. scheduled events will use the eventual calculated time.
    2) Calculation of when to send a relative schedule notification is based on the presence of an
       intervention date. This means that if the intervention date changes than relative schedules
       will no longer match historical data in the ArchivedEvents, and the notifications will be
       recalculated and become a new, unsent, notification.
    3) But we can easily filter out participants that don't have the relevant intervention date
       for this relative intervention populated.
    4) The calculation time here is mostly in the timezone/datetime computation, which is up to
       the number of participants total, and then pulling in the number of historical objects on
       this survey, which is up to the number of relative schedules times the number of
       participants.
    """
    valid_survey_archive_ids = schedule.survey.archives.values_list("id", flat=True)
    
    potentially_valid_participants = list(schedule.intervention.intervention_dates.filter(
        date__isnull=False,
        # Subtle [Django?] behavior that I don't understand: you can't exclude null database values.
        # This query in a .exclude will return instances where date is None, same for `date=None`:
        #   intervention_dates_query.exclude(date__isnull=True, ...)
    ).exclude(
        RELATED_DELETED_TRUE | RELATED_PERMANENTLY_RETIRED_TRUE
    ).values_list("participant_id", "date", "participant__timezone_name"))
    
    participant_ids_that_might_need_a_notification = [
        participant_id for participant_id, _, _ in potentially_valid_participants
    ]
    
    # This code path needs to handle the case where there are multiple relative schedules for a survey.
    # participants_and_dates = []
    participants_to_calculated_times = defaultdict(list)
    for participant_id, vention_date, timezone_name in potentially_valid_participants:
        # 'days_after' is negative or 0 for days before and day of
        schedule_time = schedule.notification_time_from_intervention_date_and_timezone(
            vention_date + timedelta(days=schedule.days_after),  # computed date
            gettz(timezone_name)  # timezone lookup based on a string is cached
        )
        participants_to_calculated_times[participant_id].append(schedule_time)
    participants_to_calculated_times = dict(participants_to_calculated_times)  # convert to non-default-dict
    
    # get participants with sent notifications on this survey due to a relative schedule on this survey
    historical_event_participant_times = ArchivedEvent.objects.filter(
        participant__in=participant_ids_that_might_need_a_notification,
        survey_archive__in=valid_survey_archive_ids,
        schedule_type=ScheduleTypes.relative,
    ).values_list("participant_id", "scheduled_time")
    
    # Rule: don't send duplicate notifications - it is possible for multiple relative schedules to
    # calculate the same scheduled time for a survey notification - dumb but true. We want our
    # return to only have one instance of each participant id.
    
    # compare the historical data against the calculated times
    participants_already_sent_this_notification = set()
    for participant_id, historical_time in historical_event_participant_times:
        if historical_time in participants_to_calculated_times[participant_id]:
            participants_already_sent_this_notification.add(participant_id)
    
    # then get only the participants that haven't been sent this notification
    return list(
        set(participant_ids_that_might_need_a_notification) -
        participants_already_sent_this_notification
    )

from datetime import date, datetime, time, timedelta
from typing import Dict, List, Optional, Set, Tuple

from django.db.models import Q
from django.utils.timezone import make_aware

from constants.schedule_constants import EMPTY_WEEKLY_SURVEY_TIMINGS
from database.schedule_models import ArchivedEvent, InterventionDate, ScheduledEvent, WeeklySchedule
from database.study_models import Study
from database.survey_models import Survey
from database.user_models_participant import Participant
from libs.utils.date_utils import date_to_end_of_day, date_to_start_of_day


ENABLE_SCHEDULE_LOGGING = False

def log(*args, **kwargs):
    if ENABLE_SCHEDULE_LOGGING:
        print(*args, **kwargs)


class UnknownScheduleScenario(Exception): pass
class NoSchedulesException(Exception): pass
dt_combine = datetime.combine  # brevity

# make type annotations useful
SchedulePK = int
ParticipantPK = int
EventLookup = Tuple[SchedulePK, ParticipantPK, datetime]


#
## Various database query helpers
#

# Determining exclusion criteria for push notifications should occur in the task that checks for
# valid push notifications to send, but participants meeting these two criteria should have their
# ScheduledEvents fully deleted.  (remember to update the constant and the function if it changes)

EXCLUDE_THESE_PARTICIPANTS = Q(permanently_retired=True) | Q(deleted=True)


def participant_allowed_surveys(participant: Participant) -> bool:
    """ Returns whether we should bother to send a participant survey push notifications.
    (there is also a participant instance method, is_allowed_surveys, that imports this.) """
    if participant.deleted or participant.permanently_retired:
        return False
    return True


#
## Creating ScheduledEvents!
#

def repopulate_all_survey_scheduled_events(study: Study, participant: Optional[Participant] = None):
    """ Runs all the survey scheduled event generations on the provided entities. """
    log("repopulate_all_survey_scheduled_events")
    
    if study.study_is_stopped:
        ScheduledEvent.objects.filter(survey__study_id=study.pk).delete()
        return
    
    unschedule_surveys = []
    for survey in study.surveys.all():
        # remove any scheduled events on surveys that have been deleted.
        if survey.deleted:
            unschedule_surveys.append(survey)
            survey.scheduled_events.all().delete()
            continue
        
        log(f"repopulating all for survey {survey.id}")
        repopulate_weekly_survey_schedule_events(survey, participant)
        repopulate_absolute_survey_schedule_events(survey, participant)
        repopulate_relative_survey_schedule_events(survey, participant)
    
    # ScheduledEvent.objects.filter(survey__in=unschedule_surveys).delete()


def common_setup(
    survey,
    schedule_type: str,
    participant: Optional[Participant] = None
) -> Tuple[List[ScheduledEvent], List[ParticipantPK]]:
    # todo: factor out the repeated database queries here when called on the same study many times
    
    # we need the correct events and the correct participant pks
    filter_by_survey_type = survey_filter_lookup[schedule_type]
    filter_by_single_participant = {"participant_id": participant.pk} if participant else {}
    
    # don't exclude unpushable participants, this is the source of truth
    existing_events: List[ScheduledEvent] = list(
        survey.scheduled_events.filter(**filter_by_survey_type, **filter_by_single_participant)
    )
    
    if participant:
        if not participant_allowed_surveys(participant):
            return existing_events, []
        return existing_events, [participant.pk]
    else:
        return existing_events, list(
            survey.study.participants.exclude(EXCLUDE_THESE_PARTICIPANTS).values_list("pk", flat=True)
        )


#
## Absolute Schedules
#

def repopulate_absolute_survey_schedule_events(survey: Survey, participant: Optional[Participant] = None) -> None:
    log("absolute schedule events")
    existing_events, participant_pks = common_setup(survey, "absolute", participant)
    valid_event_data = setup_info_from_absolute_schedules(survey, participant_pks)
    schodelod_event_database_update(
        existing_events, valid_event_data, "absolute", survey
    )


def setup_info_from_absolute_schedules(
    survey: Survey, participant_pks: List[ParticipantPK]
) -> List[EventLookup]:
    # These steps inherently deduplicates events for this time-participant-scheduletype triplet.
    timezone = survey.study.timezone
    valid_event_data: List[EventLookup] = []
    for absolute_schedule in survey.absolute_schedules.all():
        scheduled_time = absolute_schedule.event_time(timezone)
        for participant_pk in participant_pks:
            valid_event_data.append((absolute_schedule.pk, participant_pk, scheduled_time))
    return valid_event_data

#
## Relative Schedules
#

def repopulate_relative_survey_schedule_events(survey: Survey, participant: Optional[Participant] = None) -> None:
    log("relative schedule events")
    
    existing_events, participant_pks = common_setup(survey, "relative", participant)
    # fill all_schudule_pks and all_possible_event_times
    valid_event_data = setup_info_from_relative_schedules(survey, participant_pks)
    
    schodelod_event_database_update(
        existing_events, valid_event_data, "relative", survey,
    )


def setup_info_from_relative_schedules(
    survey: Survey, participant_pks: List[ParticipantPK],
) -> List[EventLookup]:
    valid_event_data: List[EventLookup] = []
    timezone = survey.study.timezone
    
    # relative schedules exist only in relation to interventions, they have to be calculated rather
    # than looked up.
    participant_pks_and_dates_by_intervention_pk: Dict[int, List[Tuple[int, date]]] = \
        get_relative_schedule_intervention_lookup(survey, participant_pks)
    
    # go through valid every participant-intevention_date-relative-schedule combination
    for relative_schedule in survey.relative_schedules.all():
        # (this list is empty if there is no intervention date, so the participant will be skipped.)
        for participant_pk, interventiondate_date in participant_pks_and_dates_by_intervention_pk[relative_schedule.intervention_id]:
            # This '+' is correct, 'days_after' is negative or 0 for days before and day of.
            scheduled_date = interventiondate_date + timedelta(days=relative_schedule.days_after)
            scheduled_time = relative_schedule.notification_time_from_intervention_date_and_timezone(scheduled_date, timezone)
            valid_event_data.append((relative_schedule.pk, participant_pk, scheduled_time))
    
    return valid_event_data


def get_relative_schedule_intervention_lookup(
    survey: Survey, participant_pks: List[ParticipantPK],
) -> Dict[int, List[Tuple[int, date]]]:
    intervention_pks = list(survey.relative_schedules.values_list("intervention_id", flat=True))
    # relative schedules are "linked" to inverventions, the lookup is obnoxious.
    linking_query = InterventionDate.objects.filter(
        participant_id__in=participant_pks,
        intervention_id__in=intervention_pks,
        date__isnull=False,
        # Subtle Django behavior: you can't exclude nulls as database values.
        # This detail in a .exclude returns instances where date is None.
    ).values_list("intervention_id", "participant_id", "date")
    
    # populate all (even empty) intervention lookups (makes other code cleaner, debugging easier)
    intervention_lookups: Dict[int, List[Tuple[int, date]]] = {
        related_intervention_pk: [] for related_intervention_pk in intervention_pks
    }
    for related_intervention_pk, participant_pk, interventiondate_date in linking_query:
        intervention_lookups[related_intervention_pk].append((participant_pk, interventiondate_date))
    
    return intervention_lookups


#
## Weekly Schedules
#


def repopulate_weekly_survey_schedule_events(survey: Survey, participant: Optional[Participant] = None) -> None:
    log("weekly schedule events")
    existing_events, participant_pks = common_setup(survey, "weekly", participant)
    valid_event_data, but_dont_actually_create_these = get_info_for_weekly_events(survey, participant_pks)
    
    schodelod_event_database_update(
        existing_events,
        valid_event_data,
        "weekly",
        survey,
        but_dont_actually_create_these=but_dont_actually_create_these
    )


def get_info_for_weekly_events(
    survey: Survey, participant_pks: List[ParticipantPK]
) -> Tuple[List[EventLookup], Set[EventLookup]]:
    valid_event_data: List[EventLookup] = []
    but_dont_actually_create_these = []
    
    now, schedule_pks_and_times = get_bounded_2_week_window_of_weekly_schedule_pks_and_times(survey)
    
    # this is a last-week, this-week, next-week window of weekly schedules
    for schedule_pk, t in schedule_pks_and_times:
        # populate the data for all participants
        for participant_pk in participant_pks:
            eventlookup = (schedule_pk, participant_pk, t)
            valid_event_data.append(eventlookup)
            # don't actually create these if they are in the past (according to the study)
            # this is only applied to weekly schedules, not absolute and relative schedules.
            if t <= now:
                but_dont_actually_create_these.append(eventlookup)
    
    return valid_event_data, set(but_dont_actually_create_these)


def get_bounded_2_week_window_of_weekly_schedule_pks_and_times(
    survey: Survey
) -> Tuple[datetime, List[Tuple[SchedulePK, datetime]]]:
    # we need the times generated for every schedule, and the pk of the schedule it "came from"
    schedule_pks_and_times_in_bounded_window: List[Tuple[int, datetime]] = []
    
    # Using the study's timezone can shift the currently-decided-week, and therefore the exact batch
    # of queued up and deleted schedules by up to one day. That's fine.
    tz = survey.study.timezone
    now = survey.study.now()
    today = now.today()
    
    # The timings schema peshed to devices mimics the Java.util.Calendar.DayOfWeek specification,
    # which is zero-indexed with day 0 as Sunday.
    date_of_day_of_event_last_week: date  # day of event
    date_of_day_of_event_this_week: date  # day of event
    date_of_day_of_event_next_week: date  # day of event
    
    # Python's date.weekday() defines Monday=0, in our schema Sunday=0 so we add 1p Sow we can just
    # add the day_of_week value later and get the correct _date_ under all conditions because the
    # datetime library will handle things like leap years.
    start_of_this_week: date = today - timedelta(days=((today.weekday()+1) % 7))  # Sunday.
    
    for pk, day_of_week, hour, minute in survey.weekly_schedules.values_list("pk", "day_of_week", "hour", "minute"):
        t = time(hour, minute)
        
        # shifting a date my a day length time delta bypasses daylight savings time stretching.
        date_of_day_of_event_this_week = start_of_this_week + timedelta(days=day_of_week)
        date_of_day_of_event_last_week = date_of_day_of_event_this_week - timedelta(days=7)
        date_of_day_of_event_next_week = date_of_day_of_event_this_week + timedelta(days=7)
        
        # We need the time of the day on a date, and then we need to handle the timezone gracefully,
        # make_aware handles shifting ambiguous times so the code doesn't crash.
        dt_of_event_last_week = make_aware(dt_combine(date_of_day_of_event_last_week, t), tz)
        dt_of_event_this_week = make_aware(dt_combine(date_of_day_of_event_this_week, t), tz)
        dt_of_event_next_week = make_aware(dt_combine(date_of_day_of_event_next_week, t), tz)
        
        # and then we wil need a listing to reverse a schedule pk to a datetime.
        schedule_pks_and_times_in_bounded_window.append((pk, dt_of_event_last_week))
        schedule_pks_and_times_in_bounded_window.append((pk, dt_of_event_this_week))
        schedule_pks_and_times_in_bounded_window.append((pk, dt_of_event_next_week))
    
    return now, schedule_pks_and_times_in_bounded_window

#
## Weekly Timings Lists
#

def export_weekly_survey_timings(survey: Survey) -> List[List[int]]:
    """Returns a json formatted list of weekly timings for use on the frontend and devices. Not part of scheduling. """
    # this weird sort order results in correctly ordered output.
    fields_ordered = ("hour", "minute", "day_of_week")
    timings = EMPTY_WEEKLY_SURVEY_TIMINGS()
    schedule_components = WeeklySchedule.objects. \
        filter(survey=survey).order_by(*fields_ordered).values_list(*fields_ordered)
    
    # get, calculate, append, dump.
    for hour, minute, day in schedule_components:
        timings[day].append((hour * 60 * 60) + (minute * 60))
    return timings


def get_start_and_end_of_java_timings_week(now: datetime) -> Tuple[datetime, datetime]:
    """ study timezone aware week start and end """
    if now.tzinfo is None:
        raise TypeError("missing required timezone-aware datetime")
    now_date: date = now.date()
    date_sunday_start_of_week = now_date - timedelta(days=now.weekday() + 1)
    date_saturday_end_of_week = now_date + timedelta(days=5 - now.weekday())  #  TODO: Test
    dt_sunday_start_of_week = date_to_start_of_day(date_sunday_start_of_week, now.tzinfo)
    dt_saturday_end_of_week = date_to_end_of_day(date_saturday_end_of_week, now.tzinfo)
    return dt_sunday_start_of_week, dt_saturday_end_of_week


def decompose_datetime_to_device_weekly_timings(dt: datetime) -> Tuple[int, int]:
    """ returns day-index, seconds into day. """
    # have to convert to sunday-zero-indexed
    return (dt.weekday() + 1) % 7, dt.hour * 60 * 60 + dt.minute * 60


#
## Shared ScheduledEvent Creation Code
#
# Give it:
# - the state of the database that you wish to compare to
# - the state of truth for what should exist
# - the type of schedule you are working with
# - Optioal: specific instances that shouldn't _actually_ be created
#
# The state provided does not need to represent the entire state of the database, only relevant
#   state, like the state of a single participant.
#
# AFTER the schedules are updated we can check on the intersection of only the newly created events
#   and archived events to see if any have already been sent, and mark them.
#
# Combined with disallowing duplicate Schedules in the first place this means we only create new
#   ScheduledEvents when we need to instead of every time we have an update.  This fixes many
#   obscure corner cases that caused bugs in the past.


def schodelod_event_database_update(
    existing_events: List[ScheduledEvent],
    valid_event_data: List[EventLookup],
    type_of_schedule: str,
    survey: Survey,
    but_dont_actually_create_these: Set[EventLookup] = set(),
):  
    existing_event_lookup: Set[EventLookup] = set(
        (event.get_schedule_pk(), event.participant_id, event.scheduled_time)
         for event in existing_events
    )
    
    new_event_info_to_create = determine_events_to_create(
        existing_event_lookup, valid_event_data, but_dont_actually_create_these
    )
    
    existing_event_pks_to_delete = determine_events_to_delete(
        existing_events, valid_event_data
    )
    
    deleted = ScheduledEvent.objects.filter(
        id__in=[pk for pk in existing_event_pks_to_delete]
    ).delete()
    log("deleted", deleted)
    
    create_new_event_objects(
        survey,
        new_event_info_to_create,
        type_of_schedule,
    )


def determine_events_to_delete(
    existing_events: List[ScheduledEvent],
    valid_event_data: List[EventLookup],
) -> List[SchedulePK]:
    existing_events_to_delete: List[SchedulePK] = []
    valid_events_lookup = set(valid_event_data)
    
    for event in existing_events:
        key: EventLookup = (event.get_schedule_pk(), event.participant_id, event.scheduled_time)
        if key not in valid_events_lookup:
            existing_events_to_delete.append(event.pk)
    
    return existing_events_to_delete


def determine_events_to_create(
    existing_event_lookup: Set[EventLookup],
    valid_event_data: List[EventLookup],
    but_dont_actually_create_these: Set[EventLookup],
) -> List[EventLookup]:
    events_to_create: List[EventLookup] = []
    
    # loop over all possible participant-time pairs, append participant-time pairs that don't already exist
    for schedule_participant_and_time in valid_event_data:
        if schedule_participant_and_time not in existing_event_lookup:
            if schedule_participant_and_time in but_dont_actually_create_these:
                continue
            events_to_create.append(schedule_participant_and_time)
    return events_to_create


def create_new_event_objects(
    survey: Survey,
    eventlookups_to_create: List[EventLookup],
    type_of_schedule: str,
):
    new_event_objects_to_create: List[ScheduledEvent] = []
    # we need a slightly different set of arguments to instantiate different ScheduledEvents
    arg_constructor: callable = arg_constructor_lookup[type_of_schedule]
    
    # schudule_pks_by_participant_and_time is a lookup for the abs/week/rel schedule pk
    for schedule_pk, participant_pk, scheduled_time in eventlookups_to_create:
        new_event_objects_to_create.append(ScheduledEvent(
            survey=survey,
            scheduled_time=scheduled_time,
            participant_id=participant_pk,
            **arg_constructor(schedule_pk),
        ))
    
    created_objects = ScheduledEvent.objects.bulk_create(new_event_objects_to_create)
    log("created", created_objects)
    
    # TODO: we should emit two creation queries instead of checking after the fact.
    check_archives_for_newly_created_scheduled_events_to_mark_as_deleted(
        survey, eventlookups_to_create, created_objects
    )


def check_archives_for_newly_created_scheduled_events_to_mark_as_deleted(
    survey: Survey, created_eventlookups: List[EventLookup], created_events: List[ScheduledEvent]
):
    # This DOES NOT filter by schedule type
    participant_pks = list(set(participant_pk for _, participant_pk, _ in created_eventlookups))
    times = list(set(time for _, _, time in created_eventlookups))
    
    new_and_marked_as_unsent_event_lookup = {
        (event.participant_id, event.scheduled_time): event.id for event in created_events
    }
    
    relevant_participant_and_schedule_time_query = ArchivedEvent.objects.filter(
        participant_id__in=participant_pks,
        scheduled_time__in=times,
        survey_archive__survey=survey,
    ).values_list("participant_id", "scheduled_time")
    
    mark_as_deleted = []
    for participant_pk, scheduled_time in relevant_participant_and_schedule_time_query:
        key = (participant_pk, scheduled_time)
        if key in new_and_marked_as_unsent_event_lookup:
            mark_as_deleted.append(new_and_marked_as_unsent_event_lookup[key])
    
    updates = ScheduledEvent.objects.filter(id__in=mark_as_deleted).update(deleted=True)
    if updates:
        log("updated already sent events", updates)
    # TODO: we should attach the survey archive to the ScheduledEvent... but there can be multiple matches...


#
# The helpers we need because we are not usisg higher levels of abstraction (this is a good thing).
#

def absolute_orgs(abs_sched_pk: int):
    return {
        "weekly_schedule": None, "relative_schedule": None, "absolute_schedule_id": abs_sched_pk
    }


def relative_orgs(rel_sched_pk: int):
    return {
        "weekly_schedule": None, "relative_schedule_id": rel_sched_pk, "absolute_schedule": None
    }


def weekly_orgs(weekly_sched_pk: int):
    return {
        "weekly_schedule_id": weekly_sched_pk, "relative_schedule": None, "absolute_schedule": None
    }


arg_constructor_lookup = {"absolute": absolute_orgs, "relative": relative_orgs, "weekly": weekly_orgs}


# These parameters go into the filter for the ScheduledEvent query.
survey_filter_lookup = {
    "absolute": dict(absolute_schedule_id__isnull=False, relative_schedule=None, weekly_schedule=None),
    "relative": dict(absolute_schedule=None, relative_schedule_id__isnull=False, weekly_schedule=None),
    "weekly":   dict(absolute_schedule=None, relative_schedule=None, weekly_schedule_id__isnull=False),
}


def get_next_weekly_event_and_schedule(survey: Survey) -> Tuple[datetime, WeeklySchedule]:
    """ Determines the next time for a particular survey, provides the relevant weekly schedule. """
    #  TODO: make this use the participant's timezone.  That introduces the possibility of a missed
    # scheduled event if the participant's timezone changes between individual survey notifications,
    # because this is set without them.  We don't support that now.
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

from collections import defaultdict
from datetime import date, datetime, time, timedelta
from typing import DefaultDict, Dict, List, Optional, Set, Tuple

from django.db.models import Q
from django.utils import timezone
from django.utils.timezone import make_aware

from constants.schedule_constants import EMPTY_WEEKLY_SURVEY_TIMINGS, ScheduleTypes
from database.schedule_models import ArchivedEvent, InterventionDate, ScheduledEvent, WeeklySchedule
from database.study_models import Study
from database.survey_models import Survey
from database.user_models_participant import Participant
from libs.push_notification_helpers import (slowly_get_stopped_study_ids,
    update_ArchivedEvents_from_SurveyNotificationReports)
from libs.utils.date_utils import date_to_end_of_day, date_to_start_of_day


ENABLE_SCHEDULE_LOGGING = False

def log(*args, **kwargs):
    if ENABLE_SCHEDULE_LOGGING:
        print(*args, **kwargs)


class UnknownScheduleScenario(Exception): pass
class NoSchedulesException(Exception): pass
dt_combine = datetime.combine  # brevity

#
## Various database query helpers
#

# Determining exclusion criteria is just complex, I've found no great way to factor it.
DELETED_TRUE = Q(deleted=True)
PERMANENTLY_RETIRED_TRUE = Q(permanently_retired=True)
EXCLUDE_THESE_PARTICIPANTS = DELETED_TRUE | PERMANENTLY_RETIRED_TRUE
RELATED_DELETED_TRUE = Q(participant__deleted=True)
RELATED_PERMANENTLY_RETIRED_TRUE = Q(participant__permanently_retired=True)
EXCLUDE_THESE_PARTICIPANTS_RELATED = RELATED_DELETED_TRUE | RELATED_PERMANENTLY_RETIRED_TRUE


def participant_allowed_surveys(participant: Participant) -> bool:
    """ Returns whether we should bother to send a participant survey push notifications. """
    if participant.deleted or participant.permanently_retired:
        return False
    return True


#
## Creating ScheduledEvents!
#

def repopulate_all_survey_scheduled_events(study: Study, participant: Participant = None):
    """ Runs all the survey scheduled event generations on the provided entities. """
    log("repopulate_all_survey_scheduled_events")
    
    if study.study_is_stopped:
        ScheduledEvent.objects.filter(survey__study=study).delete()
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
    
    ScheduledEvent.objects.filter(survey__in=unschedule_surveys).delete()

#
## Absolute Schedules
#

def repopulate_absolute_survey_schedule_events(survey: Survey, participant: Optional[Participant] = None) -> None:
    log("absolute schedule events")
    
    # if the event is from an absolute schedule, relative and weekly schedules will be None
    existing_events: List[ScheduledEvent] = list(
        survey.scheduled_events.filter(
            relative_schedule=None,
            weekly_schedule=None,
            **({"participant_id": participant.pk} if participant else {})
        )
    )
    all_schudule_pks_by_participant_and_time: Dict[Tuple[int, datetime], int] = {}
    
    # prefill a dictionary of empty sets for all (relevant) participants
    all_possible_event_times_by_participant: Dict[int, Set[datetime]] = {
        pk: set() for pk in
        survey.study.participants.exclude(EXCLUDE_THESE_PARTICIPANTS).values_list("pk", flat=True)
    }
    
    # fill all_schudule_pks and all_possible_event_times
    setup_info_from_absolute_schedules(
        survey, all_possible_event_times_by_participant, all_schudule_pks_by_participant_and_time
    )
    
    ok_now_the_almost_generic_database_update_bit(
        existing_events,
        all_schudule_pks_by_participant_and_time,
        all_possible_event_times_by_participant,
        "absolute",
        survey,
    )


def setup_info_from_absolute_schedules(
    survey: Survey,
    all_possible_event_times_by_participant: Dict[int, Set[datetime]],
    all_schudule_pks_by_participant_and_time: Dict[Tuple[int, datetime], int],
):
    # These steps inhelently deduplicates events for this time-participant-scheduletype triplet.
    timezone = survey.study.timezone
    
    for absolute_schedule in survey.absolute_schedules.all():
        scheduled_time = absolute_schedule.event_time(timezone)
        for participant_pk in list(all_possible_event_times_by_participant.keys()):
            all_possible_event_times_by_participant[participant_pk].add(scheduled_time)
            all_schudule_pks_by_participant_and_time[participant_pk, scheduled_time] = absolute_schedule.pk


#
## Relative Schedules
#


def repopulate_relative_survey_schedule_events(survey: Survey, participant: Optional[Participant] = None) -> None:
    log("relative schedule events")
    
    # if the event is from an absolute schedule, relative and weekly schedules will be None
    existing_events: List[ScheduledEvent] = list(
        survey.scheduled_events.filter(
            absolute_schedule=None,
            weekly_schedule=None,
            **({"participant_id": participant.pk} if participant else {})
        )
    )
    all_schudule_pks_by_participant_and_time: Dict[Tuple[int, datetime], int] = {}
    
    # prefill a dictionary of empty sets for all (relevant) participants
    all_possible_event_times_by_participant: Dict[int, Set[datetime]] = {
        pk: set() for pk in
        survey.study.participants.exclude(EXCLUDE_THESE_PARTICIPANTS).values_list("pk", flat=True)
    }
    
    # fill all_schudule_pks and all_possible_event_times
    setup_info_from_relative_schedules(
        survey,
        all_possible_event_times_by_participant,
        all_schudule_pks_by_participant_and_time,
    )
    
    ok_now_the_almost_generic_database_update_bit(
        existing_events,
        all_schudule_pks_by_participant_and_time,
        all_possible_event_times_by_participant,
        "relative",
        survey,
    )


def setup_info_from_relative_schedules(
    survey: Survey,
    all_possible_event_times_by_participant: Dict[int, Set[datetime]],
    all_schudule_pks_by_participant_and_time: Dict[Tuple[int, datetime], int],
):
    timezone = survey.study.timezone
    
    # relative schedules exist only in relation to interventions, they have to be calculated rather
    # than looked up.
    participant_pks_and_dates_by_intervention_pk: Dict[int, Tuple[int, date]] = \
        get_relative_schedule_intervention_lookup(survey, all_possible_event_times_by_participant)
    
    # go through valid every participant-intevention_date-relative schedule combination
    for relative_schedule in survey.relative_schedules.all():
        for participant_pk, interventiondate_date in participant_pks_and_dates_by_intervention_pk[relative_schedule.intervention_id]:
            # This '+' is correct, 'days_after' is negative or 0 for days before and day of.
            scheduled_date = interventiondate_date + timedelta(days=relative_schedule.days_after)
            scheduled_time = relative_schedule.notification_time_from_intervention_date_and_timezone(scheduled_date, timezone)
            all_possible_event_times_by_participant[participant_pk].add(scheduled_time)
            all_schudule_pks_by_participant_and_time[participant_pk, scheduled_time] = relative_schedule.pk


def get_relative_schedule_intervention_lookup(
    survey: Survey, all_possible_event_times_by_participant: Dict[int, Set[datetime]], 
) -> Dict[int, Tuple[int, date]]:
    intervention_pks = list(survey.relative_schedules.values_list("intervention_id", flat=True))
    # relative schedules are "linked" to inverventions, the lookup is obnoxious.
    linking_query = InterventionDate.objects.filter(
        participant_id__in=all_possible_event_times_by_participant.keys(),
        intervention_id__in=intervention_pks,
        date__isnull=False,
        # Subtle Django behavior: you can't exclude nulls as database values.
        # This detail in a .exclude returns instances where date is None.
    ).values_list("intervention_id", "participant_id", "date")
    
    # populate all (even empty) intervention lookups (makes other code cleaner, debugging easier)
    intervention_lookups: Dict[int, Tuple[int, date]] = {
        related_intervention_pk: [] for related_intervention_pk in intervention_pks
    }
    for related_intervention_pk, participant_pk, interventiondate_date in linking_query:
        intervention_lookups[related_intervention_pk].append((participant_pk, interventiondate_date))
    
    return intervention_lookups


#
## Weekly Schedules
#

#TODO: can we remove this function and use the top-up function instead?  no because we need to handle the case of new weeklies? no because we need to delete all weeklies?
#TODO: this should contain the deduplication code to make the new top-up not n+1 / n*k queries
def repopulate_weekly_survey_schedule_events(survey: Survey, single_participant: Optional[Participant] = None) -> None:
    """ Clear existing schedules, get participants, bulk create schedules Weekly events are
    calculated in a way that we don't bother checking for survey archives, because they only exist
    in the future. """
    
    # Clear existing schedules, get participants
    log("clearing weekly schedule events")
    events = survey.scheduled_events.filter(relative_schedule=None, absolute_schedule=None)
    if single_participant:
        events = events.filter(participant=single_participant)
    events.delete()
    
    if single_participant and not participant_allowed_surveys(single_participant):
        log("weekly bad participant")
        return
    
    # filters out participants that shouldn't receive notifications.
    valid_participant_pks, archive_scheduled_times_by_participant_pk = \
        validate_and_assemble_archive_scheduled_times_by_participant_pk(
            survey, single_participant, ScheduleTypes.weekly
        )
    
    try:
        # get_next_weekly_event forces tz-aware schedule_date datetime object
        schedule_datetime, schedule = get_next_weekly_event_and_schedule(survey)
    except NoSchedulesException:
        log("weekly no schedules configured")
        return
    
    new_weeklies = []
    for participant_id in valid_participant_pks:
        if schedule_datetime not in archive_scheduled_times_by_participant_pk[participant_id]:
            new_weeklies.append(ScheduledEvent(
                survey=survey,
                participant_id=participant_id,
                weekly_schedule=schedule,
                relative_schedule=None,
                absolute_schedule=None,
                scheduled_time=schedule_datetime,
            ))
    info = ScheduledEvent.objects.bulk_create(new_weeklies)
    log(f"weekly schedule events created {info}")
    return bool(len(info))


def validate_and_assemble_archive_scheduled_times_by_participant_pk(
    survey: Survey, participant: Optional[Participant], schedule_type: str
) -> Dict[int, Set[datetime]]:
    """ Get a fast lookup dict for sent schedules of a type for a survey. """
    # - ArchivedEvents created by schedules that get deleted (happens if they are regenerated) have
    # their uuids removed in the resend logic.
    # - Resend logic can cause many ArchivedEvents per notification.
    # - handle unapplied SurveyNotificationReports
    
    participant_pks = list(
        survey.study.participants.exclude(EXCLUDE_THESE_PARTICIPANTS).values_list("pk", flat=True)
    )
    
    # a single participant is always already checked for exclusion criteria.
    if participant:
        if not participant_allowed_surveys(participant):
            raise ValueError("participant should already be validated")
        participant_pks = [participant.pk]
    
    # We need to run part of notification resend logic to make sure confirmed_received is fully
    # populated, and minimize the window for a race condition to occur.
    update_ArchivedEvents_from_SurveyNotificationReports(participant_pks, timezone.now(), log)
    
    # - ArchivedEvents predating the resend-missed-notifications feature do not have uuids.
    # - ArchivedEvents after the feature will have a uuid, and confirmed_received=False if the
    #   device has not checked in.
    # Race Condition:  -  Push notification is sent, we have no receipt report - yet.
    # - We will not find that ArchivedEvent in this query, so we don't return the event.
    # - So we will not block a new ScheduledEvent from being created -  _with a new uuid._
    # - Receiving a receipt with a uuid of an actually-deleted ScheduledEvent does nothing.
    # However.
    # - Presumably such a survey notification will be in the past (and fully-participant-
    #   timezone-checked?), so we won't recreate it.
    archived_events = ArchivedEvent.objects.filter(
        Q(uuid__isnull=True) | Q(confirmed_received=False, uuid__isnull=False),
        survey_archive__survey=survey,
        schedule_type=schedule_type,
        participant_id__in=participant_pks
    ).exclude(
        EXCLUDE_THESE_PARTICIPANTS_RELATED
    )
    
    archive_scheduled_times_by_participant_pk = defaultdict(set)
    for p_pk, t in archived_events.values_list("participant_id", "scheduled_time"):
        archive_scheduled_times_by_participant_pk[p_pk].add(t)
    
    # we may have missed participants, we want them to have empty sets.
    for pk in participant_pks:
        archive_scheduled_times_by_participant_pk[pk]
    
    return participant_pks, dict(archive_scheduled_times_by_participant_pk)


def get_next_weekly_event_and_schedule(survey: Survey) -> Tuple[datetime, WeeklySchedule]:
    """ Determines the next time for a particular survey, provides the relevant weekly schedule. """
    # TODO: make this use the participant's timezone.  That introduces the possibility of a missed
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


def get_bounded_3_week_window_on_current_week(survey: Survey) -> WeeklySchedule:
    # We DON'T want to use study.now().today.  Day of week only cares about date.
    tz = survey.study.timezone
    today = date.today()
    
    bounded_window_of_schedule_datetimes: List[datetime] = []
    
    # The timings schema peshed to devices mimics the Java.util.Calendar.DayOfWeek specification,
    # which is zero-indexed with day 0 as Sunday.
    start_of_this_week: date  # Sunday.
    date_of_day_of_event_last_week: date  # day of event
    date_of_day_of_event_this_week: date  # day of event
    date_of_day_of_event_next_week: date  # day of event
    
    # Python's date.weekday() defines Monday=0, in our schema Sunday=0 so we add 1p Sow we can just
    # add the day_of_week value later and get the correct _date_ under all conditions because the
    # datetime library will handle things like leap years.
    start_of_this_week: date = today - timedelta(days=((today.weekday()+1) % 7))  # Sunday.
    
    for day_of_week, hour, minute in survey.weekly_schedules.values_list("day_of_week", "hour", "minute"):
        
        # shifting a date my a day length time delta bypasses daylight savings time stretching.
        date_of_day_of_event_this_week = start_of_this_week + timedelta(days=day_of_week)
        date_of_day_of_event_last_week = date_of_day_of_event_this_week - timedelta(days=7)
        date_of_day_of_event_next_week = date_of_day_of_event_this_week + timedelta(days=7)
        
        # We need the time of the day on a date, and then we need to handle the timezone gracefully,
        # make_aware handles shifting ambiguous times so the code doesn't crash.
        t = time(hour, minute)
        dt_of_event_last_week = make_aware(dt_combine(date_of_day_of_event_last_week, t), tz)
        dt_of_event_this_week = make_aware(dt_combine(date_of_day_of_event_this_week, t), tz)
        dt_of_event_next_week = make_aware(dt_combine(date_of_day_of_event_next_week, t), tz)
        
        bounded_window_of_schedule_datetimes.append(dt_of_event_last_week)
        bounded_window_of_schedule_datetimes.append(dt_of_event_this_week)
        bounded_window_of_schedule_datetimes.append(dt_of_event_next_week)


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


#TODO: does the old weekly creation function contain the deduplication code to make the new top-up not n+1 / n*k queries
def update_all_weekly_schedules() -> None:
    """ Handles updates for all weekly survey schedules for all participants. """
    participant: Participant
    survey: Survey
    
    now = timezone.now()
    
    # - We don't want to use the fcm_for_pushable_participants function, we want to top-up All
    #   Potentially Pushable participants, then clears out old weekly schedules. We want to ensure
    #   new participants created a year after creation don't get slammed with thousands of
    #   notification uuids. (its also easier than a query to exclude, and the table is smaller.)
    # - We order these queries to make the output deterministic.  There is no other reason.
    
    # There are a lot of ways to make this query, this one is fine, we don't care about performance,
    # we need to use `slowly_get_stopped_study_ids` anyway.
    study_ids_to_refresh_weeklies = list(
        set(
            WeeklySchedule.objects.filter(survey__deleted=False)
            .values_list("survey__study_id", flat=True).distinct()
        )
        -
        set(slowly_get_stopped_study_ids())
    )
    list.sort()
    
    participants = Participant.filter_possibly_pushable_participants(
        study_id__in=study_ids_to_refresh_weeklies
    ).order_by("patient_id")
    
    participants_by_study = defaultdict(list)
    for participant in participants:
        participants_by_study[participant.study_id].append(participant)
    participants_by_study = dict(participants_by_study)
    
    surveys = Survey.objects.filter(
        study_id__in=study_ids_to_refresh_weeklies, deleted=False
    ).order_by("study_id")
    
    # ok, this makes too many queries and they are the same query...
    for survey in surveys:
        for participant in participants_by_study[survey.study_id]:
            ret = set_next_weekly(participant, survey)
            survey_ident = survey.survey_name if survey.survey_name else survey.object_id
            log(f"updated weekly schedules for {participant.patient_id} '{survey_ident}': {ret}")
    
    # - Delete all weekly scheduled events that are more than 16 days old. 15 days should be greater
    #   than all reasonable timezone side effects, 16 gives enough buffer to cover for the really
    #   weird ones like flying through the international date line and on the day of a daylight
    #   savings time change. (In fact I'm probably doing the math wrong and we only need 9 days.)
    # - Its ok to delete old weekly scheduled events because they will be replaced on the real-world
    #   practical level every week.
    # - We can live with extra weekly scheduled events hanging around and possibly being sent
    #   because they will get merged ino a single notification whenever they are sent.

    ScheduledEvent.objects.filter(
        weekly_schedule__isnull=False,
        relative_schedule=None,
        absolute_schedule=None,
        scheduled_time__lt=now - timedelta(days=16)
    ).delete()


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


#
## Weekly Timings Lists
#


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


def decompose_datetime_to_device_weekly_timings(dt: datetime) -> Tuple[int, int]:
    """ returns day-index, seconds into day. """
    # have to convert to sunday-zero-indexed
    return (dt.weekday() + 1) % 7, dt.hour * 60 * 60 + dt.minute * 60


#
## Shared ScheduledEvent Creation Code 
# 
# Takes some data structures that represents what does exist and what should exist, diffs it, and
#   emits the appropriate database updates to ScheduledEvents.
# (there's also an extra data structure required to link Schedules to ScheduledEvents and a type flag)
#
# With this pattern the 
#
# The state provided does not need to represent the entire state of the database, only relevant state,
#   in particular only the state of a single participant.
# (and also it is restricted to the one type of Schedule based on the flag)
#
# AFTER the schedules are updated we can check on the intersection of only the newly created events
#   and archived events to see if any have already been sent, and mark them occasionally.
#
# Combined with disallowing duplicate Schedules in the first place this means we only create new
#   ScheduledEvents when we need to instead of every time we have an update.

def ok_now_the_almost_generic_database_update_bit(
    existing_events: List[ScheduledEvent],                                      # currently exists
    all_schudule_pks_by_participant_and_time: Dict[Tuple[int, datetime], int],  # should exist
    all_possible_event_times_by_participant: Dict[int, Set[datetime]],          # the lookup dict
    type_of_schedule: str,
    survey: Survey,
):
    new_event_info_to_create = determine_events_to_create(
        existing_events, all_possible_event_times_by_participant
    )
    existing_events_to_delete = determine_events_to_delete(
        existing_events, all_possible_event_times_by_participant
    )
    
    deleted = ScheduledEvent.objects.filter(
        id__in=[event.id for event in existing_events_to_delete]
    ).delete()
    log("deleted", deleted)
    
    create_new_event_objects(
        survey,
        new_event_info_to_create,
        all_schudule_pks_by_participant_and_time,
        type_of_schedule,
    )


def determine_events_to_delete(
    existing_events: List[ScheduledEvent],
    all_possible_event_times_by_participant: Dict[int, Set[datetime]],
) -> Tuple[List[ScheduledEvent], List[Tuple[int, datetime]]]:
    existing_events_to_delete = []
    
    # loop over existing events
    for existing_event in existing_events:
        particiant_pk = existing_event.participant_id
        existing_event_time = existing_event.scheduled_time
        # if it isn't at a possible time then it should be deleted.
        if existing_event_time not in all_possible_event_times_by_participant[particiant_pk]:
            existing_events_to_delete.append(existing_event)
    
    return existing_events_to_delete


def determine_events_to_create(
    existing_events: List[ScheduledEvent],
    all_possible_event_times_by_participant: Dict[int, Set[datetime]],
) -> List[Tuple[int, datetime]]:
    events_to_create: List[Tuple[int, datetime]] = []
    
    # events at this time already exist
    existing_events_by_participant_and_time = set(
        (event.participant_id, event.scheduled_time) for event in existing_events
    )
    
    # loop over all possible participant-time pairs
    for participant_pk in all_possible_event_times_by_participant.keys():
        for scheduled_time in all_possible_event_times_by_participant[participant_pk]:
            # append participant-time pairs that don't already exist
            if (participant_pk, scheduled_time) not in existing_events_by_participant_and_time:
                events_to_create.append((participant_pk, scheduled_time))
    
    return events_to_create


def create_new_event_objects(
    survey: Survey,
    new_event_info_to_create: List[Tuple[int, datetime]],
    all_schudule_pks_by_participant_and_time: Dict[Tuple[int, datetime], int],
    type_of_schedule: str,
):
    new_event_objects_to_create: List[ScheduledEvent] = []
    # we need a slightly different set of arguments to instantiate different ScheduledEvents
    arg_constructor = arg_constructor_lookup[type_of_schedule]
    
    # all_schudule_pks_by_participant_and_time is a lookup for the abs/week/rel schedule pk
    x = []
    for participant_pk, scheduled_time in new_event_info_to_create:
        schedule_pk = all_schudule_pks_by_participant_and_time[participant_pk, scheduled_time]
        new_event_objects_to_create.append(ScheduledEvent(
            survey=survey,
            scheduled_time=scheduled_time,
            participant_id=participant_pk,
            **arg_constructor(schedule_pk),
        ))
    
    created_objects = ScheduledEvent.objects.bulk_create(new_event_objects_to_create)
    log("created", created_objects)
    
    #TODO: we should emit two creation queries instead of checking after the fact.
    check_archives_for_newly_created_scheduled_events(survey, new_event_info_to_create, created_objects)


def check_archives_for_newly_created_scheduled_events(
    survey: Survey, new_event_infos: List[Tuple[int, datetime]], created_events: List[ScheduledEvent]
):
    # This DOES NOT filter by schedule type
    participant_pks = list(set(participant_pk for participant_pk, _ in new_event_infos))
    times = list(set(time for _, time in new_event_infos))
    
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
    log("updated already send events", updates)


# The helpers we need because we are not usisg higher levels of abstraction (this is a good thing).


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


# don't ask for a schedule type that doesn't exist.
arg_constructor_lookup = {"absolute": absolute_orgs, "relative": relative_orgs, "weekly": weekly_orgs}


#
## Old Code that deleted scheduled events and then created new ones, and then searched archives
# for events that should not be recreated.
#

# def repopulate_absolute_survey_schedule_events_old(
#         survey: Survey, single_participant: Optional[Participant] = None) -> None:
#     """ Creates new ScheduledEvents for the survey's AbsoluteSchedules while deleting the old
#     ScheduledEvents related to the survey """
#     log("absolute schedule events")
#     timezone = survey.study.timezone
    
#     # if the event is from an absolute schedule, relative and weekly schedules will be None
#     events = survey.scheduled_events.filter(relative_schedule=None, weekly_schedule=None)
#     if single_participant:
#         events = events.filter(participant=single_participant)
#     events.delete()
    
#     if single_participant and not participant_allowed_surveys(single_participant):
#         log("absolute bad participant")
#         return
    
#     # filters out participants that shouldn't receive notifications.
#     valid_participant_pks, archive_scheduled_times_by_participant_pk = \
#         validate_and_assemble_archive_scheduled_times_by_participant_pk(
#             survey, single_participant, ScheduleTypes.absolute
#         )
    
#     # for each absolute schedule on the survey create a new scheduled event for each participant.
#     new_scheduled_events = []
#     for absolute_schedule in survey.absolute_schedules.all():
#         # We created ScheduledEvents with a "Canonical Time" in the study's timezone.
#         # The Canonical Time conversions for participant timezones are done in the Celery task.
#         scheduled_time = absolute_schedule.event_time(timezone)
        
#         # filter out participants that have already received a this-timestamp notification.
#         relevant_participant_pks = [
#             pk for pk in valid_participant_pks
#             if scheduled_time not in archive_scheduled_times_by_participant_pk[pk]
#         ]
        
#         for participant_pk in relevant_participant_pks:
#             new_scheduled_events.append(ScheduledEvent(
#                 survey=survey,
#                 weekly_schedule=None,
#                 relative_schedule=None,
#                 absolute_schedule_id=absolute_schedule.pk,
#                 scheduled_time=scheduled_time,
#                 participant_id=participant_pk
#             ))
    
#     # save to database, return whether we created any new events.
#     info = ScheduledEvent.objects.bulk_create(new_scheduled_events)
#     log(f"absolute schedule events created {info}")
#     return bool(len(info))


# def repopulate_relative_survey_schedule_events_old(
#         survey: Survey, single_participant: Optional[Participant] = None) -> None:
#     """ Creates new ScheduledEvents for the survey's RelativeSchedules while deleting the old
#     ScheduledEvents related to the survey. """
#     log("relative schedule events")
#     study_timezone = survey.study.timezone
    
#     # Clear out existing events.
#     events = survey.scheduled_events.filter(absolute_schedule=None, weekly_schedule=None)
#     if single_participant:
#         events = events.filter(participant=single_participant)
#     events.delete()
    
#     if single_participant and not participant_allowed_surveys(single_participant):
#         log("relative bad participant")
#         return
    
#     # filters out participants that shouldn't receive notifications.
#     valid_participant_pks, archive_scheduled_times_by_participant_pk = \
#         validate_and_assemble_archive_scheduled_times_by_participant_pk(
#             survey, single_participant, ScheduleTypes.relative
#         )
    
#     linking_query = InterventionDate.objects.filter(
#         participant_id__in=valid_participant_pks,
#         intervention_id__in=survey.relative_schedules.values_list("intervention_id", flat=True),
#         date__isnull=False,
#         # Subtle Django behavior: you can't exclude nulls as database values.
#         # This detail in a .exclude returns instances where date is None.
#     ).values_list("intervention_id", "participant_id", "date")
    
#     # InterventionDates map through interventions - don't lock the defaultdict.
#     valid_intervention_lookups: DefaultDict[int, Tuple[int, date]] = defaultdict(list)
#     for intervention_pk, participant_pk, intervention_date_date in linking_query:
#         valid_intervention_lookups[intervention_pk].append((participant_pk, intervention_date_date))
    
#     # A participant can't have more than one intervention date per intervention per schedule.
#     new_events = []
#     for relative_schedule in survey.relative_schedules.all():
#         for participant_pk, intervention_date_date in valid_intervention_lookups[relative_schedule.intervention_id]:
#             # This '+' is correct, 'days_after' is negative or 0 for days before and day of.
#             scheduled_date = intervention_date_date + timedelta(days=relative_schedule.days_after)
#             scheduled_time = relative_schedule.notification_time_from_intervention_date_and_timezone(scheduled_date, study_timezone)
#             if scheduled_time not in archive_scheduled_times_by_participant_pk[participant_pk]:
#                 new_events.append(ScheduledEvent(
#                     survey=survey,
#                     participant_id=participant_pk,
#                     weekly_schedule=None,
#                     relative_schedule=relative_schedule,
#                     absolute_schedule=None,
#                     scheduled_time=scheduled_time,
#                 ))
    
#     # The above code is untested, it was real hard to remove the N+1 database queries.
#     # for relative_schedule in survey.relative_schedules.all():
#     #     # get interventions date that have been marked (have a date) for valid participants.
#     #     intervention_dates_query = relative_schedule.intervention.intervention_dates.filter(
#     #         date__isnull=False,
#     #         participant_id__in=valid_participant_pks,
#     #     ).values_list("participant_id", "date")
#     #
#     #     for participant_pk, intervention_date in intervention_dates_query:
#     #         # This '+' is correct, 'days_after' is negative or 0 for days before and day of.
#     #         scheduled_date = intervention_date + timedelta(days=relative_schedule.days_after)
#     #         scheduled_time = relative_schedule.notification_time_from_intervention_date_and_timezone(
#     #             scheduled_date, study_timezone
#     #         )
#     #         if scheduled_time not in archive_scheduled_times_by_participant_pk[participant_pk]
#     #             new_events.append(ScheduledEvent(
#     #                 survey=survey,
#     #                 participant_id=participant_pk,
#     #                 weekly_schedule=None,
#     #                 relative_schedule=relative_schedule,
#     #                 absolute_schedule=None,
#     #                 scheduled_time=scheduled_time,
#     #             ))
    
#     info = ScheduledEvent.objects.bulk_create(new_events)
#     log(f"relative schedule events created {info}")
#     return bool(len(info))


#
## Untested Code that compares against archived events
#  This code was developed as part of a review of push notifications. The review found no
#  Issues, but this code may be advantageous in the future.
# NO IT WON'T DELETE IT

# def get_participant_ids_with_absolute_notification_history(schedule: AbsoluteSchedule) -> List[int]:
#     """ Notes:
#     1) ScheduledEvents for absolute schedules are created with the "canonical time" indicated by
#        the date and time on the AbsoluteSchedule object as calculated by the "event_time"
#        property, which returns a timezone aware datetime in the study's timezone at that time of
#        day.
#     2) When the Celery task checks for scheduled events it does the calculation for a full day
#        ahead of the current time, and skips participants for whom that participant-tz time has not
#        yet passed.
#     3) But, the creation of an ArchivedEvent uses the ScheduledEvent time as the target, eg. the
#        "canonical time", so we should always be able to find that events scheduled canonical time
#     4) ArchivedEvents point at a survey archives, so we need to get a list of survey archive db
#        ids and filter on that.
#     5) we already don't really support live studies shifting timezones mid study, so we can ignore
#        that case.
#     survey = schedule.survey
#     valid_survey_archive_ids = schedule.survey.archives.values_list("id", flat=True)
#     study = survey.study
#     study_timezone = study.timezone
#     scheduled_time = schedule.event_time
#     """
#     tz = schedule.study.timezone
#     return list(
#         ArchivedEvent.objects.filter(
#             scheduled_time=schedule.event_time(tz),
#             survey_archive_id__in=valid_survey_archive_ids,
#             schedule_type=ScheduleTypes.absolute,
#         ).exclude(
#             EXCLUDE_THESE_PARTICIPANTS_RELATED
#         ).values_list("participant_id", flat=True)
#     )


# def get_participant_ids_with_relative_notification_history(schedule: RelativeSchedule) -> List[int]:
#     """ Returns a list of participant database pks that need to have a notification sent base on a
#     relative schedule.
#     Notes:
#     1) ScheduledEvents for relative schedules have the same logical constraints as absolute
#        schedules, e.g. scheduled events will use the eventual calculated time.
#     2) Calculation of when to send a relative schedule notification is based on the presence of an
#        intervention date. This means that if the intervention date changes than relative schedules
#        will no longer match historical data in the ArchivedEvents, and the notifications will be
#        recalculated and become a new, unsent, notification.
#     3) But we can easily filter out participants that don't have the relevant intervention date
#        for this relative intervention populated.
#     4) The calculation time here is mostly in the timezone/datetime computation, which is up to
#        the number of participants total, and then pulling in the number of historical objects on
#        this survey, which is up to the number of relative schedules times the number of
#        participants.
#     """
#     valid_survey_archive_ids = schedule.survey.archives.values_list("id", flat=True)
#
#     potentially_valid_participants = list(schedule.intervention.intervention_dates.filter(
#         date__isnull=False,
#         # Subtle [Django?] behavior that I don't understand: you can't exclude null database values.
#         # This query in a .exclude will return instances where date is None, same for `date=None`:
#         #   intervention_dates_query.exclude(date__isnull=True, ...)
#     ).exclude(
#         EXCLUDE_THESE_PARTICIPANTS_RELATED
#     ).values_list("participant_id", "date", "participant__timezone_name"))
#
#     participant_ids_that_might_need_a_notification = [
#         participant_id for participant_id, _, _ in potentially_valid_participants
#     ]
#
#     # This code path needs to handle the case where there are multiple relative schedules for a survey.
#     # participants_and_dates = []
#     participants_to_calculated_times = defaultdict(list)
#     for participant_id, vention_date, timezone_name in potentially_valid_participants:
#         # 'days_after' is negative or 0 for days before and day of
#         schedule_time = schedule.notification_time_from_intervention_date_and_timezone(
#             vention_date + timedelta(days=schedule.days_after),  # computed date
#             gettz(timezone_name)  # timezone lookup based on a string is cached
#         )
#         participants_to_calculated_times[participant_id].append(schedule_time)
#     participants_to_calculated_times = dict(participants_to_calculated_times)  # convert to non-default-dict
#
#     # get participants with sent notifications on this survey due to a relative schedule on this survey
#     historical_event_participant_times = ArchivedEvent.objects.filter(
#         participant__in=participant_ids_that_might_need_a_notification,
#         survey_archive__in=valid_survey_archive_ids,
#         schedule_type=ScheduleTypes.relative,
#     ).values_list("participant_id", "scheduled_time")
#
#     # Rule: don't send duplicate notifications - it is possible for multiple relative schedules to
#     # calculate the same scheduled time for a survey notification - dumb but true. We want our
#     # return to only have one instance of each participant id.
#
#     # compare the historical data against the calculated times
#     participants_already_sent_this_notification = set()
#     for participant_id, historical_time in historical_event_participant_times:
#         if historical_time in participants_to_calculated_times[participant_id]:
#             participants_already_sent_this_notification.add(participant_id)
#
#     # then get only the participants that haven't been sent this notification
#     return list(
#         set(participant_ids_that_might_need_a_notification) -
#         participants_already_sent_this_notification
#     )

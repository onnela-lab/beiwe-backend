import json
from datetime import datetime, timedelta
from json import JSONDecodeError

import pytz
from django.utils.timezone import is_aware, is_naive, make_aware
from firebase_admin import (delete_app as delete_firebase_instance,
    get_app as get_firebase_app, initialize_app as initialize_firebase_app)
from firebase_admin.credentials import Certificate as FirebaseCertificate

from config.constants import (ANDROID_FIREBASE_CREDENTIALS, BACKEND_FIREBASE_CREDENTIALS,
    FIREBASE_APP_TEST_NAME, IOS_FIREBASE_CREDENTIALS)
from database.schedule_models import (AbsoluteSchedule, ArchivedEvent, ScheduledEvent,
    WeeklySchedule)
from database.survey_models import Survey
from database.system_models import FileAsText
from database.user_models import Participant


class FirebaseMisconfigured(Exception): pass
class NoSchedulesException(Exception): pass


def update_firebase_instance(credentials: str or None = None) -> None:
    """ Ensure that the current firebase credentials being used reflect the state of the
    database, including possibly removing the app if credentials have been removed. This function
    can be called at any point to verify that a firebase connection exists. """
    if credentials is None:
        credentials = FileAsText.objects.filter(tag=BACKEND_FIREBASE_CREDENTIALS).first()
        if credentials is None:  # no credentials passed in and none in the database
            try:
                delete_firebase_instance(get_firebase_app())
            except ValueError:
                # this occurs when the firebase app does not already exist, it can be safely ignored
                pass
            return
        credentials_changed = False
        credentials = credentials.text
    else:
        credentials_changed = True

    try:
        encoded_credentials = json.loads(credentials)
    except JSONDecodeError as e:
        raise FirebaseMisconfigured(e)

    try:
        initialize_firebase_app(FirebaseCertificate(encoded_credentials), name=FIREBASE_APP_TEST_NAME)
    except ValueError as e:
        raise FirebaseMisconfigured(e)
    # this should never error because it would have been escalated above
    delete_firebase_instance(get_firebase_app(name=FIREBASE_APP_TEST_NAME))

    try:
        delete_firebase_instance(get_firebase_app())
    except ValueError:
        # occurs when the firebase app does not already exist, it can be safely ignored
        pass

    # can now safely initialize the [possibly new] firebase app, and mutate the db if necessary.
    initialize_firebase_app(FirebaseCertificate(encoded_credentials))
    if credentials_changed:
        FileAsText.objects.filter(tag=BACKEND_FIREBASE_CREDENTIALS).delete()
        FileAsText.objects.create(tag=BACKEND_FIREBASE_CREDENTIALS, text=credentials)


def check_firebase_instance(require_android=False, require_ios=False) -> bool:
    """ Test the database state for the various creds. If creds are present test the firebase
    initialization process (update_firebase_instance). """
    active_creds = list(FileAsText.objects.filter(
        tag__in=[BACKEND_FIREBASE_CREDENTIALS, ANDROID_FIREBASE_CREDENTIALS, IOS_FIREBASE_CREDENTIALS]
    ).values_list("tag", flat=True))

    if (  # keep those parens.
            BACKEND_FIREBASE_CREDENTIALS not in active_creds
            or (require_android and ANDROID_FIREBASE_CREDENTIALS not in active_creds)
            or (require_ios and IOS_FIREBASE_CREDENTIALS not in active_creds)
    ):
        return False

    try:
        update_firebase_instance()
    except FirebaseMisconfigured as E:
        return False

    return True


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
    events = survey.scheduled_events.filter(relative_schedule=None, absolute_schedule=None)
    if participant is not None:
        events = events.filter(participant=participant)
        participant_ids = [participant.pk]
    else:
        participant_ids = survey.study.participants.values_list("pk", flat=True)
    events.delete()

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
    events = survey.scheduled_events.filter(relative_schedule=None, weekly_schedule=None)
    if participant is not None:
        events = events.filter(participant=participant)
    events.delete()

    new_events = []
    for schedule_pk, scheduled_time in survey.absolute_schedules.values_list("pk", "scheduled_date"):
        # if the schedule is somehow not tz-aware, force update it.
        if is_naive(scheduled_time):
            scheduled_time = make_aware(scheduled_time, survey.study.timezone)
            AbsoluteSchedule.objects.filter(pk=schedule_pk).update(scheduled_time=scheduled_time)

        # if one participant
        if participant is not None:
            archive_exists = ArchivedEvent.objects.filter(
                survey_archive__survey=survey,
                scheduled_time=scheduled_time,
                participant_id=participant.pk).exists()
            relevant_participants = [] if archive_exists else [participant.pk]

        # if many participants
        else:
            # don't create events for already sent notifications
            irrelevant_participants = ArchivedEvent.objects.filter(
                survey_archive__survey=survey, scheduled_time=scheduled_time,
            ).values_list("participant_id", flat=True)
            relevant_participants = survey.study.participants.exclude(
                pk__in=irrelevant_participants
            ).values_list("pk", flat=True)

        # populate
        for participant_id in relevant_participants:
            new_events.append(ScheduledEvent(
                survey=survey,
                weekly_schedule=None,
                relative_schedule=None,
                absolute_schedule_id=schedule_pk,
                scheduled_time=scheduled_time,
                participant_id=participant_id
            ))
    # instantiate
    ScheduledEvent.objects.bulk_create(new_events)


def repopulate_relative_survey_schedule_events(survey: Survey, participant: Participant = None) -> None:
    """ Creates new ScheduledEvents for the survey's RelativeSchedules while deleting the old
    ScheduledEvents related to the survey. """
    study_tz = survey.study.timezone or pytz.timezone("America/New_York")

    # Clear out existing events.
    # if the event is from an relative schedule, absolute and weekly schedules will be None
    events = survey.scheduled_events.filter(absolute_schedule=None, weekly_schedule=None)
    if participant is not None:
        events = events.filter(participant=participant)
    events.delete()

    # This is per schedule, and a participant can't have more than one intervention date per
    # intervention per schedule.  It is also per survey and all we really care about is
    # whether an event ever triggered on that survey.
    new_events = []
    for relative_schedule in survey.relative_schedules.all():
        # only interventions that have been marked, handle single user case, get data points.
        interventions_query = relative_schedule.intervention.intervention_dates.exclude(date=None)
        if participant is None:
            interventions_query = interventions_query.filter(participant=participant)
        interventions_query = interventions_query.values_list("participant_id", "date")

        for participant_id, intervention_date in interventions_query:
            scheduled_date = intervention_date + timedelta(days=relative_schedule.days_after)
            schedule_time = relative_schedule.scheduled_time(scheduled_date, study_tz)

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

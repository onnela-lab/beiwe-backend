from datetime import date, datetime, time, timedelta, tzinfo
from typing import List

from dateutil.tz import gettz
from django.core.paginator import Paginator
from django.core.validators import MaxValueValidator
from django.db import models
from django.utils.timezone import localtime, make_aware

from config.constants import DEV_TIME_FORMAT, ScheduleTypes
from database.common_models import TimestampedModel
from database.survey_models import Survey, SurveyArchive
from database.user_models import Participant
from libs.dev_utils import disambiguate_participant_survey, TxtClr


class AbsoluteSchedule(TimestampedModel):
    survey = models.ForeignKey('Survey', on_delete=models.CASCADE, related_name='absolute_schedules')
    date = models.DateField(null=False, blank=False)
    hour = models.PositiveIntegerField(validators=[MaxValueValidator(23)])
    minute = models.PositiveIntegerField(validators=[MaxValueValidator(59)])

    @property
    def event_time(self):
        return datetime(
            year=self.date.year,
            month=self.date.month,
            day=self.date.day,
            hour=self.hour,
            minute=self.minute,
            tzinfo=self.survey.study.timezone
        )

    @staticmethod
    def create_absolute_schedules(timings: List[List[int]], survey: Survey) -> bool:
        """ Creates new AbsoluteSchedule objects from a frontend-style list of dates and times"""
        survey.absolute_schedules.all().delete()

        if survey.deleted or not timings:
            return False

        duplicated = False
        for year, month, day, num_seconds in timings:
            _, created = AbsoluteSchedule.objects.get_or_create(
                survey=survey,
                date=date(year=year, month=month, day=day),
                hour=num_seconds // 3600,
                minute=num_seconds % 3600 // 60
            )
            if not created:
                duplicated = True

        return duplicated


class RelativeSchedule(TimestampedModel):
    survey = models.ForeignKey('Survey', on_delete=models.CASCADE, related_name='relative_schedules')
    intervention = models.ForeignKey('Intervention', on_delete=models.CASCADE, related_name='relative_schedules', null=True)
    days_after = models.IntegerField(default=0)
    # to be clear: these are absolute times of day, not offsets
    hour = models.PositiveIntegerField(validators=[MaxValueValidator(23)])
    minute = models.PositiveIntegerField(validators=[MaxValueValidator(59)])

    def scheduled_time(self, intervention_date: date, tz: tzinfo) -> datetime:
        # timezone should be determined externally and passed in

        # There is a small difference between applying the timezone via make_aware and
        # via the tzinfo keyword.  Make_aware seems less weird, so we use that one
        # example order is make_aware and then tzinfo, input was otherwise identical:
        # datetime.datetime(2020, 12, 5, 14, 30, tzinfo=<DstTzInfo 'America/New_York' EST-1 day, 19:00:00 STD>)
        # datetime.datetime(2020, 12, 5, 14, 30, tzinfo=<DstTzInfo 'America/New_York' LMT-1 day, 19:04:00 STD>)

        # the time of day (hour, minute) are not offsets, they are absolute.
        return make_aware(
            datetime.combine(intervention_date, time(self.hour, self.minute)), tz
        )

    @staticmethod
    def create_relative_schedules(timings: List[List[int]], survey: Survey) -> bool:
        """
        Creates new RelativeSchedule objects from a frontend-style list of interventions and times
        If you modify this please update create_relative_schedules_by_name in libs.copy_study too.
        """
        survey.relative_schedules.all().delete()
        if survey.deleted or not timings:
            return False

        duplicated = False
        for intervention_id, days_after, num_seconds in timings:
            # using get_or_create to catch duplicate schedules
            _, created = RelativeSchedule.objects.get_or_create(
                survey=survey,
                intervention=Intervention.objects.get(id=intervention_id),
                days_after=days_after,
                hour=num_seconds // 3600,
                minute=num_seconds % 3600 // 60,
            )
            if not created:
                duplicated = True

        return duplicated


class WeeklySchedule(TimestampedModel):
    """ Represents an instance of a time of day within a week for the weekly survey schedule.
        day_of_week is an integer, day 0 is Sunday.

        The timings schema mimics the Java.util.Calendar.DayOfWeek specification: it is zero-indexed
         with day 0 as Sunday."""

    survey = models.ForeignKey('Survey', on_delete=models.CASCADE, related_name='weekly_schedules')
    day_of_week = models.PositiveIntegerField(validators=[MaxValueValidator(6)])
    hour = models.PositiveIntegerField(validators=[MaxValueValidator(23)])
    minute = models.PositiveIntegerField(validators=[MaxValueValidator(59)])

    @staticmethod
    def create_weekly_schedules(timings: List[List[int]], survey: Survey) -> bool:
        """ Creates new WeeklySchedule objects from a frontend-style list of seconds into the day. """

        if survey.deleted or not timings:
            survey.weekly_schedules.all().delete()
            return False

        # asserts are not bypassed in production. Keep.
        if len(timings) != 7:
            raise Exception(
                f"Must have schedule for every day of the week, found {len(timings)} instead."
            )
        survey.weekly_schedules.all().delete()

        duplicated = False
        for day in range(7):
            for seconds in timings[day]:
                # should be all ints, use integer division.
                hour = seconds // 3600
                minute = seconds % 3600 // 60
                # using get_or_create to catch duplicate schedules
                _, created = WeeklySchedule.objects.get_or_create(
                    survey=survey, day_of_week=day, hour=hour, minute=minute
                )
                if not created:
                    duplicated = True

        return duplicated

    @classmethod
    def export_survey_timings(cls, survey: Survey) -> List[List[int]]:
        """Returns a json formatted list of weekly timings for use on the frontend"""
        # this weird sort order results in correctly ordered output.
        fields_ordered = ("hour", "minute", "day_of_week")
        timings = [[], [], [], [], [], [], []]
        schedule_components = WeeklySchedule.objects. \
            filter(survey=survey).order_by(*fields_ordered).values_list(*fields_ordered)

        # get, calculate, append, dump.
        for hour, minute, day in schedule_components:
            timings[day].append((hour * 60 * 60) + (minute * 60))
        return timings

    def get_prior_and_next_event_times(self, now: datetime) -> (datetime, datetime):
        """ Identify the start of the week relative to now, determine this week's push notification
        moment, then add 7 days. tzinfo of input is used to populate tzinfos of return. """
        today = now.date()

        # today.weekday defines Monday=0, in our schema Sunday=0 so we add 1
        start_of_this_week = today - timedelta(days=((today.weekday()+1) % 7))

        event_this_week = datetime(
            year=start_of_this_week.year,
            month=start_of_this_week.month,
            day=start_of_this_week.day,
            tzinfo=now.tzinfo,
        ) + timedelta(days=self.day_of_week, hours=self.hour, minutes=self.minute)
        event_next_week = event_this_week + timedelta(days=7)
        return event_this_week, event_next_week


class ScheduledEvent(TimestampedModel):
    survey = models.ForeignKey('Survey', on_delete=models.CASCADE, related_name='scheduled_events')
    participant = models.ForeignKey('Participant', on_delete=models.PROTECT, related_name='scheduled_events')
    weekly_schedule = models.ForeignKey('WeeklySchedule', on_delete=models.CASCADE, related_name='scheduled_events', null=True, blank=True)
    relative_schedule = models.ForeignKey('RelativeSchedule', on_delete=models.CASCADE, related_name='scheduled_events', null=True, blank=True)
    absolute_schedule = models.ForeignKey('AbsoluteSchedule', on_delete=models.CASCADE, related_name='scheduled_events', null=True, blank=True)
    scheduled_time = models.DateTimeField()

    # due to import complexity (needs those classes) this is the best place to stick the lookup dict.
    SCHEDULE_CLASS_LOOKUP = {
        ScheduleTypes.absolute: AbsoluteSchedule,
        ScheduleTypes.relative: RelativeSchedule,
        ScheduleTypes.weekly: WeeklySchedule,
        AbsoluteSchedule: ScheduleTypes.absolute,
        RelativeSchedule: ScheduleTypes.relative,
        WeeklySchedule: ScheduleTypes.weekly,
    }

    # This constraint is no longer necessary because de-duplication occurs when scheduled events are
    # generated. Additionally, in some corner cases, it causes a database error in relative
    # survey events where a survey is sent multiple times and multiple interventions exist such
    # that the event times overlap (eg, two interventions a day apart and two surveys at the same
    # time the day of and day after the intervention results in the same survey being scheduled
    # for a user twice at the same time
    #  class Meta:
    #     unique_together = ('survey', 'participant', 'scheduled_time',)

    def get_schedule_type(self):
        return self.SCHEDULE_CLASS_LOOKUP[self.get_schedule().__class__]

    def get_schedule(self):
        number_schedules = sum((
            self.weekly_schedule is not None,
            self.relative_schedule is not None,
            self.absolute_schedule is not None
        ))

        if number_schedules > 1:
            raise Exception(f"ScheduledEvent had {number_schedules} associated schedules.")

        if self.weekly_schedule:
            return self.weekly_schedule
        elif self.relative_schedule:
            return self.relative_schedule
        elif self.absolute_schedule:
            return self.absolute_schedule
        else:
            raise Exception("ScheduledEvent had no associated schedule")

    def archive(
            self, self_delete: bool, status: str, created_on: datetime = None,
    ):
        """ Create an ArchivedEvent from a ScheduledEvent. """
        # We need to handle the case of no-existing-survey-archive on the referenced survey,  Could
        # be cleaner, but there is an interaction with a  migration that will break; not worth it.
        try:
            survey_archive = self.survey.most_recent_archive()
        except SurveyArchive.DoesNotExist:
            self.survey.archive()
            survey_archive = self.survey.most_recent_archive()

        # Args, call, conditionally self-delete
        kwargs = dict(
            survey_archive=survey_archive,
            participant=self.participant,
            schedule_type=self.get_schedule_type(),
            scheduled_time=self.scheduled_time,
            status=status
        )
        if created_on:
            kwargs["created_on"] = created_on
        ArchivedEvent.objects.create(**kwargs)
        if self_delete:
            self.delete()

    @staticmethod
    @disambiguate_participant_survey
    def find_pending_events(
            participant: Participant = None, survey: Survey or str = None,
            tz: tzinfo = gettz('America/New_York'),
    ):
        """ THIS FUNCTION IS FOR DEBUGGING PURPOSES ONLY

        Throw in a participant and or survey object, OR THEIR IDENTIFYING STRING and we make it work
        'tz' will normalize timestamps to that timezone, default is us east.
        """
        # this is a simplified, modified version ofg the find_notification_events on ArchivedEvent.
        filters = {}
        if participant:
            filters['participant'] = participant
        if survey:
            filters["survey"] = survey
        elif participant:  # if no survey, yes participant:
            filters["survey__in"] = participant.study.surveys.all()

        query = ScheduledEvent.objects.filter(**filters).order_by(
            "survey__object_id", "participant__patient_id", "-scheduled_time"
        )
        survey_id = ""
        for a in query:
            # only print participant name and survey id when it changes
            if a.survey.object_id != survey_id:
                print(f"{a.survey.survey_type} {TxtClr.CYAN}{a.survey.object_id}{TxtClr.BLACK}:")
                survey_id = a.survey.object_id

            # data points of interest for sending information
            sched_time = localtime(a.scheduled_time, tz)
            sched_time_print = datetime.strftime(sched_time, DEV_TIME_FORMAT)
            print(
                f"  {a.get_schedule_type()} FOR {TxtClr.CYAN}{a.participant.patient_id}{TxtClr.BLACK}"
                f" AT {TxtClr.GREEN}{sched_time_print}{TxtClr.BLACK}",
            )


# TODO there is no code that updates the response_time field.  That should be rolled into the
#  check-for-downloads as an optional parameter passed in.  If it doesn't get hit then there is
#  no guarantee that the app checked in.
class ArchivedEvent(TimestampedModel):
    SUCCESS = "success"

    survey_archive = models.ForeignKey('SurveyArchive', on_delete=models.PROTECT, related_name='archived_events', db_index=True)
    participant = models.ForeignKey('Participant', on_delete=models.PROTECT, related_name='archived_events', db_index=True)
    schedule_type = models.CharField(max_length=32, db_index=True)
    scheduled_time = models.DateTimeField(db_index=True)
    response_time = models.DateTimeField(null=True, blank=True, db_index=True)
    status = models.TextField(null=False, blank=False, db_index=True)

    @property
    def survey(self):
        return self.survey_archive.survey

    @classmethod
    def get_values_for_notification_history(cls, participant_id):
        return cls.objects\
            .filter(participant_id=participant_id)\
            .order_by('-created_on')\
            .annotate(survey_id=models.F('survey_archive__survey'),
                      survey_version=models.F('survey_archive__archive_start'))\
            .values('scheduled_time', 'created_on', 'survey_id', 'survey_version', 'schedule_type', 'status')

    @classmethod
    def get_values_for_most_recent_notification(cls, participant_id):
        return cls.get_values_for_notification_history(participant_id).first()

    @classmethod
    def get_values_for_notification_history_paginated(cls, participant_id, per_page=100):
        return Paginator(cls.get_values_for_notification_history(participant_id), per_page)

    @staticmethod
    @disambiguate_participant_survey
    def find_notification_events(
            participant: Participant = None, survey: Survey or str = None, schedule_type: str = None,
            tz: tzinfo = gettz('America/New_York'), flat=False
    ):
        """ THIS FUNCTION IS FOR DEBUGGING PURPOSES ONLY

        Throw in a participant and or survey object, OR THEIR IDENTIFYING STRING and we make it work

        'survey_type'  will filter by survey type, duh.
        'flat'         disables alternating line colors.
        'tz'           will normalize timestamps to that timezone, default is us east.
        """
        filters = {}
        if participant:
            filters['participant'] = participant
        if schedule_type:
            filters["schedule_type"] = schedule_type
        if survey:
            filters["survey_archive__survey"] = survey
        elif participant:  # if no survey, yes participant:
            filters["survey_archive__survey__in"] = participant.study.surveys.all()

        # order by participant to separate out the core related events, then order by survey
        # to group the participant's related events together, and do this in order of most recent
        # at the top of all sub-lists.
        query = ArchivedEvent.objects.filter(**filters).order_by(
            "participant__patient_id", "survey_archive__survey__object_id", "-created_on"
        )

        print(f"There were {query.count()} sent scheduled events matching your query.")
        participant_name = ""
        survey_id = ""
        for a in query:
            # only print participant name and survey id when it changes
            if a.participant.patient_id != participant_name:
                print(f"\nparticipant {TxtClr.CYAN}{a.participant.patient_id}{TxtClr.BLACK}:")
                participant_name = a.participant.patient_id
            if a.survey.object_id != survey_id:
                print(f"{a.survey.survey_type} {TxtClr.CYAN}{a.survey.object_id}{TxtClr.BLACK}:")
                survey_id = a.survey.object_id

            # data points of interest for sending information
            sched_time = localtime(a.scheduled_time, tz)
            sent_time = localtime(a.created_on, tz)
            time_diff_minutes = (sent_time - sched_time).total_seconds() / 60
            sched_time_print = datetime.strftime(sched_time, DEV_TIME_FORMAT)
            sent_time_print = datetime.strftime(sent_time, DEV_TIME_FORMAT)

            print(
                f"  {a.schedule_type} FOR {TxtClr.GREEN}{sched_time_print}{TxtClr.BLACK} "
                f"SENT {TxtClr.GREEN}{sent_time_print}{TxtClr.BLACK}  "
                f"\u0394 of {time_diff_minutes:.1f} min",
                end="",
                # \u0394 is the delta character
            )

            if a.status == ArchivedEvent.SUCCESS:
                print(f'  status: "{TxtClr.GREEN}{a.status}{TxtClr.BLACK}"')
            else:
                print(f'  status: "{TxtClr.YELLOW}{a.status}{TxtClr.BLACK}"')

            if not flat:
                # these lines get hard to read, color helps, we can alternate brightness like this!
                TxtClr.brightness_swap()


class Intervention(TimestampedModel):
    name = models.TextField()
    study = models.ForeignKey('Study', on_delete=models.PROTECT, related_name='interventions')


class InterventionDate(TimestampedModel):
    date = models.DateField(null=True, blank=True)
    participant = models.ForeignKey('Participant', on_delete=models.CASCADE, related_name='intervention_dates')
    intervention = models.ForeignKey('Intervention', on_delete=models.CASCADE, related_name='intervention_dates')

    class Meta:
        unique_together = ('participant', 'intervention',)

from __future__ import annotations

import uuid
from datetime import date, datetime, time, timedelta, tzinfo
from typing import List, Tuple

from django.core.validators import MaxValueValidator
from django.db import models
from django.db.models import Manager
from django.utils.timezone import make_aware

from constants.message_strings import MESSAGE_SEND_SUCCESS
from constants.schedule_constants import ScheduleTypes
from database.common_models import TimestampedModel
from database.survey_models import Survey, SurveyArchive


# this is an import hack to improve IDE assistance
try:
    from database.models import Participant, Study
except ImportError:
    pass


class BadWeeklyCount(Exception): pass
InterventionPK = int
DaysAfter = int
Year = int
Month = int
Day = int
SecondsIntoDay = int

class AbsoluteSchedule(TimestampedModel):
    survey: Survey = models.ForeignKey('Survey', on_delete=models.CASCADE, related_name='absolute_schedules')
    date = models.DateField(null=False, blank=False)
    hour = models.PositiveIntegerField(validators=[MaxValueValidator(23)])
    minute = models.PositiveIntegerField(validators=[MaxValueValidator(59)])
    
    # related field typings (IDE halp)
    scheduled_events: Manager[ScheduledEvent]
    
    def event_time(self, tz: tzinfo) -> datetime:
        """ Expects the study timezone's tzinfo. """
        return datetime(
            year=self.date.year,
            month=self.date.month,
            day=self.date.day,
            hour=self.hour,
            minute=self.minute,
            tzinfo=tz,
        )
    
    @staticmethod
    def configure_absolute_schedules(timings: List[Tuple[Year, Month, Day, SecondsIntoDay]], survey: Survey):
        """ Creates and deletes AbsoluteSchedules for a survey to match the frontend's absolute
        schedule timings, a list of year, month, day, seconds-into-the-day defining a time. """
        
        if survey.deleted or not timings:
            survey.absolute_schedules.all().delete()
            return False
        
        valid_pks = []
        for year, month, day, num_seconds in timings:
            instance, _ = AbsoluteSchedule.objects.get_or_create(
                survey=survey,
                date=date(year=year, month=month, day=day),
                hour=num_seconds // 3600,
                minute=num_seconds % 3600 // 60
            )
            valid_pks.append(instance.pk)
        survey.absolute_schedules.exclude(id__in=valid_pks).delete()


class RelativeSchedule(TimestampedModel):
    survey: Survey = models.ForeignKey('Survey', on_delete=models.CASCADE, related_name='relative_schedules')
    intervention: Intervention = models.ForeignKey('Intervention', on_delete=models.CASCADE, related_name='relative_schedules', null=True)
    days_after = models.IntegerField(default=0)
    # to be clear: these are absolute times of day, not offsets
    hour = models.PositiveIntegerField(validators=[MaxValueValidator(23)])
    minute = models.PositiveIntegerField(validators=[MaxValueValidator(59)])
    
    # related field typings (IDE halp)
    scheduled_events: Manager[ScheduledEvent]
    
    def notification_time_from_intervention_date_and_timezone(self, a_date: date, tz: tzinfo) -> datetime:
        """ TIMEZONE SHOULD BE THE STUDY TIMEZONE. The timezone is used to determine the "canonical
        time" of the ScheduledEvent, which is shifted to the participant timezone. """
        # The time of day (hour, minute) are not offsets, they are absolute times of day.
        # doing self.study.timezone here is a database query so don't do that
        # make_aware handles shifting ambiguous times so the code doesn't crash.
        return make_aware(datetime.combine(a_date, time(self.hour, self.minute)), tz)
    
    @staticmethod
    def configure_relative_schedules(timings: List[Tuple[InterventionPK, DaysAfter, SecondsIntoDay]], survey: Survey):
        """ Creates and deletes RelativeSchedules for a survey to match the frontend's relative
        schedule input timings, the pk of the relevant intervention to target, the number of days
        after, and the number of seconds into that day. """
        
        if survey.deleted or not timings:
            survey.relative_schedules.all().delete()
            return False
            
        valid_pks: List[int] = []
        for intervention_pk, days_after, num_seconds in timings:
            # using get_or_create to catch duplicate schedules
            instance, _ = RelativeSchedule.objects.get_or_create(
                survey=survey,
                intervention_id=intervention_pk,
                days_after=days_after,
                hour=num_seconds // 3600,
                minute=num_seconds % 3600 // 60,
            )
            valid_pks.append(instance.pk)
        survey.relative_schedules.exclude(id__in=valid_pks).delete()


class WeeklySchedule(TimestampedModel):
    """ Represents an instance of a time of day within a week for the weekly survey schedule.
        day_of_week is an integer, day 0 is Sunday.
        
        The timings schema mimics the Java.util.Calendar.DayOfWeek specification: it is zero-indexed
        with day 0 as Sunday. """
    
    survey: Survey = models.ForeignKey('Survey', on_delete=models.CASCADE, related_name='weekly_schedules')
    day_of_week = models.PositiveIntegerField(validators=[MaxValueValidator(6)])
    hour = models.PositiveIntegerField(validators=[MaxValueValidator(23)])
    minute = models.PositiveIntegerField(validators=[MaxValueValidator(59)])
    
    # related field typings (IDE halp)
    scheduled_events: Manager[ScheduledEvent]
    
    @staticmethod
    def configure_weekly_schedules(timings: List[List[int]], survey: Survey):
        """ Creates and deletes WeeklySchedule for a survey to match the input timings. """
        if survey.deleted or not timings:
            survey.weekly_schedules.all().delete()
            return False
        
        # asserts are not bypassed in production. Keep.
        if len(timings) != 7:
            raise BadWeeklyCount(
                f"Must have schedule for every day of the week, found {len(timings)} instead."
            )
        
        valid_pks: List[int] = []
        for day in range(7):
            for seconds in timings[day]:
                # should be all ints, use integer division.
                hour = seconds // 3600
                minute = seconds % 3600 // 60
                instance, created = WeeklySchedule.objects.get_or_create(
                    survey=survey, day_of_week=day, hour=hour, minute=minute
                )
                valid_pks.append(instance.pk)
        survey.weekly_schedules.exclude(id__in=valid_pks).delete()
    
    @classmethod
    def export_survey_timings(cls, survey: Survey) -> List[List[int]]:
        """Returns a json formatted list of weekly timings for use on the frontend"""
        # this weird sort order results in correctly ordered output.
        fields_ordered = ("hour", "minute", "day_of_week")
        timings = [[], [], [], [], [], [], []]
        schedule_components = WeeklySchedule.objects. \
            filter(survey=survey).order_by(*fields_ordered).values_list(*fields_ordered)
        
        # get, calculate, append, dump. -- day 0 is monday, day 6 is sunday
        for hour, minute, day in schedule_components:
            timings[day].append((hour * 60 * 60) + (minute * 60))
        return timings
    
    def get_prior_and_next_event_times(self, now: datetime) -> Tuple[datetime, datetime]:
        """ Identify the start of the week relative to now, determine this week's push notification
        moment, then add 7 days. tzinfo of input is used to populate tzinfos of return. """
        today = now.date()
        
        # today.weekday defines Monday=0, in our schema Sunday=0 so we add 1
        start_of_this_week = today - timedelta(days=((today.weekday()+1) % 7))
        
        event_this_week = datetime(
            year=start_of_this_week.year,
            month=start_of_this_week.month,
            day=start_of_this_week.day,
            tzinfo=self.survey.study.timezone,
        ) + timedelta(days=self.day_of_week, hours=self.hour, minutes=self.minute)
        event_next_week = event_this_week + timedelta(days=7)
        return event_this_week, event_next_week


class ScheduledEvent(TimestampedModel):
    survey: Survey = models.ForeignKey('Survey', on_delete=models.CASCADE, related_name='scheduled_events')
    participant: Participant = models.ForeignKey('Participant', on_delete=models.PROTECT, related_name='scheduled_events')
    weekly_schedule: WeeklySchedule = models.ForeignKey('WeeklySchedule', on_delete=models.CASCADE, related_name='scheduled_events', null=True, blank=True)
    relative_schedule: RelativeSchedule = models.ForeignKey('RelativeSchedule', on_delete=models.CASCADE, related_name='scheduled_events', null=True, blank=True)
    absolute_schedule: AbsoluteSchedule = models.ForeignKey('AbsoluteSchedule', on_delete=models.CASCADE, related_name='scheduled_events', null=True, blank=True)
    scheduled_time = models.DateTimeField()
    deleted = models.BooleanField(null=False, default=False, db_index=True)
    uuid = models.UUIDField(null=True, blank=True, db_index=True, unique=True, default=uuid.uuid4)  # see ArchivedEvent
    most_recent_event: ArchivedEvent = models.ForeignKey("ArchivedEvent", on_delete=models.DO_NOTHING, null=True, blank=True)
    no_resend = models.BooleanField(default=False, null=False)
    
    # due to import complexity (needs those classes) this is the best place to stick the lookup dict.
    SCHEDULE_CLASS_LOOKUP = {
        ScheduleTypes.absolute: AbsoluteSchedule,
        ScheduleTypes.relative: RelativeSchedule,
        ScheduleTypes.weekly: WeeklySchedule,
        AbsoluteSchedule: ScheduleTypes.absolute,
        RelativeSchedule: ScheduleTypes.relative,
        WeeklySchedule: ScheduleTypes.weekly,
    }
    
    @property
    def scheduled_time_in_canonical_form(self) -> datetime:
        # canonical form is the study timezone, that should match the time of day on the survey editor
        return self.scheduled_time.astimezone(self.survey.study.timezone)
    
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
            raise TypeError("ScheduledEvent had no associated schedule")
    
    def get_schedule_pk(self):
        if self.weekly_schedule_id:
            return self.weekly_schedule_id
        elif self.relative_schedule_id:
            return self.relative_schedule_id
        elif self.absolute_schedule_id:
            return self.absolute_schedule_id
        else:
            # TypeError because we hav lost the tye of schedule this is....? sure
            raise TypeError("ScheduledEvent had no associated schedule")
        
    def archive(
        self,
        participant: Participant,
        status: str,
    ):
        """ Create an ArchivedEvent from a ScheduledEvent. """
        ## Hot code path, Participant is passed in here to avoid a database call.
        survey_archive_pk = self.survey.most_recent_archive_pk()  
        the_uuid = self.uuid if participant.can_handle_push_notification_resends else None
        
        # create ArchivedEvent, link to most_recent_event, conditionally mark self as deleted.
        archive = ArchivedEvent(
            survey_archive_id=survey_archive_pk,
            participant=self.participant,
            schedule_type=self.get_schedule_type(),
            scheduled_time=self.scheduled_time,
            status=status,
            uuid=the_uuid,
            was_resend=self.most_recent_event is not None,
        )
        archive.save()
        
        # mark self as deleted on success.
        self.update(most_recent_event=archive, deleted=status == MESSAGE_SEND_SUCCESS)
    
    def __str__(self):
        t = "Manual"
        if self.weekly_schedule_id:
            t = "Weekly"
        if self.relative_schedule_id:
            t = "Relative"
        if self.absolute_schedule_id:
            t = "Absolute"
        # comes out as <ScheduledEvent: Relative>
        return t


class ArchivedEvent(TimestampedModel):
    # The survey archive cannot point to schedule objects because schedule objects can be deleted
    # (not just marked as deleted)
    survey_archive: SurveyArchive = models.ForeignKey('SurveyArchive', on_delete=models.PROTECT, related_name='archived_events', db_index=True)
    participant: Participant = models.ForeignKey('Participant', on_delete=models.PROTECT, related_name='archived_events', db_index=True)
    schedule_type = models.CharField(null=True, blank=True, max_length=32, db_index=True)
    scheduled_time = models.DateTimeField(null=True, blank=True, db_index=True)
    status = models.TextField(null=False, blank=False, db_index=True)
    uuid = models.UUIDField(null=True, blank=True, db_index=True)  # see comment below field listing
    confirmed_received = models.BooleanField(default=False, db_index=True, null=True)
    was_resend = models.BooleanField(default=False, null=False)
    
    # The uuid field cannot have not-null or unique constraints because there is behavior that
    # depends on those value. We are using uuids to connect ArchivedEvents to ScheduledEvents, and
    # to identify groups of ArchivedEvents that went out in the same single notification.
    
    @property
    def survey(self) -> Survey:
        return self.survey_archive.survey


class Intervention(TimestampedModel):
    name = models.TextField()
    study: Study = models.ForeignKey('Study', on_delete=models.PROTECT, related_name='interventions')
    
    # related field typings (IDE halp)
    intervention_dates: Manager[InterventionDate]
    relative_schedules: Manager[RelativeSchedule]


class InterventionDate(TimestampedModel):
    date = models.DateField(null=True, blank=True)
    participant: Participant = models.ForeignKey('Participant', on_delete=models.CASCADE, related_name='intervention_dates')
    intervention: Intervention = models.ForeignKey('Intervention', on_delete=models.CASCADE, related_name='intervention_dates')
    
    class Meta:
        unique_together = ('participant', 'intervention', )

import json
from datetime import datetime, time, timedelta
from typing import List

import pytz
from django.core.validators import MaxValueValidator
from django.db import models
from django.utils.timezone import is_naive, make_aware

from config.constants import ScheduleTypes
from database.common_models import TimestampedModel
from database.survey_models import Survey, SurveyArchive


class AbsoluteSchedule(TimestampedModel):
    survey = models.ForeignKey('Survey', on_delete=models.CASCADE, related_name='absolute_schedules')
    scheduled_date = models.DateTimeField()

    @staticmethod
    def create_absolute_schedules(timings: List[List[int]], survey: Survey) -> bool:
        """ Creates new AbsoluteSchedule objects from a frontend-style list of dates and times"""
        if not timings:
            return False

        survey.absolute_schedules.all().delete()
        duplicated = False
        for year, month, day, num_seconds in timings:
            hour = num_seconds // 3600
            minute = num_seconds % 3600 // 60
            schedule_date = datetime(year=year, month=month, day=day, hour=hour, minute=minute)
            # using get_or_create to catch duplicate schedules
            _, created = AbsoluteSchedule.objects.get_or_create(survey=survey, scheduled_date=schedule_date)
            if not created:
                duplicated = True

        return duplicated

    # TODO: delete? not used anywhere
    def create_events(self) -> None:
        new_events = []
        for participant in self.survey.study.participants.all():
            new_events.append(ScheduledEvent(
                survey=self.survey,
                participant=participant,
                weekly_schedule=None,
                relative_schedule=None,
                absolute_schedule=self,
                scheduled_time=self.scheduled_date,
            ))
        ScheduledEvent.objects.bulk_create(new_events)


class RelativeSchedule(TimestampedModel):
    survey = models.ForeignKey('Survey', on_delete=models.CASCADE, related_name='relative_schedules')
    intervention = models.ForeignKey('Intervention', on_delete=models.CASCADE, related_name='relative_schedules', null=True)
    days_after = models.IntegerField(default=0)
    hour = models.PositiveIntegerField(validators=[MaxValueValidator(23)])
    minute = models.PositiveIntegerField(validators=[MaxValueValidator(59)])

    # TODO: delete? not used anywhere
    def create_events(self):
        for participant in self.survey.study.participants.all():
            scheduled_time = datetime.combine(
                self.intervention.intervention_dates.get(participant=participant).date,
                time(self.hour, self.minute)
            )
            ScheduledEvent.objects.create(
                survey=self.survey,
                participant=participant,
                weekly_schedule=None,
                relative_schedule=self,
                absolute_schedule=None,
                scheduled_time=scheduled_time,
            )

    @staticmethod
    def create_relative_schedules(timings: List[List[int]], survey: Survey) -> bool:
        """
        Creates new RelativeSchedule objects from a frontend-style list of interventions and times
        """
        if not timings:
            return False

        survey.relative_schedules.all().delete()
        duplicated = False
        # should be all ints
        for intervention_id, days_after, num_seconds in timings:
            hour = num_seconds // 3600
            minute = num_seconds % 3600 // 60
            # using get_or_create to catch duplicate schedules
            _, created = RelativeSchedule.objects.get_or_create(
                survey=survey,
                intervention=Intervention.objects.get(id=intervention_id),
                days_after=days_after,
                hour=hour,
                minute=minute,
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
    def create_weekly_schedules_from_json(timings: str, survey: Survey) -> None:
        WeeklySchedule.create_weekly_schedules(json.loads(timings), survey)

    @staticmethod
    def create_weekly_schedules(timings: List[List[int]], survey: Survey) -> bool:
        """ Creates new WeeklySchedule objects from a frontend-style list of seconds into the day. """
        if not timings:
            return False
        
        assert len(timings) == 7
        survey.weekly_schedules.all().delete()
        duplicated = False
        for day in range(7):
            for seconds in timings[day]:
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
        # this sort order results in nicely ordered output.
        fields_ordered = ("hour", "minute", "day_of_week")
        timings = [[], [], [], [], [], [], []]
        schedule_components = WeeklySchedule.objects.\
            filter(survey=survey).order_by(*fields_ordered).values_list(*fields_ordered)

        # get, calculate, append, dump.
        for hour, minute, day in schedule_components:
            timings[day].append((hour * 60 * 60) + (minute * 60))
        return timings

    def get_prior_and_next_event_times(self, now: datetime=None) -> (datetime, datetime):
        """ Identify the start of the week relative to the current time, use that to determine this
        week's (past or present) push notification event time, and the same event for next week.
        If now is passed in it must have a UTC timezone. """

        if now is None:
            # handle case of utc date not matching date of local time.
            today = make_aware(datetime.utcnow(), timezone=pytz.utc).date()
        elif isinstance(now, datetime) and not is_naive(now) and now.tzinfo.zone == "UTC":
            # now must be a datetime with a timezone of UTC
            today = now.date()
        else:
            raise TypeError(f"Datetime must be UTC and timezone aware, received {str(now)}")

        # today.weekday defines Monday=0, in our schema Sunday=0 so we add 1
        start_of_this_week = today - timedelta(days=((today.weekday()+1) % 7))

        event_this_week = make_aware(
                datetime(
                    year=start_of_this_week.year,
                    month=start_of_this_week.month,
                    day=start_of_this_week.day,
                ) +
                timedelta(
                    days=self.day_of_week,
                    hours=self.hour,
                    minutes=self.minute,
                ),
                timezone=pytz.utc,
        )
        event_next_week = event_this_week + timedelta(days=7)
        return event_this_week, event_next_week


class ScheduledEvent(TimestampedModel):
    survey = models.ForeignKey('Survey', on_delete=models.CASCADE, related_name='scheduled_events')
    participant = models.ForeignKey('Participant', on_delete=models.PROTECT, related_name='scheduled_events')
    weekly_schedule = models.ForeignKey('WeeklySchedule', on_delete=models.CASCADE, related_name='scheduled_events', null=True, blank=True)
    relative_schedule = models.ForeignKey('RelativeSchedule', on_delete=models.CASCADE, related_name='scheduled_events', null=True, blank=True)
    absolute_schedule = models.ForeignKey('AbsoluteSchedule', on_delete=models.CASCADE, related_name='scheduled_events', null=True, blank=True)
    scheduled_time = models.DateTimeField()

    # due to import complexity right here this is the best place to stick this
    SCHEDULE_CLASS_LOOKUP = {
        ScheduleTypes.absolute: AbsoluteSchedule,
        ScheduleTypes.relative: RelativeSchedule,
        ScheduleTypes.weekly: WeeklySchedule,
        AbsoluteSchedule: ScheduleTypes.absolute,
        RelativeSchedule: ScheduleTypes.relative,
        WeeklySchedule: ScheduleTypes.weekly,
    }

    class Meta:
        unique_together = ('survey', 'participant', 'scheduled_time',)

    def get_schedule_type(self):
        return self.SCHEDULE_CLASS_LOOKUP[self.get_schedule().__class__]

    def get_schedule(self):
        number_schedules = sum((
            self.weekly_schedule is not None, self.relative_schedule is not None,
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

    def archive(self):
        # for stupid reasons involving the legacy mechanism for creating a survey archive we need
        # to handle the case where the object does not exist.
        try:
            survey_archive = self.survey.most_recent_archive()
        except SurveyArchive.DoesNotExist:
            self.survey.archive()  # force create a survey archive
            survey_archive = self.survey.most_recent_archive()

        ArchivedEvent.objects.create(
            survey_archive=survey_archive,
            participant=self.participant,
            schedule_type=self.get_schedule_type(),
            scheduled_time=self.scheduled_time,
        ).save()
        self.delete()


class ArchivedEvent(TimestampedModel):
    survey_archive = models.ForeignKey('SurveyArchive', on_delete=models.PROTECT, related_name='archived_events')
    participant = models.ForeignKey('Participant', on_delete=models.PROTECT, related_name='archived_events')
    schedule_type = models.CharField(max_length=32)
    scheduled_time = models.DateTimeField()
    response_time = models.DateTimeField(null=True)


# TODO update endpoint to update response_time
class Intervention(models.Model):
    name = models.TextField()
    study = models.ForeignKey('Study', on_delete=models.PROTECT, related_name='interventions')


class InterventionDate(models.Model):
    date = models.DateField(null=True)
    participant = models.ForeignKey('Participant', on_delete=models.CASCADE, related_name='intervention_dates')
    intervention = models.ForeignKey('Intervention', on_delete=models.CASCADE, related_name='intervention_dates')

    class Meta:
        unique_together = ('participant', 'intervention',)

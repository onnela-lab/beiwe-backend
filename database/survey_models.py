from __future__ import annotations

import json
from typing import List, Tuple

from django.db import models
from django.db.models import Manager, QuerySet

from constants.study_constants import AUDIO_SURVEY_SETTINGS
from database.common_models import JSONTextField, ObjectIDModel, TimestampedModel
from database.validators import LengthValidator


# this is an import hack to improve IDE assistance
try:
    from database.models import (AbsoluteSchedule, ArchivedEvent, ChunkRegistry, RelativeSchedule,
        ScheduledEvent, Study, WeeklySchedule)
except ImportError:
    pass


class SurveyBase(TimestampedModel, ObjectIDModel):
    """ SurveyBase contains all fields that we want to have copied into a survey backup whenever
    it is updated. (used in surveys and survey archives.) """
    
    AUDIO_SURVEY = 'audio_survey'
    TRACKING_SURVEY = 'tracking_survey'
    SURVEY_TYPE_CHOICES = (
        (AUDIO_SURVEY, AUDIO_SURVEY),
        (TRACKING_SURVEY, TRACKING_SURVEY),
    )
    
    # for some reasons these nee to have diverged
    SURVEY_EXPORT_FIELDS = ["content", "survey_type", "settings", "object_id", "name", "id"]
    SURVEY_DEVICE_EXPORT_FIELDS = ["content", "survey_type", "settings", "object_id", "name"]
    
    content = JSONTextField(default='[]', help_text='JSON blob containing information about the survey questions.')
    survey_type = models.CharField(max_length=16, choices=SURVEY_TYPE_CHOICES, help_text='What type of survey this is.')
    settings = JSONTextField(default='{}', help_text='JSON blob containing settings for the survey.')
    
    deleted = models.BooleanField(default=False)
    
    class Meta:
        abstract = True


class Survey(SurveyBase):
    """ Surveys contain all information the app needs to display the survey correctly to a
    participant, and when it should push the notifications to take the survey.
    
    Surveys must have a 'survey_type', which is a string declaring the type of survey it contains,
    which the app uses to display the correct interface.
    
    Surveys contain 'content', which is a JSON blob that is unpacked on the app and displayed to the
    participant in the form indicated by the survey_type.
    
    Timings schema: a survey must indicate the day of week and time of day on which to trigger; by
    default it contains no values. The timings schema mimics the Java.util.Calendar.DayOfWeek
    specification: it is zero-indexed with day 0 as Sunday. 'timings' is a list of 7 lists, each
    inner list containing any number of times of the day. Times of day are integer values indicating
    the number of seconds past midnight.
    
    Inherits the following fields from SurveyBase content survey_type settings timings """
    
    # This is required for file name and path generation
    object_id = models.CharField(max_length=24, unique=True, validators=[LengthValidator(24)])
    # the study field is not inherited because we need to change its related name
    study: Study = models.ForeignKey('Study', on_delete=models.PROTECT, related_name='surveys')
    name = models.TextField(blank=True, null=False, default="")
    
    # related field typings (IDE halp)
    absolute_schedules: Manager[AbsoluteSchedule]
    archives: Manager[SurveyArchive]
    chunk_registries: Manager[ChunkRegistry]
    relative_schedules: Manager[RelativeSchedule]
    scheduled_events: Manager[ScheduledEvent]
    weekly_schedules: Manager[WeeklySchedule]
    
    @classmethod
    def create_with_object_id(cls, **kwargs) -> Survey:
        object_id = cls.generate_objectid_string("object_id")
        survey = cls.objects.create(object_id=object_id, **kwargs)
        return survey
    
    @classmethod
    def create_with_settings(cls, survey_type: str, **kwargs) -> Survey:
        """ Create a new Survey with the provided survey type and attached to the given Study, as
        well as any other given keyword arguments. If the Survey is audio and no other
        settings are given, give it the default audio survey settings. """
        if survey_type == cls.AUDIO_SURVEY and 'settings' not in kwargs:
            kwargs['settings'] = json.dumps(AUDIO_SURVEY_SETTINGS)
        
        survey = cls.create_with_object_id(survey_type=survey_type, **kwargs)
        return survey
    
    def weekly_timings(self) -> List[List[int]]:
        """ Returns a json serializable object that represents the weekly schedules of this survey.
        The return object is a list of 7 lists of ints """
        from libs.schedules import export_weekly_survey_timings
        return export_weekly_survey_timings(self)
    
    def relative_timings(self) -> List[Tuple[int, int, int]]:
        """ Returns a json serializable object that represents the relative schedules of the survey
        The return object is a list of lists of intervention ids, days offset, and seconds offset. """
        schedules = []
        for rel_sched in self.relative_schedules.all():
            num_seconds = rel_sched.minute * 60 + rel_sched.hour * 3600
            schedules.append((rel_sched.intervention.id, rel_sched.days_after, num_seconds))
        return schedules
    
    def relative_timings_by_name(self) -> List[Tuple[str, int, int]]:
        """ Returns a json serializable object that represents the relative schedules of the survey
        The return object is a list of lists of intervention names, days offset, and seconds offset. """
        schedules = []
        for rel_sched in self.relative_schedules.all():
            num_seconds = rel_sched.minute * 60 + rel_sched.hour * 3600
            schedules.append((rel_sched.intervention.name, rel_sched.days_after, num_seconds))
        return schedules
    
    def absolute_timings(self) -> List[Tuple[int, int, int, int]]:
        """ Returns a json serializable object that represents the absolute schedules of the survey
        The return object is a list of lists of the year, month, day and seconds within the day. """
        schedules = []
        tz = self.study.timezone
        for abs_sched in self.absolute_schedules.all():
            event_time = abs_sched.event_time(tz)
            num_seconds = event_time.minute * 60 + event_time.hour * 3600
            schedules.append((event_time.year, event_time.month, event_time.day, num_seconds))
        return schedules
    
    def notification_events(self, **archived_event_filter_kwargs) -> QuerySet[ArchivedEvent]:
        from database.schedule_models import ArchivedEvent
        return ArchivedEvent.objects.filter(
            survey_archive_id__in=self.archives.values_list("id", flat=True)
        ).filter(**archived_event_filter_kwargs).order_by("-scheduled_time")
    
    def save(self, *args, **kwargs):
        super().save(*args, **kwargs)
        self.archive()
    
    def most_recent_archive_pk(self) -> int:
        # This is in a hot path, this saves milliseconds, whatever.
        pk = self.archives.order_by("-archive_start").values_list("pk", flat=True).first()
        if pk is None:
            return self.archive().pk
        return pk
    
    def most_recent_archive(self) -> SurveyArchive:
        # handle case where no archives exist
        try:
            return self.archives.latest('archive_start')
        except SurveyArchive.DoesNotExist:
            return self.archive()
    
    def archive(self) -> SurveyArchive:
        """ Create an archive if there were any changes to the data since the last archive was
        created, or if no archive exists. """
        # get self as dictionary representation, remove fields that don't exist, extract last
        # updated and the survey id.
        new_data = self.as_dict()
        archive_start = new_data.pop("last_updated")
        survey_id = new_data.pop("id")
        new_data.pop("object_id")
        new_data.pop("created_on")
        new_data.pop("study")
        new_data.pop("name")
        
        # Get the most recent archive for this Survey, to check whether the Survey has been edited
        try:
            prior_archive = self.archives.latest('archive_start').as_dict()
        except SurveyArchive.DoesNotExist:
            prior_archive = None
        
        # if there was a prior archive identify if there were any changes, don't create an
        # archive if there were no changes.
        if prior_archive is not None:
            if not any(prior_archive[shared_field_name] != shared_field_value
                       for shared_field_name, shared_field_value in new_data.items()):
                return
        
        ret = SurveyArchive(
            **new_data,
            survey_id=survey_id,
            archive_start=archive_start,
        ).save()
        return ret


class SurveyArchive(SurveyBase):
    """ All fields declared in abstract survey are copied whenever a change is made to a survey """
    archive_start = models.DateTimeField(db_index=True)
    survey: Survey = models.ForeignKey('Survey', on_delete=models.PROTECT, related_name='archives', db_index=True)
    
    # related field typings (IDE halp)
    archived_events: Manager[ArchivedEvent]

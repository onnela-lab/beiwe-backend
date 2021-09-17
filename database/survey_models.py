import json

from django.db import models

from config.study_constants import AUDIO_SURVEY_SETTINGS, IMAGE_SURVEY_SETTINGS
from database.common_models import JSONTextField, TimestampedModel
from database.validators import LengthValidator


class SurveyBase(TimestampedModel):
    """ SurveyBase contains all fields that we want to have copied into a survey backup whenever
    it is updated. """

    AUDIO_SURVEY = 'audio_survey'
    TRACKING_SURVEY = 'tracking_survey'
    DUMMY_SURVEY = 'dummy'  # this does not appear to exist elsewhere.
    IMAGE_SURVEY = 'image_survey'
    SURVEY_TYPE_CHOICES = (
        (AUDIO_SURVEY, AUDIO_SURVEY),
        (TRACKING_SURVEY, TRACKING_SURVEY),
        (DUMMY_SURVEY, DUMMY_SURVEY),
        (IMAGE_SURVEY, IMAGE_SURVEY)
    )

    content = JSONTextField(default='[]', help_text='JSON blob containing information about the survey questions.')
    survey_type = models.CharField(max_length=16, choices=SURVEY_TYPE_CHOICES,
                                   help_text='What type of survey this is.')
    settings = JSONTextField(default='{}', help_text='JSON blob containing settings for the survey.')

    deleted = models.BooleanField(default=False)

    class Meta:
        abstract = True


class Survey(SurveyBase):
    """
    Surveys contain all information the app needs to display the survey correctly to a participant,
    and when it should push the notifications to take the survey.

    Surveys must have a 'survey_type', which is a string declaring the type of survey it
    contains, which the app uses to display the correct interface.

    Surveys contain 'content', which is a JSON blob that is unpacked on the app and displayed
    to the participant in the form indicated by the survey_type.

    Timings schema: a survey must indicate the day of week and time of day on which to trigger;
    by default it contains no values. The timings schema mimics the Java.util.Calendar.DayOfWeek
    specification: it is zero-indexed with day 0 as Sunday. 'timings' is a list of 7 lists, each
    inner list containing any number of times of the day. Times of day are integer values
    indicating the number of seconds past midnight.

    Inherits the following fields from SurveyBase
    content
    survey_type
    settings
    timings
    """

    # This is required for file name and path generation
    object_id = models.CharField(max_length=24, unique=True, validators=[LengthValidator(24)])
    # the study field is not inherited because we need to change its related name
    study = models.ForeignKey('Study', on_delete=models.PROTECT, related_name='surveys')

    @classmethod
    def create_with_object_id(cls, **kwargs):
        object_id = cls.generate_objectid_string("object_id")
        survey = cls.objects.create(object_id=object_id, **kwargs)
        return survey

    @classmethod
    def create_with_settings(cls, survey_type: str, **kwargs):
        """
        Create a new Survey with the provided survey type and attached to the given Study,
        as well as any other given keyword arguments. If the Survey is audio/image and no other
        settings are given, give it the default audio/image survey settings.
        """

        if survey_type == cls.AUDIO_SURVEY and 'settings' not in kwargs:
            kwargs['settings'] = json.dumps(AUDIO_SURVEY_SETTINGS)
        elif survey_type == cls.IMAGE_SURVEY and 'settings' not in kwargs:
            kwargs['settings'] = json.dumps(IMAGE_SURVEY_SETTINGS)

        survey = cls.create_with_object_id(survey_type=survey_type, **kwargs)
        return survey

    def weekly_timings(self):
        """
        Returns a json serializable object that represents the weekly schedules of this survey.
        The return object is a list of 7 lists of ints
        """
        from database.schedule_models import WeeklySchedule
        return WeeklySchedule.export_survey_timings(self)

    def relative_timings(self):
        """
        Returns a json serializable object that represents the relative schedules of the survey
        The return object is a list of lists of intervention ids, days offset, and seconds offset.
        """
        schedules = []
        for schedule in self.relative_schedules.all():
            num_seconds = schedule.minute * 60 + schedule.hour * 3600
            schedules.append([schedule.intervention.id, schedule.days_after, num_seconds])
        return schedules

    def relative_timings_by_name(self):
        """
        Returns a json serializable object that represents the relative schedules of the survey
        The return object is a list of lists of intervention names, days offset, and seconds offset.
        """
        schedules = []
        for schedule in self.relative_schedules.all():
            num_seconds = schedule.minute * 60 + schedule.hour * 3600
            schedules.append([schedule.intervention.name, schedule.days_after, num_seconds])

        return schedules

    def absolute_timings(self):
        """
        Returns a json serializable object that represents the absolute schedules of the survey
        The return object is a list of lists of the year, month, day and seconds within the day.
        """
        schedules = []
        for schedule in self.absolute_schedules.all():
            event_time = schedule.event_time
            num_seconds = event_time.minute * 60 + event_time.hour * 3600
            schedules.append([event_time.year, event_time.month, event_time.day, num_seconds])
        return schedules

    def notification_events(self, **archived_event_filter_kwargs):
        from database.schedule_models import ArchivedEvent
        return ArchivedEvent.objects.filter(
            survey_archive_id__in=self.archives.values_list("id", flat=True)
        ).filter(**archived_event_filter_kwargs).order_by("-scheduled_time")

    def format_survey_for_study(self):
        """
        Returns a dict with the values of the survey fields for download to the app
        """
        survey_dict = self.as_unpacked_native_python()
        # Make the dict look like the old Mongolia-style dict that the frontend is expecting
        survey_dict.pop('id')
        survey_dict.pop('deleted')
        survey_dict['_id'] = survey_dict.pop('object_id')

        # the old timings object does need to be provided
        from database.schedule_models import WeeklySchedule
        survey_dict['timings'] = WeeklySchedule.export_survey_timings(self)

        return survey_dict

    def save(self, *args, **kwargs):
        super().save(*args, **kwargs)
        self.archive()

    def most_recent_archive(self):
        return self.archives.latest('archive_start')

    def archive(self):
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

        # Get the most recent archive for this Survey, to check whether the Survey has been edited
        try:
            prior_archive = self.most_recent_archive().as_dict()
        except SurveyArchive.DoesNotExist:
            prior_archive = None

        # if there was a prior archive identify if there were any changes, don't create an
        # archive if there were no changes.
        if prior_archive is not None:
            if not any(prior_archive[shared_field_name] != shared_field_value
                       for shared_field_name, shared_field_value in new_data.items()):
                return

        SurveyArchive(
            **new_data,
            survey_id=survey_id,
            archive_start=archive_start,
        ).save()


class SurveyArchive(SurveyBase):
    """ All fields declared in abstract survey are copied whenever a change is made to a survey """
    archive_start = models.DateTimeField()
    survey = models.ForeignKey('Survey', on_delete=models.PROTECT, related_name='archives', db_index=True)

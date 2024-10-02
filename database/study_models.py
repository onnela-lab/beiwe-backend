from __future__ import annotations

from datetime import datetime, tzinfo
from typing import Any, Dict

from dateutil.tz import gettz
from django.core.exceptions import ObjectDoesNotExist
from django.core.validators import MaxValueValidator, MinValueValidator
from django.db import models
from django.db.models import F, Func, Manager
from django.db.models.query import QuerySet
from django.utils.timezone import localtime

from constants.data_stream_constants import ALL_DATA_STREAMS
from constants.message_strings import DEFAULT_HEARTBEAT_MESSAGE
from constants.study_constants import (ABOUT_PAGE_TEXT, CONSENT_FORM_TEXT,
    DEFAULT_CONSENT_SECTIONS_JSON, SURVEY_SUBMIT_SUCCESS_TOAST_TEXT)
from constants.user_constants import ResearcherRole
from database.common_models import ObjectIDModel, UtilityModel
from database.models import JSONTextField, TimestampedModel
from database.validators import LengthValidator
from libs.utils.date_utils import date_is_in_the_past


# this is an import hack to improve IDE assistance
try:
    from database.models import (ChunkRegistry, DashboardColorSetting, FileToProcess, Intervention,
        Participant, ParticipantFieldValue, Researcher, StudyRelation, Survey)
except ImportError:
    pass


class Study(TimestampedModel, ObjectIDModel):
    # When a Study object is created, a default DeviceSettings object is automatically
    # created alongside it. If the Study is created via the researcher interface (as it
    # usually is) the researcher is immediately shown the DeviceSettings to edit. The code
    # to create the DeviceSettings object is in database.signals.populate_study_device_settings.
    
    STUDY_EXPORT_FIELDS = ["name", "timezone_name", "deleted", "forest_enabled", "id", "object_id"]
    
    name = models.TextField(unique=True, help_text='Name of the study; can be of any length')
    encryption_key = models.CharField(
        max_length=32, validators=[LengthValidator(32)],
        help_text='Key used for encrypting the study data'
    )
    object_id = models.CharField(
        max_length=24, unique=True, validators=[LengthValidator(24)],
        help_text='ID used for naming S3 files'
    )
    timezone_name = models.CharField(
        max_length=256, default="America/New_York", null=False, blank=False
    )
    
    end_date = models.DateField(null=True, blank=True)
    manually_stopped = models.BooleanField(default=False)
    
    deleted = models.BooleanField(default=False)
    forest_enabled = models.BooleanField(default=False)
    
    # easy enrollment disables the password (skips the check) at participant registration
    # for all participants in the study.
    easy_enrollment = models.BooleanField(default=False)
    
    # Researcher security settings
    password_minimum_length = models.PositiveIntegerField(default=8, validators=[MinValueValidator(8), MaxValueValidator(20)])
    password_max_age_enabled = models.BooleanField(default=False)
    password_max_age_days = models.PositiveIntegerField(default=365, validators=[MinValueValidator(30), MaxValueValidator(365)])
    mfa_required = models.BooleanField(default=False)
    
    # related field typings (IDE halp)
    chunk_registries: Manager[ChunkRegistry]
    dashboard_colors: Manager[DashboardColorSetting]
    device_settings: DeviceSettings
    fields: Manager[StudyField]
    files_to_process: Manager[FileToProcess]
    interventions: Manager[Intervention]
    participants: Manager[Participant]
    study_relations: Manager[StudyRelation]
    surveys: Manager[Survey]
    
    def save(self, *args, **kwargs):
        """ Ensure there is a study device settings attached to this study. """
        # First we just save. This code has vacillated between throwing a validation error and not
        # during study creation.  Our current fix is to save, then test whether a device settings
        # object exists.  If not, create it.
        super().save(*args, **kwargs)
        try:
            # trunk-ignore(ruff/B018)
            self.device_settings
        except ObjectDoesNotExist:
            settings = DeviceSettings(study=self)
            self.device_settings = settings
            settings.save()
            # update the study object to have a device settings object (possibly unnecessary?).
            super().save(*args, **kwargs)
    
    @classmethod
    def create_with_object_id(cls, **kwargs) -> Study:
        """ Creates a new study with a populated object_id field. """
        study = cls(object_id=cls.generate_objectid_string("object_id"), **kwargs)
        study.save()
        return study
    
    @classmethod
    def get_all_studies_by_name(cls) -> QuerySet[Study]:
        """ Sort the un-deleted Studies a-z by name, ignoring case. """
        return (cls.objects
                .filter(deleted=False)
                .annotate(name_lower=Func(F('name'), function='LOWER'))
                .order_by('name_lower'))
    
    @classmethod
    def _get_administered_studies_by_name(cls, researcher) -> QuerySet[Study]:
        return cls.get_all_studies_by_name().filter(
                study_relations__researcher=researcher,
                study_relations__relationship=ResearcherRole.study_admin,
            )
    
    @classmethod
    def get_researcher_studies_by_name(cls, researcher) -> QuerySet[Study]:
        return cls.get_all_studies_by_name().filter(study_relations__researcher=researcher)
    
    def get_researchers(self) -> QuerySet[Researcher]:
        from database.user_models_researcher import Researcher
        return Researcher.objects.filter(study_relations__study=self)
    
    def notification_events(self, **archived_event_filter_kwargs):
        from database.schedule_models import ArchivedEvent
        return ArchivedEvent.objects.filter(
            survey_archive_id__in=self.surveys.all().values_list("archives__id", flat=True)
        ).filter(**archived_event_filter_kwargs).order_by("-scheduled_time")
    
    def now(self) -> datetime:
        """ Returns a timezone.now() equivalence in the study's timezone. """
        return localtime(localtime(), timezone=self.timezone)  # localtime(localtime(... saves an import... :D
    
    @property
    def timezone(self) -> tzinfo:
        """ So pytz.timezone("America/New_York") provides a tzinfo-like object that is wrong by 4
        minutes.  That's insane.  The dateutil gettz function doesn't have that fun insanity. """
        # profiling info: gettz takes on the order of 10s of microseconds
        return gettz(self.timezone_name)
    
    @property
    def end_date_is_in_the_past(self) -> bool:
        """ Returns True if the study end date is in the past. """
        return date_is_in_the_past(self.end_date, self.timezone_name)
    
    @property
    def study_is_stopped(self) -> bool:
        """ Returns True if the study meets stopped criteria. This function is the source of truth
        for this data point. """
        return self.deleted or self.manually_stopped or self.end_date_is_in_the_past
    
    @property
    def data_quantity_metrics(self):
        """ Get the data quantities for each data stream, format the number in base-2 MB with no
        decimal places and comma separators. """
        print("data quantity metrics for study: ", self.name)
        super_total = 0
        for data_stream in ALL_DATA_STREAMS:
            summation = sum(
                self.chunk_registries.filter(data_type=data_stream).values_list("file_size", flat=True)
            )
            print(f"{data_stream}: {summation / 1024 / 1024:,.0f} MB")
            super_total += summation
        print(f"total: {super_total / 1024 / 1024:,.0f} MB")


class StudyField(UtilityModel):
    study: Study = models.ForeignKey(Study, on_delete=models.PROTECT, related_name='fields')
    field_name = models.TextField()
    
    class Meta:
        unique_together = (("study", "field_name"),)
    
    # related field typings (IDE halp)
    field_values: Manager[ParticipantFieldValue]


class DeviceSettings(TimestampedModel):
    """ The DeviceSettings database contains the structure that defines settings pushed to devices
    of users in of a study. """
    
    def export(self) -> Dict[str, Any]:
        """ DeviceSettings is a special case where we want to export all fields.  Do not add fields
        to this model that cannot be trivially exported inside as_unpacked_native_python. """
        field_names = self.local_field_names()
        field_names.remove("created_on")
        field_names.remove("last_updated")
        return self.as_unpacked_native_python(field_names)
    
    # Whether various device options are turned on
    accelerometer = models.BooleanField(default=True)
    gps = models.BooleanField(default=True)
    calls = models.BooleanField(default=True)
    texts = models.BooleanField(default=True)
    wifi = models.BooleanField(default=True)
    bluetooth = models.BooleanField(default=False)
    power_state = models.BooleanField(default=True)
    use_anonymized_hashing = models.BooleanField(default=True)
    use_gps_fuzzing = models.BooleanField(default=False)
    call_clinician_button_enabled = models.BooleanField(default=True)
    call_research_assistant_button_enabled = models.BooleanField(default=True)
    ambient_audio = models.BooleanField(default=False)
    
    # Whether iOS-specific data streams are turned on
    proximity = models.BooleanField(default=False)
    gyro = models.BooleanField(default=False)  # not ios-specific anymore
    magnetometer = models.BooleanField(default=False)  # not ios-specific anymore
    devicemotion = models.BooleanField(default=False)
    reachability = models.BooleanField(default=True)
    
    # Upload over cellular data or only over WiFi (WiFi-only is default)
    allow_upload_over_cellular_data = models.BooleanField(default=False)
    
    # Timer variables
    accelerometer_off_duration_seconds = models.PositiveIntegerField(default=10, validators=[MinValueValidator(1)])
    accelerometer_on_duration_seconds = models.PositiveIntegerField(default=10, validators=[MinValueValidator(1)])
    accelerometer_frequency = models.PositiveIntegerField(default=10, validators=[MinValueValidator(1)])
    ambient_audio_off_duration_seconds = models.PositiveIntegerField(default=10*60, validators=[MinValueValidator(1)])
    ambient_audio_on_duration_seconds = models.PositiveIntegerField(default=10*60, validators=[MinValueValidator(1)])
    ambient_audio_bitrate = models.PositiveIntegerField(default=24000, validators=[MinValueValidator(16000)])
    ambient_audio_sampling_rate = models.PositiveIntegerField(default=44100, validators=[MinValueValidator(16000)])
    bluetooth_on_duration_seconds = models.PositiveIntegerField(default=60, validators=[MinValueValidator(1)])
    bluetooth_total_duration_seconds = models.PositiveIntegerField(default=300, validators=[MinValueValidator(1)])
    bluetooth_global_offset_seconds = models.PositiveIntegerField(default=0)
    check_for_new_surveys_frequency_seconds = models.PositiveIntegerField(default=3600, validators=[MinValueValidator(30)])
    create_new_data_files_frequency_seconds = models.PositiveIntegerField(default=15 * 60, validators=[MinValueValidator(30)])
    gps_off_duration_seconds = models.PositiveIntegerField(default=600, validators=[MinValueValidator(1)])
    gps_on_duration_seconds = models.PositiveIntegerField(default=60, validators=[MinValueValidator(1)])
    seconds_before_auto_logout = models.PositiveIntegerField(default=600, validators=[MinValueValidator(1)])
    upload_data_files_frequency_seconds = models.PositiveIntegerField(default=3600, validators=[MinValueValidator(10)])
    voice_recording_max_time_length_seconds = models.PositiveIntegerField(default=240)
    wifi_log_frequency_seconds = models.PositiveIntegerField(default=300, validators=[MinValueValidator(10)])
    gyro_off_duration_seconds = models.PositiveIntegerField(default=600, validators=[MinValueValidator(1)])
    gyro_on_duration_seconds = models.PositiveIntegerField(default=60, validators=[MinValueValidator(1)])
    gyro_frequency = models.PositiveIntegerField(default=10, validators=[MinValueValidator(1)])
    
    # iOS-specific timer variables)
    magnetometer_off_duration_seconds = models.PositiveIntegerField(default=600, validators=[MinValueValidator(1)])
    magnetometer_on_duration_seconds = models.PositiveIntegerField(default=60, validators=[MinValueValidator(1)])
    devicemotion_off_duration_seconds = models.PositiveIntegerField(default=600, validators=[MinValueValidator(1)])
    devicemotion_on_duration_seconds = models.PositiveIntegerField(default=60, validators=[MinValueValidator(1)])
    
    # Text strings
    about_page_text = models.TextField(default=ABOUT_PAGE_TEXT)
    call_clinician_button_text = models.TextField(default='Call My Clinician')
    consent_form_text = models.TextField(default=CONSENT_FORM_TEXT, blank=True, null=False)
    survey_submit_success_toast_text = models.TextField(default=SURVEY_SUBMIT_SUCCESS_TOAST_TEXT)
    
    # Consent sections
    consent_sections = JSONTextField(default=DEFAULT_CONSENT_SECTIONS_JSON)
    
    # Slightly diverging from study _device_ settings to just a study setting because our logic
    # for checking and saving these is all connected to this device settings model, and
    # it doesn't matter if the device receives some extra settings that it doesn't use.
    # heartbeat settings
    heartbeat_message = models.TextField(default=DEFAULT_HEARTBEAT_MESSAGE)
    heartbeat_timer_minutes = models.PositiveIntegerField(default=60, validators=[MinValueValidator(30)])
    
    study: Study = models.OneToOneField(Study, on_delete=models.PROTECT, related_name='device_settings')

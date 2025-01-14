from __future__ import annotations

import json
import os
import uuid
from datetime import datetime, timedelta, tzinfo
from pprint import pprint
from typing import Dict, List, Optional, Tuple, Union

import orjson
import zstd
from Cryptodome.PublicKey import RSA
from dateutil.tz import gettz
from django.core.exceptions import ImproperlyConfigured
from django.core.validators import MinLengthValidator
from django.db import models
from django.db.models import Manager, QuerySet
from django.utils import timezone

from config.settings import DOMAIN_NAME
from constants.action_log_messages import HEARTBEAT_PUSH_NOTIFICATION_SENT
from constants.common_constants import LEGIBLE_DT_FORMAT, RUNNING_TESTS
from constants.data_stream_constants import ALL_DATA_STREAMS, IDENTIFIERS
from constants.user_constants import (ACTIVE_PARTICIPANT_FIELDS, ANDROID_API, IOS_API,
    IOS_APP_MINIMUM_PUSH_NOTIFICATION_RESEND_VERSION, OS_TYPE_CHOICES)
from database.common_models import UtilityModel
from database.models import TimestampedModel
from database.study_models import Study
from database.user_models_common import AbstractPasswordUser
from database.validators import ID_VALIDATOR
from libs.s3 import s3_retrieve
from libs.utils.participant_app_version_comparison import (is_participants_version_gte_target,
    VersionError)
from libs.utils.security_utils import (compare_password, device_hash, django_password_components,
    generate_easy_alphanumeric_string)


# This is an import hack to improve IDE assistance.  Most of these imports are cyclical and fail at
# runtime, but the exception is caught.  The IDE's parser doesn't know it would fail and just uses
# the information correctly, allowing us to use them in type annotations.  There are no weird
# runtime errors because type annotations are Completely Elided before runtime.  Then, with these
# annotations, your IDE is able to provide type inferencing assistance throughout the codebase.
#
# By attaching some extra type declarations to model classes for django's dynamically generated
# properties (example: "scheduled_events" on the Participant class below) we magically get that type
# information almost everywhere.  (These can be generated for you automatically by running `python
# run_script.py generate_relation_hax` and pasting as required.)
#
# If you must to use an unimportable class (like ArchivedEvent in the notification_events()
# convenience method on Participants below) you will need to add a local import.
try:
    from database.models import (ArchivedEvent, ChunkRegistry, EncryptionErrorMetadata,
        FileToProcess, ForestTask, InterventionDate, IOSDecryptionKey, LineEncryptionError,
        ScheduledEvent, StudyField, SummaryStatisticDaily, UploadTracking)
except ImportError:
    pass


class Participant(AbstractPasswordUser):
    """ The Participant database object contains the password hashes and unique usernames of any
    participants in the study, as well as information about the device the participant is using.
    A Participant uses mobile, so their passwords are hashed accordingly. """
    DESIRED_ALGORITHM = "sha1"   # Yes, Bad, but this password doesn't actually protect access to data.
    DESIRED_ITERATIONS = 1000  # We will be completely reworking participant authentication soon anyway.
    
    patient_id = models.CharField(max_length=8, unique=True, validators=[ID_VALIDATOR])
    device_id = models.CharField(max_length=256, blank=True)
    os_type = models.CharField(max_length=16, choices=OS_TYPE_CHOICES, blank=True)
    study: Study = models.ForeignKey(
        Study, on_delete=models.PROTECT, related_name='participants', null=False
    )
    # see timezone property
    timezone_name = models.CharField(  # Warning: this is not used yet.
        max_length=256, default="America/New_York", null=False, blank=False
    )
    unknown_timezone = models.BooleanField(default=True)  # flag for using participant's timezone.
    
    push_notification_unreachable_count = models.SmallIntegerField(default=0, null=False, blank=False)
    last_heartbeat_notification = models.DateTimeField(null=True, blank=True)
    last_heartbeat_checkin = models.DateTimeField(null=True, blank=True)
    
    # TODO: clean out or maybe rename these fields to distinguish from last_updated? also wehave two survey checkin timestamps
    # new checkin logic
    first_push_notification_checkin = models.DateTimeField(null=True, blank=True)
    last_push_notification_checkin = models.DateTimeField(null=True, blank=True)
    last_survey_checkin = models.DateTimeField(null=True, blank=True)
    
    # pure tracking - these are used to track the last time a participant did something.
    last_get_latest_surveys = models.DateTimeField(null=True, blank=True)
    last_upload = models.DateTimeField(null=True, blank=True)
    first_register_user = models.DateTimeField(null=True, blank=True)
    last_register_user = models.DateTimeField(null=True, blank=True)
    last_set_password = models.DateTimeField(null=True, blank=True)
    last_set_fcm_token = models.DateTimeField(null=True, blank=True)
    last_get_latest_device_settings = models.DateTimeField(null=True, blank=True)
    
    # participant device tracking
    # the version code and name are slightly different between android and ios. (android HAS a
    # monotonic version code, so we use it. ios has semantic versioning as the best code.
    # android: last_version_code': '68', 'last_version_name': '3.4.2'
    # ios:  'last_version_code': '2.5.1', 'last_version_name': '2024.21',
    last_version_code = models.CharField(max_length=32, blank=True, null=True)
    last_version_name = models.CharField(max_length=32, blank=True, null=True)
    last_os_version = models.CharField(max_length=32, blank=True, null=True)
    raw_notification_report = models.TextField(default=None, null=True, blank=True)
    last_active_survey_ids = models.TextField(default=None, null=True, blank=True)
    device_status_report = models.TextField(default=None, null=True, blank=True)
    
    deleted = models.BooleanField(default=False)
    
    # retired participants are blocked from uploading further data.
    permanently_retired = models.BooleanField(default=False)
    # easy enrolement disables the need for a password at registration (ignores the password)
    easy_enrollment = models.BooleanField(default=False)
    
    # Participant experiments, beta features - these fields literally may not be used Anywhere, some
    # of them are filler for future features that may or may not be implemented. Some are for
    # backend feature, some are for app features. (features under active development should be
    # annotated in some way but no promises.)
    # Set help text over in libs/django_forms/forms.py
    enable_aggressive_background_persistence = models.BooleanField(default=False)
    enable_binary_uploads = models.BooleanField(default=False)
    enable_new_authentication = models.BooleanField(default=False)
    enable_developer_datastream = models.BooleanField(default=False)
    enable_beta_features = models.BooleanField(default=False)
    enable_extensive_device_info_tracking = models.BooleanField(default=False)
    
    EXPERIMENT_FIELDS = (
        # "enable_aggressive_background_persistence",
        # "enable_binary_uploads",
        # "enable_new_authentication",
        # "enable_developer_datastream",
        # "enable_beta_features",
        "enable_extensive_device_info_tracking",
    )
    
    # related field typings (IDE halp)
    archived_events: Manager[ArchivedEvent]
    chunk_registries: Manager[ChunkRegistry]
    deletion_event: Manager[ParticipantDeletionEvent]
    device_status_reports: Manager[DeviceStatusReportHistory]
    fcm_tokens: Manager[ParticipantFCMHistory]
    field_values: Manager[ParticipantFieldValue]
    files_to_process: Manager[FileToProcess]
    heartbeats: Manager[AppHeartbeats]
    intervention_dates: Manager[InterventionDate]
    scheduled_events: Manager[ScheduledEvent]
    upload_trackers: Manager[UploadTracking]
    action_logs: Manager[ParticipantActionLog]
    app_version_history: Manager[AppVersionHistory]
    notification_reports: Manager[SurveyNotificationReport]
    # undeclared:
    encryptionerrormetadata_set: Manager[EncryptionErrorMetadata]  # TODO: remove when ios stops erroring
    foresttask_set: Manager[ForestTask]
    iosdecryptionkey_set: Manager[IOSDecryptionKey]  # TODO: remove when ios stops erroring
    lineencryptionerror_set: Manager[LineEncryptionError]  # TODO: remove when ios stops erroring?
    pushnotificationdisabledevent_set: Manager[PushNotificationDisabledEvent]
    summarystatisticdaily_set: Manager[SummaryStatisticDaily]
    
    ################################################################################################
    ###################################### Timezones ###############################################
    ################################################################################################
    
    @property
    def timezone(self) -> tzinfo:
        """ So pytz.timezone("America/New_York") provides a tzinfo-like object that is wrong by 4
        minutes.  That's insane.  The dateutil gettz function doesn't have that fun insanity. """
        return gettz(self.timezone_name)
    
    def try_set_timezone(self, new_timezone_name: str):
        """ Use dateutil to test whether the timezone is valid, only set timezone_name field if it
        is. Set unknown_timezone to True if the timezone is invalid, false if it is valid. """
        if new_timezone_name is None or new_timezone_name == "":
            raise TypeError("None and the empty string actually coerce to the UTC timezone, which is weird and undesireable.")
        
        new_tz = gettz(new_timezone_name)
        if new_tz is None:
            # if study timezone is null or empty, use the default timezone.
            study_timezone_name = self.study.timezone_name
            if study_timezone_name is None or study_timezone_name == "":
                study_timezone_name = Participant._meta.get_field("timezone_name").default
            self.update_only(unknown_timezone=True, timezone_name=study_timezone_name)
        else:
            # force setting unknown_timezone false if the value is valid
            self.update_only(unknown_timezone=False, timezone_name=new_timezone_name)
    
    ################################################################################################
    ########################## Participant Creation and Passwords ##################################
    ################################################################################################
    
    @classmethod
    def create_with_password(cls, **kwargs) -> Tuple[str, str]:
        """ Creates a new participant with randomly generated patient_id and password. """
        # Ensure that a unique patient_id is generated. If it is not after
        # twenty tries, raise an error.
        patient_id = generate_easy_alphanumeric_string()
        for _ in range(20):
            if not cls.objects.filter(patient_id=patient_id).exists():
                # If patient_id does not exist in the database already
                break
            patient_id = generate_easy_alphanumeric_string()
        else:
            raise RuntimeError('Could not generate unique Patient ID for new Participant.')
        
        # Create a Participant, and generate for them a password
        participant = cls(patient_id=patient_id, **kwargs)
        password = participant.reset_password()  # this saves participant
        return patient_id, password
    
    def generate_hash_and_salt(self, password: bytes) -> Tuple[bytes, bytes]:
        """ The Participant's device runs sha256 on the input password before sending it. """
        return super().generate_hash_and_salt(device_hash(password))
    
    def debug_validate_password(self, compare_me: str) -> bool:
        """ Hardcoded values for a test, this is for a test, do not use, this is just for tests. """
        if not RUNNING_TESTS:
            raise PermissionError("This method is for testing only.")
        _algorithm, _iterations, password, salt = django_password_components(self.password)
        return compare_password('sha1', 2, device_hash(compare_me.encode()), password, salt)
    
    def get_private_key(self) -> RSA.RsaKey:
        from libs.s3 import get_client_private_key  # weird import triangle
        return get_client_private_key(self.patient_id, self.study.object_id)
    
    ################################################################################################
    ########################## FCM TOKENS AND PUSH NOTIFICATIONS ###################################
    ################################################################################################
    
    def assign_fcm_token(self, fcm_instance_id: str):
        ParticipantFCMHistory.objects.create(participant=self, token=fcm_instance_id)
    
    def get_valid_fcm_token(self) -> ParticipantFCMHistory:
        try:
            return self.fcm_tokens.get(unregistered__isnull=True)
        except ParticipantFCMHistory.DoesNotExist:
            return None
    
    @property
    def participant_push_enabled(self) -> bool:
        # this import causes a super stupid import triangle over in celery push notifications
        # after a refactor that literally just separated code into multiple files. Obviously.
        from libs.firebase_config import check_firebase_instance
        return (
            self.os_type == ANDROID_API and check_firebase_instance(require_android=True) or
            self.os_type == IOS_API and check_firebase_instance(require_ios=True)
        )
    
    ################################################################################################
    ###################################### History I Guess #########################################
    ################################################################################################
    
    def notification_events(self, **archived_event_filter_kwargs) -> Manager[ArchivedEvent]:
        """ convenience methodd for use debugging in the terminal mostly. """
        from database.schedule_models import ArchivedEvent
        return ArchivedEvent.objects.filter(participant=self) \
            .filter(**archived_event_filter_kwargs).order_by("-scheduled_time")
    
    def log(self, action: str):
        """ Creates a ParticipantActionLog object. """
        return ParticipantActionLog.objects.create(
            participant=self, timestamp=timezone.now(), action=action)
    
    ################################################################################################
    ################################## PARTICIPANT STATE ###########################################
    ################################################################################################
    
    @property
    def is_allowed_surveys(self) -> bool:
        from libs.schedules import participant_allowed_surveys
        return participant_allowed_surveys(self)
    
    @property
    def is_active_one_week(self) -> bool:
        return Participant._is_active(self, timezone.now() - timedelta(days=7))
    
    @staticmethod
    def _is_active(participant: Participant, activity_threshold: datetime) -> bool:
        """ Logic to determine if a participant counts as active. """
        # get the most recent timestamp from the list of fields, and check if it is more recent than
        # now the participant is considered active.
        
        # permanently retired participants are not active.
        if participant.permanently_retired:
            return False
        
        for key in ACTIVE_PARTICIPANT_FIELDS:
            if not hasattr(participant, key):
                raise ImproperlyConfigured("Participant model does not have a field named {key}.")
            
            # special case for permanently_retired, which is a boolean field.
            if key == "permanently_retired":
                continue
            
            # The rest are datetimes, if they are more recent than the threshold, they are active.
            value = getattr(participant, key)
            if value is not None and value >= activity_threshold:
                return True
        
        # The case where the participant has no activity at all returns False - this is correct
        # behavior, they are not active if they has no timestamps. Other code that cares about this
        # scenario needs to detect this case and handle it.   ????
        return False
    
    @property
    def most_recent_activity(self):
        # get the most recent timestamp out of the active participant fields, handle Nones
        values = [getattr(self, key) for key in ACTIVE_PARTICIPANT_FIELDS]
        return max([v for v in values if v is not None], default=None)
    
    
    @property
    def is_dead(self) -> bool:
        return self.deleted or self.has_deletion_event
    
    @classmethod
    def filter_possibly_pushable_participants(cls) -> QuerySet[Participant]:
        return cls.objects.filter(deleted=False, deletion_event__isnull=True, permanently_retired=False)
    
    @property
    def has_deletion_event(self) -> bool:
        try:
            # trunk-ignore(ruff/B018)
            self.deletion_event
            return True
        except ParticipantDeletionEvent.DoesNotExist:
            return False
    
    @property 
    def can_handle_push_notification_resends(self) -> bool:
        if self.os_type != IOS_API or self.last_version_code is None or self.last_version_name is None:
            return False
        try:
            # does all the tests for None-ness etc.
            return is_participants_version_gte_target(
                self.os_type,
                self.last_version_code,
                self.last_version_name,
                IOS_APP_MINIMUM_PUSH_NOTIFICATION_RESEND_VERSION,
            )
        
        except VersionError:
            return False
    
    ################################################################################################
    ######################################### S3 DATA ##############################################
    ################################################################################################
    
    def s3_retrieve(self, s3_path: str) -> bytes:
        raw_path = s3_path.startswith(self.study.object_id)
        return s3_retrieve(s3_path, self, raw_path=raw_path)
    
    @property
    def get_identifiers(self):
        for identifier in self.chunk_registries.filter(data_type=IDENTIFIERS).order_by("created_on"):
            print(identifier.s3_retrieve().decode())
    
    ################################################################################################
    ######################################### LOGGING ##############################################
    ################################################################################################
    
    def generate_app_version_history(self, version_code: str, version_name: str, os_version: str):
        """ Creates an AppVersionHistory object. """
        AppVersionHistory.objects.create(
            participant=self,
            app_version_code=version_code or "missing",
            app_version_name=version_name or "missing",
            os_version=os_version or "missing",
            os_is_ios=self.os_type == IOS_API,
        )
    
    def generate_device_status_report_history(self, url: str):
        # this is just stupid but a mistake ages ago means we have to do this.
        if self.last_os_version == IOS_API:
            app_version = str(self.last_version_code) + " " + str(self.last_version_name)
        else:
            app_version = str(self.last_version_name) + " " + str(self.last_version_code)
        
        # testing on a (sloooowww) aws T3 server got about 20us on a 1.5k string of json data
        # with a compression ratio of about 3x.  7 was slightly better than others on test data.
        if self.device_status_report:
            compressed_data = zstd.compress(self.device_status_report.encode(), 7, 1)
        else:
            compressed_data = b"empty"
        
        DeviceStatusReportHistory.objects.create(
            participant=self,
            app_os=self.os_type or "None",
            os_version=self.last_os_version or "None",
            app_version=app_version or "None",
            endpoint=url or "None",
            compressed_report=compressed_data,
        )
    
    ################################################################################################
    ############################### TERMINAL DEBUGGING FUNCTIONS ###################################
    ################################################################################################
    
    def __str__(self) -> str:
        return f'{self.patient_id} of Study "{self.study.name}"'
    
    @property
    def _recents(self) -> Dict[str, Union[str, Optional[str]]]:
        self.refresh_from_db()
        now = timezone.now()
        return {
            "last_version_code": self.last_version_code,
            "last_version_name": self.last_version_name,
            "last_os_version": self.last_os_version,
            "last_get_latest_surveys": f"{(now - self.last_get_latest_surveys).total_seconds() // 60} minutes ago" if self.last_get_latest_surveys else None,
            "last_push_notification_checkin": f"{(now - self.last_push_notification_checkin).total_seconds() // 60} minutes ago" if self.last_push_notification_checkin else None,
            "last_register_user": f"{(now - self.last_register_user).total_seconds() // 60} minutes ago" if self.last_register_user else None,
            "last_set_fcm_token": f"{(now - self.last_set_fcm_token).total_seconds() // 60} minutes ago" if self.last_set_fcm_token else None,
            "last_set_password": f"{(now - self.last_set_password).total_seconds() // 60} minutes ago" if self.last_set_password else None,
            "last_survey_checkin": f"{(now - self.last_survey_checkin).total_seconds() // 60} minutes ago" if self.last_survey_checkin else None,
            "last_upload": f"{(now - self.last_upload).total_seconds() // 60} minutes ago" if self.last_upload else None,
            "last_get_latest_device_settings": f"{(now - self.last_get_latest_device_settings).total_seconds() // 60} minutes ago" if self.last_get_latest_device_settings else None,
        }
    
    def get_data_summary(self) -> Dict[str, Union[str, int]]:
        """ Assembles a summary of data quantities for the participant, for debugging. """
        data = {stream: 0 for stream in ALL_DATA_STREAMS}
        for data_type, size in self.chunk_registries.values_list("data_type", "file_size").iterator():
            data[data_type] += size
        
        # print with 2 digits after decimal point
        for k, v in data.items():
            print(f"{k}:", f"{v / 1024 / 1024:.2f} MB")
    
    def logs(self) -> List[str]:
        return self._logs()
    
    def logs_heartbeats_sent(self):
        return self._logs(HEARTBEAT_PUSH_NOTIFICATION_SENT)
    
    def _logs(self, action: str = None)  -> QuerySet[Tuple[datetime, str]]:
        # this is for terminal debugging - so most recent LAST.
        query: QuerySet[Tuple[datetime, str]] = self.action_logs.order_by("timestamp").values_list("timestamp", "action")
        if action:
            query = query.filter(action=action)
        tz = self.timezone
        return [
            f"{t.astimezone(tz).strftime(LEGIBLE_DT_FORMAT)}: '{action}'" for t, action in query
        ]
    
    @property
    def participant_page(self):
        """ returns a url for the participant page for this user (debugging function) """
        from libs.utils.http_utils import easy_url  # import triangle
        return f"https://{DOMAIN_NAME}" + easy_url(
            "participant_endpoints.participant_page", self.study.id, self.patient_id
        )
    
    @property
    def pprint(self):
        columns = os.get_terminal_size().columns
        d = self._pprint()
        d.pop("password") # not important or desired
        d.pop("device_id") # not important or desired
        dsr = d.pop("device_status_report")
        rn_uuids = d.pop("raw_notification_report")
        pprint(d, width=columns)
        
        if rn_uuids:
            print("\nRaw Notification UUIDs:")
            pprint(json.loads(rn_uuids), width=columns)
        else:
            print("\n(No raw notification report.)")
            
        # it can be None, and empty string
        if dsr:
            print("\nDevice Status Report:")
            pprint(json.loads(dsr), width=columns)
        else:
            print("\n(No device status report.)")
    
    def get_status_report_datum(self, key: str):
        """ For debugging, returns the value of a key in the device status report. """
        if not self.device_status_report:
            return "(no device status report)"
        data: dict = json.loads(self.device_status_report)
        return data.get(key, f"(no match for '{key}')")


class PushNotificationDisabledEvent(UtilityModel):
    # There may be many events
    # this is (currently) purely for record keeping.
    participant: Participant = models.ForeignKey(Participant, null=False, on_delete=models.PROTECT)
    count = models.IntegerField(null=False)
    timestamp = models.DateTimeField(null=False, blank=False, auto_now_add=True, db_index=True)


class ParticipantFCMHistory(TimestampedModel):
    # by making the token unique the solution to problems becomes "reinstall the app"
    participant: Participant = models.ForeignKey("Participant", null=False, on_delete=models.PROTECT, related_name="fcm_tokens")
    token = models.CharField(max_length=256, blank=False, null=False, db_index=True, unique=True,
                             validators=[MinLengthValidator(1)])
    unregistered = models.DateTimeField(null=True, blank=True)


class ParticipantFieldValue(UtilityModel):
    """ These objects can be deleted.  These are values for per-study custom fields for users """
    participant: Participant = models.ForeignKey(Participant, on_delete=models.PROTECT, related_name='field_values')
    field: StudyField = models.ForeignKey('StudyField', on_delete=models.CASCADE, related_name='field_values')
    value = models.TextField(null=False, blank=True, default="")
    
    class Meta:
        unique_together = (("participant", "field"),)


class ParticipantDeletionEvent(TimestampedModel):
    """ This is a list of participants that have been deleted, but we are keeping around for a while
    in case we need to restore them. """
    participant: Participant = models.OneToOneField(Participant, on_delete=models.PROTECT, related_name="deletion_event")
    files_deleted_count = models.BigIntegerField(null=False, blank=False, default=0)
    purge_confirmed_time = models.DateTimeField(null=True, blank=True, db_index=True)
    
    @classmethod
    def summary(cls):
        """ Provides a simple overview of the current state of the deletion queue. """
        now = timezone.now()
        now_minus_30 = now - timedelta(minutes=30)
        now_minus_6 = now - timedelta(minutes=6)
        past_24_hours = now - timedelta(hours=24)
        base = cls.objects.order_by("participant__patient_id")
        base_unfinished = base.filter(purge_confirmed_time__isnull=True).exclude(created_on__gt=now_minus_30)
        
        # deletion events with a created_on timestamp less than 30 minutes ago will have
        # last_updated of almost same age, and will not run.
        held_events = base.filter(created_on__gt=now_minus_30)
        print(
            f"There are {held_events.count()} participants with held deletion events:\n"
            f"{', '.join(p for p in held_events.values_list('participant__patient_id', flat=True))}"
            "\n"
        )
        
        # deletion events with a last updated time older than 30 minutes are eligible for deletion.
        # (excludes held events)
        eligible_events = base_unfinished.filter(last_updated__lt=now_minus_30)
        print(
            f"There are {eligible_events.count()} participants eligible for deletion:\n"
            f"{', '.join(p for p in eligible_events.values_list('participant__patient_id', flat=True))}"
            "\n"
        )
        
        # active deletion events are those with a last updated timestamp more recent than 6 minutes
        # ago (excludes held events).
        active_events = base_unfinished.filter(last_updated__gt=now_minus_6)
        print(
            f"There are {active_events.count()} potentially active deletion events:\n"
            f"{', '.join(p for p in active_events.values_list('participant__patient_id', flat=True))}"
            "\n"
        )
        
        # finished events are any with a purge_confirmed_time.
        finished_events = base.filter(purge_confirmed_time__isnull=False)
        finished_24 = finished_events.filter(purge_confirmed_time__gt=past_24_hours)
        print(
            f"There are {finished_events.count()} finished deletion events, with "
            f"{finished_24.count()} in the past 24 hours:\n"
            f"{', '.join(p for p in finished_24.values_list('participant__patient_id', flat=True))}"
        )


class AppHeartbeats(UtilityModel):
    """ Storing heartbeats is intended as a debugging tool for monitoring app uptime, the idea is 
    that the app checks in every 5 minutes so we can see when it doesn't. (And then send it a push
    notification) """
    participant = models.ForeignKey(Participant, null=False, on_delete=models.PROTECT, related_name="heartbeats")
    timestamp = models.DateTimeField(null=False, blank=False, db_index=True)
    # TODO: message is not intended to be surfaced to anyone other than developers, at time of comment
    # contains ios debugging info.
    message = models.TextField(null=True, blank=True)
    
    @classmethod
    def create(cls, participant: Participant, timestamp: datetime, message: str = None):
        participant.update_only(last_heartbeat_checkin=timestamp)
        return cls.objects.create(participant=participant, timestamp=timestamp, message=message)


# todo: add more ParticipantActionLog entries
class ParticipantActionLog(UtilityModel):
    """ This is a log of actions taken by participants, for debugging purposes. """
    participant: Participant = models.ForeignKey(Participant, null=False, on_delete=models.PROTECT, related_name="action_logs")
    timestamp = models.DateTimeField(null=False, blank=False, db_index=True)
    action = models.TextField(null=False, blank=False)
    
    @classmethod
    def heartbeat_notifications(cls) -> QuerySet[ParticipantActionLog]:
        return cls.objects.filter(action=HEARTBEAT_PUSH_NOTIFICATION_SENT)


class AppVersionHistory(TimestampedModel):
    """ We can't apply a unique constraint on these fields because that would cover over some 
    possible bugs that we want to be able to detect (and further complicate the participant
    credential code path).
    
    Notes
     - This model was added to the codebase in 2024.
     - IOS did not have app version code until 2.4.13.
     - Android has had both fields forever.
     - If either field is missing it gets filled as "missing"
     - Android's app version code is a monotonic integer.
     - IOS' app version code is a Semantic version string of the forms 2.x, 2.x.y, or 2.x.yz
     - Android's app version name is a Semantic version string of the form x.y.z. or x.y.z-likeRC1
     - IOS' app version Name is "missing" (OLD), a commit hash (old), or a year.build_count like 2024.21
     - os_is_ios is populated for old participants in a migration (130 at time of writing).
    """
    participant = models.ForeignKey(Participant, null=False, on_delete=models.PROTECT, related_name="app_version_history")
    app_version_code = models.CharField(max_length=16, blank=False, null=False)
    app_version_name = models.CharField(max_length=16, blank=False, null=False)
    os_version = models.CharField(max_length=16, blank=False, null=False)
    os_is_ios = models.BooleanField(null=True, blank=False)


class SurveyNotificationReport(TimestampedModel):
    """ This is a simple record of the notifications sent to participants. """
    participant = models.ForeignKey(Participant, null=False, on_delete=models.PROTECT, related_name="notification_reports")
    notification_uuid = models.UUIDField(default=uuid.uuid4, null=False, blank=False)
    applied = models.BooleanField(default=False)
    
    class Meta:
        unique_together = (("participant", "notification_uuid"),)  # statistically global


# device status report history
class DeviceStatusReportHistory(UtilityModel):
    created_on = models.DateTimeField(default=timezone.now)
    participant = models.ForeignKey(
        Participant, null=False, on_delete=models.PROTECT, related_name="device_status_reports"
    )
    app_os = models.CharField(max_length=32, blank=False, null=False)
    os_version = models.CharField(max_length=32, blank=False, null=False)
    app_version = models.CharField(max_length=32, blank=False, null=False)
    endpoint = models.TextField(null=False, blank=False)
    compressed_report: bytes = models.BinaryField(null=False, blank=False)  # used to be a memoryview
    
    @property
    def decompress(self) -> Dict[str, Union[str, int]]:
        return zstd.decompress(self.compressed_report).decode()
    
    @property
    def load_json(self):
        return orjson.loads(zstd.decompress(self.compressed_report))
    
    @classmethod
    def bulk_decode(cls, list_of_compressed_reports: List[bytes]) -> List[str]:
        return [
            zstd.decompress(report).decode() for report in list_of_compressed_reports
        ]
    
    @classmethod
    def bulk_load_json(cls, list_of_compressed_reports: List[bytes]) -> List[Dict[str, Union[str, int]]]:
        return [
            orjson.loads(zstd.decompress(report)) for report in list_of_compressed_reports
        ]

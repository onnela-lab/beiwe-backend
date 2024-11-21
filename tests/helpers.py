# trunk-ignore-all(bandit/B404,bandit/B105,ruff/B018)
import subprocess
import uuid
from datetime import date, datetime, timedelta, tzinfo
from typing import Any, List, Tuple

import orjson
from django.db.models import (AutoField, CharField, DateField, FloatField, ForeignKey, IntegerField,
    TextField)
from django.http.response import HttpResponse
from django.utils import timezone

from config.django_settings import STATIC_ROOT
from constants.celery_constants import ForestTaskStatus
from constants.common_constants import BEIWE_PROJECT_ROOT, REQUIRED_PARTICIPANT_PURGE_AGE_MINUTES
from constants.data_stream_constants import IDENTIFIERS
from constants.forest_constants import ForestTree
from constants.message_strings import MESSAGE_SEND_SUCCESS
from constants.schedule_constants import ScheduleTypes
from constants.testing_constants import REAL_ROLES
from constants.user_constants import (ANDROID_API, IOS_API,
    IOS_APP_MINIMUM_PUSH_NOTIFICATION_RESEND_VERSION, NULL_OS, ResearcherRole)
from database.common_models import generate_objectid_string
from database.data_access_models import ChunkRegistry, FileToProcess
from database.forest_models import ForestTask, SummaryStatisticDaily
from database.schedule_models import (AbsoluteSchedule, ArchivedEvent, Intervention,
    InterventionDate, RelativeSchedule, ScheduledEvent, WeeklySchedule)
from database.study_models import DeviceSettings, Study, StudyField
from database.survey_models import Survey
from database.user_models_participant import (AppHeartbeats, DeviceStatusReportHistory, Participant,
    ParticipantActionLog, ParticipantDeletionEvent, ParticipantFCMHistory, ParticipantFieldValue)
from database.user_models_researcher import Researcher, StudyRelation
from libs.internal_types import Schedule
from libs.schedules import repopulate_weekly_survey_schedule_events
from libs.utils.security_utils import device_hash, generate_easy_alphanumeric_string


CURRENT_TEST_HTML_FILEPATH = BEIWE_PROJECT_ROOT + "private/current_test_page.html"
ABS_STATIC_ROOT = (BEIWE_PROJECT_ROOT + STATIC_ROOT).encode()

# we need this to not be an _instance_ variable in TestDownloadParticipantTreeData
CURRENT_TEST_DATE = timezone.now().today().date()
CURRENT_TEST_DATE_TEXT = CURRENT_TEST_DATE.isoformat()
CURRENT_TEST_DATE_BYTES = CURRENT_TEST_DATE_TEXT.encode()

# the default participant must have all fields populated here and documented
# In General - the expectation should be that the default participant will not satisfy conditions
# where they are required and so will be set within the tests.
DEFAULT_PARTICIPANT_PARAMS = {
    # obvious base state
    "deleted": False,
    "permanently_retired": False,
    
    # explicit feature/setting flags should default to off
    "easy_enrollment": False,
    
    # timezone
    "timezone_name": "America/New_York",  # matching the default study
    "unknown_timezone": False,
    
    # app version info, set in tests that care
    "last_os_version": None,
    "last_version_code": None,
    "last_version_name": None,
    
    # server-side tracking timestamps
    "last_heartbeat_notification": None,
    "first_push_notification_checkin": None,
    "first_register_user": None,  # barely used
    
    # ... its a push notification setting, 0 is correct because that is in fact the true number.
    "push_notification_unreachable_count": 0,
    
    # beta features, enable manually if there are any tests for them at all
    "enable_aggressive_background_persistence": False,
    "enable_beta_features": False,
    "enable_binary_uploads": False,
    "enable_developer_datastream": False,
    "enable_extensive_device_info_tracking": False,
    "enable_new_authentication": False,
    
    # device action timestamps - only set these when there is an explicit test
    # The default expectation should be that the default participant will not satisfy conditions.
    "last_get_latest_device_settings": None,
    "last_get_latest_surveys": None,
    "last_heartbeat_checkin": None,
    "last_push_notification_checkin": None,
    "last_register_user": None,
    "last_set_fcm_token": None,
    "last_set_password": None,
    "last_survey_checkin": None,
    "last_upload": None,
    
    # remote device state, used in debugging, should be None and manipulated in tests
    "device_status_report": None,
    "raw_notification_report": None,
    "last_active_survey_ids": None,
}

# populated elsewvhere, note that user is an android user.
_skip = ("id","created_on","last_updated","password","patient_id","device_id","os_type","study")

_all_field_names = [field.name for field in Participant._meta.fields]
_unskipped_names = [field_name for field_name in _all_field_names if field_name not in _skip]

for _field_name in _unskipped_names:
    if _field_name not in DEFAULT_PARTICIPANT_PARAMS:
        raise (f"field {_field_name} is not populated in DEFAULT_PARTICIPANT_PARAMS." )
for _field_name in DEFAULT_PARTICIPANT_PARAMS:
    if _field_name not in _all_field_names:
        raise (f"field {_field_name} is not in the Participant model." )


# this is a real, if simple, survey, it contains logically displayed questions based on the slider Q

class DatabaseHelperMixin:
    """ This class implements DB object creation.  Some objects have convenience property wrappers
    because they are so common. """
    
    DEFAULT_RESEARCHER_NAME = "session_researcher"
    DEFAULT_RESEARCHER_PASSWORD = "abcABC123!@#" * 2  # we want a very long password for testing
    DEFAULT_STUDY_NAME = "session_study"
    DEFAULT_SURVEY_OBJECT_ID = 'u1Z3SH7l2xNsw72hN3LnYi96'
    DEFAULT_PARTICIPANT_NAME = "patient1"  # has to be 8 characters
    DEFAULT_PARTICIPANT_PASSWORD = "abcABC123"
    DEFAULT_PARTICIPANT_PASSWORD_HASHED = device_hash(DEFAULT_PARTICIPANT_PASSWORD.encode()).decode()
    DEFAULT_PARTICIPANT_DEVICE_ID = "default_device_id"
    DEFAULT_INTERVENTION_NAME = "default_intervention_name"
    DEFAULT_FCM_TOKEN = "abc123"  # actual value is irrelevant
    SOME_SHA1_PASSWORD_COMPONENTS = 'sha1$1000$zsk387ts02hDMRAALwL2SL3nVHFgMs84UcZRYIQWYNQ=$hllJauvRYDJMQpXQKzTdwQ=='
    DEFAULT_STUDY_FIELD_NAME = "default_study_field_name"
    DEFAULT_PARTICIPANT_FIELD_VALUE = "default_study_field_value"
    
    REFERENCE_SURVEY_CONTENT = orjson.dumps(
        [{'display_if': None,
          'max': '6',
          'min': '1',
          'question_id': 'cf46f66f-6360-469c-a2ca-2c15e02d19be',
          'question_text': 'slider',
          'question_type': 'slider'},
         {'display_if': {'>': ['cf46f66f-6360-469c-a2ca-2c15e02d19be', 2]},
          'question_id': '31c2af15-151e-4373-95bd-dafe10178a02',
          'question_text': 'greater than 2',
          'question_type': 'info_text_box'},
         {'display_if': {'<=': ['cf46f66f-6360-469c-a2ca-2c15e02d19be', 2]},
          'question_id': '1fea73d0-a341-4646-971c-fd2c4ca428c3',
          'question_text': 'less than equal to 2',
          'question_type': 'info_text_box'},
         {'display_if': None,
          'question_id': '474e6a25-b07e-4e44-c794-8e62f8b717c2',
          'question_text': 'always display',
          'question_type': 'info_text_box'},
         {'display_if': {'==': ['cf46f66f-6360-469c-a2ca-2c15e02d19be', 99]},
          'question_id': '868a0e85-436c-42a1-a6d8-ad76754c7fd0',
          'question_text': '',
          'question_type': 'info_text_box'}]
    ).decode()
    
    # this needs to be a dynamic property in order for the time_machine library to work
    @property
    def CURRENT_DATE(self) -> date:
        return timezone.now().today().date()
    
    @property
    def YESTERDAY(self) -> date:
        ret = self.CURRENT_DATE - timedelta(days=1)
        assert isinstance(ret, date)
        return ret
    
    @property
    def TOMORROW(self) -> date:
        return self.CURRENT_DATE + timedelta(days=1)
    
    # For all defaults make sure to maintain the pattern that includes the use of the save function,
    # this codebase implements a special save function that validates before passing through.
    
    #
    ## Study objects
    #
    @property
    def default_study(self) -> Study:
        """ Gets or creates a default study object.  Note that this has the side effect of creating
        a study settings db object as well.  This is a default object, and will be auto-populated
        in scenarios where such an object is required but not provided. """
        try:
            return self._default_study
        except AttributeError:
            pass
        self._default_study = self.generate_study(self.DEFAULT_STUDY_NAME)
        return self._default_study
    
    @property
    def session_study(self):
        """ alias for default_study """
        return self.default_study
    
    def generate_study(
        self,
        name: str,
        encryption_key: str = None,
        object_id: str = None,
        forest_enabled: bool = None
    ):
        study = Study(
            name=name,
            encryption_key=encryption_key or "thequickbrownfoxjumpsoverthelazy",
            object_id=object_id or generate_objectid_string(),
            forest_enabled=forest_enabled or True,
            # timezone_name="UTC",
            timezone_name="America/New_York",
            deleted=False,
        )
        study.save()
        return study
    
    def set_session_study_relation(
        self, relation: ResearcherRole = ResearcherRole.researcher
    ) -> StudyRelation:
        """ Applies the study relation to the session researcher to the session study. """
        if hasattr(self, "_default_study_relation"):
            raise Exception("can only be called once per test (currently?)")
        
        self._default_study_relation = self.generate_study_relation(
            self.session_researcher, self.session_study, relation
        )
        return self._default_study_relation
    
    def generate_study_relation(self, researcher: Researcher, study: Study, relation: str) -> StudyRelation:
        """ Creates a study relation based on the input values, returns it. """
        if relation is None:
            researcher.study_relations.filter(study=self.session_study).delete()
            return relation
        
        if relation == ResearcherRole.site_admin:
            researcher.update(site_admin=True)
            return relation
        relation: StudyRelation = StudyRelation(researcher=researcher, study=study, relationship=relation)
        relation.save()
        return relation
    
    # I seem to have built this and then forgotten about it because I stuck in somewhere weird.
    def assign_role(self, researcher: Researcher, role: ResearcherRole):
        """ Helper function to assign a user role to a Researcher.  Clears all existing roles on
        that user. """
        if role in REAL_ROLES:
            researcher.study_relations.all().delete()
            self.generate_study_relation(researcher, self.session_study, role)
            researcher.update(site_admin=False)
        elif role is None:
            researcher.study_relations.all().delete()
            researcher.update(site_admin=False)
        elif role == ResearcherRole.site_admin:
            researcher.study_relations.all().delete()
            researcher.update(site_admin=True)
    
    #
    ## Researcher objects
    #
    @property
    def session_researcher(self) -> Researcher:
        """ Gets or creates the session researcher object.  This is a default object, and will be
        auto-populated in scenarios where such an object is required but not provided.  """
        try:
            return self._default_researcher
        except AttributeError:
            pass
        self._default_researcher = self.generate_researcher(self.DEFAULT_RESEARCHER_NAME)
        return self._default_researcher
    
    def generate_researcher(
        self, name: str = None, relation_to_session_study: str = None
    ) -> Researcher:
        """ Generate a researcher based on the parameters provided, relation_to_session_study is
        optional. """
        researcher = Researcher(
            username=name or generate_easy_alphanumeric_string(),
            password=self.SOME_SHA1_PASSWORD_COMPONENTS,
            site_admin=relation_to_session_study == ResearcherRole.site_admin,
            password_force_reset=False,  # is True by default, makes no sense in a test context
        )
        # set password saves...
        researcher.set_password(self.DEFAULT_RESEARCHER_PASSWORD)
        if relation_to_session_study not in (None, ResearcherRole.site_admin):
            self.generate_study_relation(researcher, self.session_study, relation_to_session_study)
        
        return researcher
    
    #
    ## Objects for Studies
    #
    
    @property
    def default_survey(self) -> Survey:
        """ Creates a survey with no content attached to the session study. """
        try:
            return self._default_survey
        except AttributeError:
            pass
        self._default_survey = self.generate_survey(
            self.session_study, Survey.TRACKING_SURVEY, self.DEFAULT_SURVEY_OBJECT_ID,
        )
        return self._default_survey
    
    def generate_survey(
        self, study: Study, survey_type: str, object_id: str = None, content: Any = False, **kwargs
    ) -> Survey:
        
        survey = Survey(
            study=study,
            survey_type=survey_type,
            object_id=object_id or generate_objectid_string(),
            # conditionally add a content field as a one-liner :D
            **{"content": self.REFERENCE_SURVEY_CONTENT} if isinstance(content, bool) and content else {},
            **kwargs
        )
        survey.save()
        return survey
    
    @property
    def session_device_settings(self) -> DeviceSettings:
        """ Providing the comment about using the save() pattern is observed, this cannot fail. """
        return self.session_study.device_settings
    
    @property
    def default_intervention(self) -> Intervention:
        try:
            return self._default_intervention
        except AttributeError:
            pass
        self._default_intervention = self.generate_intervention(
            self.session_study, self.DEFAULT_INTERVENTION_NAME
        )
        return self._default_intervention
    
    def generate_intervention(self, study: Study, name: str) -> Intervention:
        intervention = Intervention(study=study, name=name)
        intervention.save()
        return intervention
    
    @property
    def default_study_field(self) -> StudyField:
        try:
            return self._default_study_field
        except AttributeError:
            pass
        self._default_study_field = self.generate_study_field(
            self.default_study, self.DEFAULT_STUDY_FIELD_NAME
        )
        return self._default_study_field
    
    def generate_study_field(self, study: Study, name: str) -> StudyField:
        study_field = StudyField(study=study, field_name=name)
        study_field.save()
        return study_field
    
    #
    ## Participant objects
    #
    
    @property
    def default_participant(self) -> Participant:
        """ Creates a participant object on the session study.  This is a default object, and will
        be auto-populated in scenarios where such an object is required but not provided. """
        try:
            return self._default_participant
        except AttributeError:
            pass
        
        self._default_participant = self.generate_participant(
            self.session_study,
            self.DEFAULT_PARTICIPANT_NAME,
            ios=False,
            device_id=self.DEFAULT_PARTICIPANT_DEVICE_ID,
            **DEFAULT_PARTICIPANT_PARAMS,
        )
        return self._default_participant
    
    def using_default_participant(self):
        """ Literally just a placeholder so that you can expressively say that you are 
        using/creating the default participant. (and also linters might complain) """
        self.default_participant
    
    @property
    def default_fcm_token(self) -> ParticipantFCMHistory:
        try:
            return self._default_fcm_token
        except AttributeError:
            pass
        
        self._default_fcm_token = ParticipantFCMHistory(
            token=self.DEFAULT_FCM_TOKEN, participant=self.default_participant
        )
        self._default_fcm_token.save()
        return self._default_fcm_token
    
    @property
    def default_participant_field_value(self) -> ParticipantFieldValue:
        try:
            return self._default_participant_field_value
        except AttributeError:
            pass
        self._default_participant_field_value = self.generate_participant_field_value(
            self.default_study_field, self.default_participant, self.DEFAULT_PARTICIPANT_FIELD_VALUE
        )
        return self._default_participant_field_value
    
    def generate_participant_field_value(
        self,
        study_field: StudyField,
        participant: Participant,
        value: str = None
    ) -> ParticipantFieldValue:
        pfv = ParticipantFieldValue(
            participant=participant,
            field=study_field,
            value=value if value else self.DEFAULT_PARTICIPANT_FIELD_VALUE,
        )
        pfv.save()
        return pfv
    
    @property
    def generate_10_default_participants(self) -> List[Participant]:
        return [self.generate_participant(self.session_study) for _ in range(10)]
    
    def generate_participant(
        self, study: Study, patient_id: str = None, ios=False, device_id=None, **kwargs,
    ) -> Participant:
        participant = Participant(
            patient_id=patient_id or generate_easy_alphanumeric_string(),
            os_type=IOS_API if ios else ANDROID_API,
            study=study,
            device_id=device_id or self.DEFAULT_PARTICIPANT_DEVICE_ID,
            password=self.SOME_SHA1_PASSWORD_COMPONENTS,
            **kwargs,
        )
        participant.set_password(self.DEFAULT_PARTICIPANT_PASSWORD)  # saves
        return participant
    
    def generate_fcm_token(self, participant: Participant, unregistered_datetime: datetime = None):
        token = ParticipantFCMHistory(
            participant=participant,
            token="token-" + generate_easy_alphanumeric_string(),
            unregistered=unregistered_datetime,
        )
        token.save()
        return token
    
    @property
    def default_populated_intervention_date(self) -> InterventionDate:
        try:
            return self._default_populated_intervention_date
        except AttributeError:
            pass
        self._default_populated_intervention_date = self.generate_intervention_date(
            self.default_participant, self.default_intervention, self.CURRENT_DATE
        )
        return self._default_populated_intervention_date
    
    @property
    def default_unpopulated_intervention_date(self) -> InterventionDate:
        try:
            return self._default_unpopulated_intervention_date
        except AttributeError:
            pass
        self._default_unpopulated_intervention_date = \
            self.generate_intervention_date(self.default_participant, self.default_intervention)
        return self._default_unpopulated_intervention_date
    
    def generate_intervention_date(
        self, participant: Participant, intervention: Intervention, date: date = None
    ) -> InterventionDate:
        intervention_date = InterventionDate(
            participant=participant, intervention=intervention, date=date
        )
        intervention_date.save()
        return intervention_date
    
    def generate_file_to_process(
        self,
        path: str,
        study: Study = None,
        participant: Participant = None,
        deleted: bool = False,
        os_type: str = NULL_OS,
    ):
        ftp = FileToProcess(
            s3_file_path=path,
            study=study or self.default_study,
            participant=participant or self.default_participant,
            deleted=deleted,
            os_type=os_type,
        )
        ftp.save()
        return ftp
    
    @property
    def default_participant_deletion_event(self):
        # note that the DEFAULT participant deletion object has its last_updated time backdated by
        # 42 minutes.  This is to make it easier to test with as the participant data deletion won't
        # start/restart until the last_updated time is at least 30 minutes ago.
        try:
            return self._default_participant_deletion_event
        except AttributeError:
            pass
        self._default_participant_deletion_event = self.generate_participant_deletion_event(
            self.default_participant,
            confirmed=None,
            last_updated=timezone.now() - timedelta(minutes=(REQUIRED_PARTICIPANT_PURGE_AGE_MINUTES*2))
        )
        return self._default_participant_deletion_event
    
    def generate_participant_deletion_event(
        self,
        participant: Participant,
        deleted_count: int = 0,
        confirmed: datetime = None,
        last_updated: datetime = None
    ) -> ParticipantDeletionEvent:
        
        deletion_event = ParticipantDeletionEvent(
            participant=participant, files_deleted_count=deleted_count, purge_confirmed_time=confirmed
        )
        deletion_event.save()
        # logic to update the last_updated time is here because its an auto_now field
        if last_updated:
            ParticipantDeletionEvent.objects.filter(pk=deletion_event.pk).update(last_updated=last_updated)
        deletion_event.refresh_from_db()
        
        return deletion_event
    
    #
    ## Heartbeats
    #
    
    def generate_heartbeat(self, participant: Participant = None, time: datetime = None):
        if time is None:
            time = timezone.now()
        if participant is None:
            participant = self.default_participant
        heartbeat = AppHeartbeats(participant=participant, timestamp=time, message="test")
        heartbeat.save()
        return heartbeat
    
    #
    # Schedules
    #
    
    def generate_participant_action_log(
        self, participant: Participant = None, time: datetime = None
    ):
        if time is None:
            time = timezone.now()
        if participant is None:
            participant = self.default_participant
        heartbeat = ParticipantActionLog(participant=participant, timestamp=time, action="test")
        heartbeat.save()
        return heartbeat
    
    def generate_weekly_schedule(
        self,
        survey: Survey = None,
        day_of_week: int = 0,
        hour: int = 0,
        minute: int = 0
    ) -> WeeklySchedule:
        weekly = WeeklySchedule(
            survey=survey or self.default_survey,
            day_of_week=day_of_week,
            hour=hour,
            minute=minute,
        )
        weekly.save()
        return weekly
    
    @property
    def default_relative_schedule(self) -> RelativeSchedule:
        try:
            return self._default_relative_schedule
        except AttributeError:
            pass
        self._default_relative_schedule = \
            self.generate_relative_schedule(self.default_survey)
        return self._default_relative_schedule
    
    def generate_relative_schedule(
        self,
        survey: Survey,
        intervention: Intervention = None,
        days_after: int = 0,
        hours_after: int = 0,
        minutes_after: int = 0,
    ) -> RelativeSchedule:
        relative = RelativeSchedule(
            survey=survey or self.default_survey,
            intervention=intervention or self.default_intervention,
            days_after=days_after,
            hour=hours_after,
            minute=minutes_after,
        )
        relative.save()
        return relative
    
    def generate_absolute_schedule(
        self,
        a_date: date,
        survey: Survey = None,
        hour: int = 0,
        minute: int = 0,
    ) -> AbsoluteSchedule:
        absolute = AbsoluteSchedule(
            survey=survey or self.default_survey,
            date=a_date,
            hour=hour,
            minute=minute,
        )
        absolute.save()
        return absolute
    
    def generate_absolute_schedule_from_datetime(self, survey: Survey, a_dt: datetime):
        absolute = AbsoluteSchedule(
            survey=survey or self.default_survey,
            date=a_dt.date(),
            hour=a_dt.hour,
            minute=a_dt.minute,
        )
        absolute.save()
        return absolute
    
    #
    ## ScheduledEvents
    #
    
    def generate_easy_absolute_scheduled_event_with_absolute_schedule(self, time: datetime, **kwargs) -> ScheduledEvent:
        """ Note that no intervention is marked, this just creates the schedule basics. """
        schedule = self.generate_absolute_schedule_from_datetime(self.default_survey, time)
        return self.generate_scheduled_event(
            self.default_survey, self.default_participant, schedule, time, **kwargs
        )
    
    def generate_easy_relative_schedule_event_with_relative_schedule(self, event_time_offset_now: timedelta):
        """ Note that no intervention is marked, this just creates the schedule basics """
        now = timezone.now() + event_time_offset_now
        schedule = self.generate_relative_schedule(
            self.default_survey,
            self.default_intervention,
            days_after=event_time_offset_now.days,
            hours_after=event_time_offset_now.seconds // 60 // 60,  # the offset isn't perfect but 
            minutes_after=event_time_offset_now.seconds // 60 % 60,  # this is fine for tests...
        )
        return self.generate_scheduled_event(
            self.default_survey, self.default_participant, schedule, now
        )
    
    def generate_a_real_weekly_schedule_event_with_schedule(
        self, day_of_week: int = 0, hour: int = 0, minute: int = 0, tz: tzinfo = None
    ) -> Tuple[ScheduledEvent, int]:
        raise NotImplementedError("take the code below and paste it into your test, or use an absolute survey")
        # 0,0,0 is a sunday at midnight
        sched = self.generate_weekly_schedule(self.default_survey, day_of_week, hour, minute)
        repopulate_weekly_survey_schedule_events(self.default_survey)
        events = ScheduledEvent.objects.filter(weekly_schedule=sched)
    
    def generate_scheduled_event(
        self,
        survey: Survey,
        participant: Participant,
        schedule: Schedule,
        time: datetime,
        a_uuid: uuid.UUID = None
    ) -> ScheduledEvent:
        scheduled_event = ScheduledEvent(
            survey=survey,
            participant=participant,
            weekly_schedule=schedule if isinstance(schedule, WeeklySchedule) else None,
            relative_schedule=schedule if isinstance(schedule, RelativeSchedule) else None,
            absolute_schedule=schedule if isinstance(schedule, AbsoluteSchedule) else None,
            scheduled_time=time,
            deleted=False,
            uuid=a_uuid if a_uuid else uuid.uuid4(),
            most_recent_event=None,
        )
        scheduled_event.save()
        return scheduled_event
    
    #
    ## ArchivedEvent
    #
    
    def generate_archived_event(
        self,
        survey: Survey,
        participant: Participant,
        schedule_type: str = None,
        scheduled_time: datetime = None,
        status: str = None,
        a_uuid: uuid.UUID = None
    ):
        archived_event = ArchivedEvent(
            survey_archive=survey.archives.first(),
            participant=participant,
            schedule_type=schedule_type or ScheduleTypes.weekly,
            scheduled_time=scheduled_time or timezone.now(),
            status=status or MESSAGE_SEND_SUCCESS,
            uuid=a_uuid if a_uuid else uuid.uuid4(),
        )
        archived_event.save()
        return archived_event
    
    def bulk_generate_archived_events(
        self, quantity: int, survey: Survey, participant: Participant, schedule_type: str = None,
        scheduled_time: datetime = None, status: str = None
    ):
        events = [
            ArchivedEvent(
                survey_archive=survey.archives.first(),
                participant=participant,
                schedule_type=schedule_type or ScheduleTypes.weekly,
                scheduled_time=scheduled_time or timezone.now(),
                status=status or MESSAGE_SEND_SUCCESS,
            )
            for _ in range(quantity)
        ]
        return ArchivedEvent.objects.bulk_create(events)
    
    def generate_archived_event_for_absolute_schedule(self, absolute: AbsoluteSchedule, a_uuid: uuid.UUID = None):
        # absolute is super easy
        return self.generate_archived_event(
            absolute.survey,
            self.default_participant,
            ScheduleTypes.absolute,
            absolute.event_time(self.default_study.timezone),
            a_uuid=a_uuid
        )
    
    def generate_archived_event_for_relative_schedule(
        self, relative: RelativeSchedule, participant: Participant = None, override_tz=tzinfo
    ):
        if not relative.intervention:
            raise ValueError("relative schedule must have an intervention")
        
        if not participant.intervention_dates.filter(intervention=relative.intervention).exists():
            raise ValueError("participant must have an intervention date shared with the relative schedule")
        
        the_intervention_date: date = participant.intervention_dates.filter(
            intervention=relative.intervention
        ).values_list("date", flat=True).get()
        
        # at some point we have to call real code for the relative schedule to get the output time,
        # the point of this is to generate a correct one.
        the_time = relative.notification_time_from_intervention_date_and_timezone(
            the_intervention_date + timedelta(days=1), participant.timezone
        )
        
        return self.generate_archived_event(
            relative.survey,
            participant,
            ScheduleTypes.relative,
            # status defaults to the success message, MESSAGE_SEND_SUCCESS
            the_time,
        )
    
    #
    ## Forest Task
    #
    
    @property
    def default_forest_task(self) -> ForestTask:
        try:
            return self._default_forest_task
        except AttributeError:
            pass
        self._default_forest_task = self.generate_forest_task(self.default_participant)
        return self._default_forest_task
    
    def generate_forest_task(
        self,
        participant: Participant = None,
        data_date_start: datetime = timezone.now(),    # generated once at import time. will differ,
        data_date_end: datetime = timezone.now(),      # slightly, but end is always after start.
        forest_tree: str = ForestTree.jasmine,
        **kwargs
    ):
        task = ForestTask(
            participant=participant or self.default_participant,
            data_date_start=data_date_start,
            data_date_end=data_date_end,
            forest_tree=forest_tree,
            status=ForestTaskStatus.queued,
            **kwargs
        )
        task.save()
        return task
    
    
    #
    ## ChunkRegistry
    #
    
    @property
    def default_chunkregistry(self) -> ChunkRegistry:
        # the default chunkrestry object is an identifiers instance, this is likely irrelevant.
        try:
            return self._default_chunkregistry
        except AttributeError:
            self._default_chunkregistry = self.generate_chunkregistry(
                self.session_study, self.default_participant, IDENTIFIERS
            )
            return self._default_chunkregistry
    
    def generate_chunkregistry(
        self,
        study: Study,
        participant: Participant,
        data_type: str,
        path: str = None,
        hash_value: str = None,
        time_bin: datetime = None,
        file_size: int = None,
        survey: Survey = None,
        is_chunkable: bool = False,
    ) -> ChunkRegistry:
        chunk_reg = ChunkRegistry(
            study=study,
            participant=participant,
            data_type=data_type,
            chunk_path=path or generate_easy_alphanumeric_string(),
            chunk_hash=hash_value or generate_easy_alphanumeric_string(),
            time_bin=time_bin or timezone.now(),
            file_size=file_size or 0,
            is_chunkable=is_chunkable,
            survey=survey,
        )
        chunk_reg.save()
        return chunk_reg
    
    #
    ## SummaryStatisticDaily
    #
    
    @property
    def default_summary_statistic_daily(self) -> SummaryStatisticDaily:
        try:
            return self._default_summary_statistic_daily
        except AttributeError:
            # its empty, this is ok
            self._default_summary_statistic_daily = self.generate_summary_statistic_daily()
            return self._default_summary_statistic_daily
    
    def generate_summary_statistic_daily(self, a_date: date = None, participant: Participant = None) -> SummaryStatisticDaily:
        field_dict = self.default_summary_statistic_daily_cheatsheet()
        params = {}
        for field in SummaryStatisticDaily._meta.fields:
            if field.name in ["id", "created_on", "last_updated", "jasmine_task", "willow_task", "sycamore_task", "oak_task"]:
                continue
            elif field.name == "participant":
                params[field.name] = participant or self.default_participant
            elif field.name == "date":
                params[field.name] = a_date or date.today()
            else:
                params[field.name] = field_dict[field.name]
        stats = SummaryStatisticDaily(**params)
        stats.save()
        return stats
    
    def default_summary_statistic_daily_cheatsheet(self):
        # this is used to populate default values in a SummaryStatisticDaily in a way that creates
        # legible output when something goes wrong.  The meaning of these values is literally never
        # important in the context of the Beiwe Backend, they are purely hosted for download.
        field_dict = {}
        for i, field in enumerate(SummaryStatisticDaily._meta.fields):
            if isinstance(field, (ForeignKey, DateField, AutoField)):
                continue
            elif isinstance(field, IntegerField):
                field_dict[field.name] = i
            elif isinstance(field, FloatField):
                field_dict[field.name] = float(i)
            elif isinstance(field, (TextField, CharField)):
                field_dict[field.name] = str(i)
            else:
                raise TypeError(f"encountered unhandled SummaryStatisticDaily type: {type(field)}")
        return field_dict
    
    #
    ## DeviceStatusReportHistory
    #
    
    def generate_device_status_report_history(
            self, participant: Participant = None,
            app_os: str = ANDROID_API,
            os_version: str = "1.0",
            app_version: str = "1.0",
            endpoint: str = "test",
            compressed_report: bytes = b"empty",
    ):
        report = DeviceStatusReportHistory(
            participant=participant or self.default_participant,
            app_os=app_os,
            os_version=os_version,
            app_version=app_version,
            endpoint=endpoint,
            compressed_report=compressed_report,
        )
        report.save()
        return report
    
    #
    ## Push notification common setup
    #
    
    @property
    def set_working_push_notification_basices(self):
        # we are not testing fcm token details in these tests.
        self.default_participant.update(deleted=False, permanently_retired=False)
        self.default_fcm_token
    
    @property
    def set_working_heartbeat_notification_fully_valid(self):
        # we will set last upload to as our active field, it can be any of the active fields.
        # after calling this function the default participant should be found by the heartbeat query
        now = timezone.now()
        self.default_participant.update(
            deleted=False, permanently_retired=False, last_upload=now - timedelta(minutes=61),
        )
        self.default_fcm_token
    
    @property
    def set_default_participant_all_push_notification_features(self):
        self.default_participant.update(
            deleted=False,
            permanently_retired=False,
            last_version_name=IOS_APP_MINIMUM_PUSH_NOTIFICATION_RESEND_VERSION,
            os_type=IOS_API,
        )
        self.default_fcm_token


def compare_dictionaries(reference, comparee, ignore=None):
    """ Compares two dictionary objects and displays the differences in a useful fashion. """
    
    if not isinstance(reference, dict):
        raise Exception("reference was %s, not dictionary" % type(reference))
    if not isinstance(comparee, dict):
        raise Exception("comparee was %s, not dictionary" % type(comparee))
    
    if ignore is None:
        ignore = []
    
    b = set((x, y) for x, y in comparee.items() if x not in ignore)
    a = set((x, y) for x, y in reference.items() if x not in ignore)
    differences_a = a - b
    differences_b = b - a
    
    if len(differences_a) == 0 and len(differences_b) == 0:
        return True
    
    try:
        differences_a = sorted(differences_a)
        differences_b = sorted(differences_b)
    except Exception:
        pass
    
    print("These dictionaries are not identical:")
    if differences_a:
        print("in reference, not in comparee:")
        for x, y in differences_a:
            print("\t", x, y)
    if differences_b:
        print("in comparee, not in reference:")
        for x, y in differences_b:
            print("\t", x, y)
    
    return False


class ParticipantTableHelperMixin:
    """ We have 2 instances of tests needing this, purpose is as a hardcoded clone that of the
    output of the participant_table_data.get_table_columns function. """
    
    HEADER_1 = ",".join(("Created On", "Patient ID", "Status", "OS Type")) + ","  # trailing comma
    HEADER_2 = ",".join((
        "First Registration Date",
        "Last Registration",
        "Last Upload",
        "Last Survey Download",
        "Last Set Password",
        "Last Push Token Update",
        "Last Device Settings Update",
        "Last OS Version",
        "App Version Code",
        "App Version Name",
        "Last Heartbeat"
    )) + "\r\n"
    
    def header(self, intervention: bool = False, custom_field: bool = False) -> str:
        ret = self.HEADER_1
        
        if intervention:
            ret += "default_intervention_name,"
        if custom_field:
            ret += "default_study_field_name,"
        
        ret += self.HEADER_2
        return ret


class DummyThreadPool():
    """ a dummy threadpool object because the test suite has weird problems with ThreadPool """
    def __init__(self, *args, **kwargs) -> None:
        pass
    
    # @staticmethod
    def imap_unordered(self, func, iterable, **kwargs):
        # we actually want to cut off any threadpool args, which is conveniently easy because map
        # does not use kwargs
        return map(func, iterable)
    
    # @staticmethod
    def terminate(self):
        pass
    
    # @staticmethod
    def close(self):
        pass


def render_test_html_file(response: HttpResponse, url: str):
    print("\nwriting url:", url)
    
    with open(CURRENT_TEST_HTML_FILEPATH, "wb") as f:
        f.write(response.content.replace(b"/static/", ABS_STATIC_ROOT))
    
    subprocess.check_call(["google-chrome", CURRENT_TEST_HTML_FILEPATH])
    x = input(f"opening {url} rendered html, press enter to continue test(s) or anything else to exit.")
    if x:
        exit()

from typing import Dict, Union

from django.db.models import Manager, QuerySet
from django.http.request import HttpRequest
from django.http.response import HttpResponse, HttpResponseRedirect

from database.dashboard_models import DashboardColorSetting, DashboardGradient, DashboardInflection
from database.data_access_models import ChunkRegistry, FileToProcess, IOSDecryptionKey
from database.forest_models import ForestTask, SummaryStatisticDaily
from database.profiling_models import EncryptionErrorMetadata, LineEncryptionError, UploadTracking
from database.schedule_models import (AbsoluteSchedule, ArchivedEvent, Intervention,
    InterventionDate, RelativeSchedule, ScheduledEvent, WeeklySchedule)
from database.security_models import ApiKey
from database.study_models import DeviceSettings, Study, StudyField
from database.survey_models import Survey, SurveyArchive
from database.system_models import FileAsText, GenericEvent
from database.user_models_participant import (Participant, ParticipantFCMHistory,
    ParticipantFieldValue, PushNotificationDisabledEvent)
from database.user_models_researcher import Researcher, StudyRelation


""" This file includes types and typing information that may be missing from your
developmentenvironment or your IDE, as well as some useful type hints. """

#
## Request objects
#

class ResearcherRequest(HttpRequest):
    # these attributes are present on the normal researcher endpoints
    session_researcher: Researcher


class ApiStudyResearcherRequest(HttpRequest):
    api_researcher: Researcher
    api_study: Study


class ApiResearcherRequest(HttpRequest):
    api_researcher: Researcher


class ParticipantRequest(HttpRequest):
    session_participant: Participant


class TableauRequest(HttpRequest): pass

# used in tests
ResponseOrRedirect = Union[HttpResponse, HttpResponseRedirect]

#
## Other classes
#

# Go ahead and add to these, until we update to 3.10 we can't use the | operator for Union
StrOrBytes = Union[str, bytes]
DictOfStrStr = Dict[str, str]
DictOfStrInt = Dict[str, int]
DictOfIntStr = Dict[int, str]
DictOfIntInt = Dict[int, int]

# used in s3
StrOrParticipantOrStudy = Union[str, Participant, Study]

# A Schedule
Schedule = Union[WeeklySchedule, RelativeSchedule, AbsoluteSchedule]


# Generated with scripts/generate_typing_hax.py on 2022-10-04
AbsoluteScheduleQuerySet = QuerySet[AbsoluteSchedule]
ApiKeyQuerySet = QuerySet[ApiKey]
ArchivedEventQuerySet = QuerySet[ArchivedEvent]
ChunkRegistryQuerySet = QuerySet[ChunkRegistry]
DashboardColorSettingQuerySet = QuerySet[DashboardColorSetting]
DashboardGradientQuerySet = QuerySet[DashboardGradient]
DashboardInflectionQuerySet = QuerySet[DashboardInflection]
DeviceSettingsQuerySet = QuerySet[DeviceSettings]
EncryptionErrorMetadataQuerySet = QuerySet[EncryptionErrorMetadata]
FileAsTextQuerySet = QuerySet[FileAsText]
FileToProcessQuerySet = QuerySet[FileToProcess]
ForestTaskQuerySet = QuerySet[ForestTask]
GenericEventQuerySet = QuerySet[GenericEvent]
IOSDecryptionKeyQuerySet = QuerySet[IOSDecryptionKey]
InterventionDateQuerySet = QuerySet[InterventionDate]
InterventionQuerySet = QuerySet[Intervention]
LineEncryptionErrorQuerySet = QuerySet[LineEncryptionError]
ParticipantFCMHistoryQuerySet = QuerySet[ParticipantFCMHistory]
ParticipantFieldValueQuerySet = QuerySet[ParticipantFieldValue]
ParticipantQuerySet = QuerySet[Participant]
PushNotificationDisabledEventQuerySet = QuerySet[PushNotificationDisabledEvent]
RelativeScheduleQuerySet = QuerySet[RelativeSchedule]
ResearcherQuerySet = QuerySet[Researcher]
ScheduledEventQuerySet = QuerySet[ScheduledEvent]
StudyFieldQuerySet = QuerySet[StudyField]
StudyQuerySet = QuerySet[Study]
StudyRelationQuerySet = QuerySet[StudyRelation]
SummaryStatisticDailyQuerySet = QuerySet[SummaryStatisticDaily]
SurveyArchiveQuerySet = QuerySet[SurveyArchive]
SurveyQuerySet = QuerySet[Survey]
UploadTrackingQuerySet = QuerySet[UploadTracking]
WeeklyScheduleQuerySet = QuerySet[WeeklySchedule]

AbsoluteScheduleManager = Manager[AbsoluteSchedule]
ApiKeyManager = Manager[ApiKey]
ArchivedEventManager = Manager[ArchivedEvent]
ChunkRegistryManager = Manager[ChunkRegistry]
DashboardColorSettingManager = Manager[DashboardColorSetting]
DashboardGradientManager = Manager[DashboardGradient]
DashboardInflectionManager = Manager[DashboardInflection]
DeviceSettingsManager = Manager[DeviceSettings]
FileToProcessManager = Manager[FileToProcess]
InterventionDateManager = Manager[InterventionDate]
InterventionManager = Manager[Intervention]
ParticipantFCMHistoryManager = Manager[ParticipantFCMHistory]
ParticipantFieldValueManager = Manager[ParticipantFieldValue]
ParticipantManager = Manager[Participant]
RelativeScheduleManager = Manager[RelativeSchedule]
ScheduledEventManager = Manager[ScheduledEvent]
StudyFieldManager = Manager[StudyField]
StudyRelationManager = Manager[StudyRelation]
SummaryStatisticDailyManager = Manager[SummaryStatisticDaily]
SurveyArchiveManager = Manager[SurveyArchive]
SurveyManager = Manager[Survey]
UploadTrackingManager = Manager[UploadTracking]
WeeklyScheduleManager = Manager[WeeklySchedule]

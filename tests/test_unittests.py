# trunk-ignore-all(bandit/B101,bandit/B106,ruff/B018,ruff/E701)
import hashlib
import time
import unittest
import uuid
from datetime import datetime, timedelta
from io import BytesIO
from typing import Optional
from unittest.mock import _Call, MagicMock, Mock, patch

import dateutil
from dateutil.tz import gettz
from django.utils import timezone

from constants.common_constants import API_TIME_FORMAT, CHUNKS_FOLDER, EASTERN, UTC
from constants.data_stream_constants import SURVEY_ANSWERS, SURVEY_TIMINGS, VOICE_RECORDING
from constants.message_strings import (ERR_ANDROID_REFERENCE_VERSION_CODE_DIGITS,
    ERR_ANDROID_TARGET_VERSION_DIGITS, ERR_IOS_REFERENCE_VERSION_NAME_FORMAT,
    ERR_IOS_TARGET_VERSION_FORMAT, ERR_IOS_VERSION_COMPONENTS_DIGITS,
    ERR_TARGET_VERSION_CANNOT_BE_MISSING, ERR_TARGET_VERSION_MUST_BE_STRING, ERR_UNKNOWN_OS_TYPE)
from constants.s3_constants import (COMPRESSED_DATA_MISSING_AT_UPLOAD,
    COMPRESSED_DATA_MISSING_ON_POP, COMPRESSED_DATA_PRESENT_AT_COMPRESSION,
    COMPRESSED_DATA_PRESENT_ON_ASSIGNMENT, COMPRESSED_DATA_PRESENT_ON_DOWNLOAD,
    IOSDataRecoveryDisabledException, UNCOMPRESSED_DATA_MISSING_AT_COMPRESSION,
    UNCOMPRESSED_DATA_MISSING_ON_POP, UNCOMPRESSED_DATA_PRESENT_ON_ASSIGNMENT,
    UNCOMPRESSED_DATA_PRESENT_ON_DOWNLOAD, UNCOMPRESSED_DATA_PRESENT_WRONG_AT_UPLOAD)
from constants.user_constants import ACTIVE_PARTICIPANT_FIELDS, ANDROID_API, IOS_API
from database.data_access_models import IOSDecryptionKey
from database.models import ArchivedEvent, S3File, ScheduledEvent
from database.profiling_models import EncryptionErrorMetadata, UploadTracking
from database.user_models_participant import (AppHeartbeats, AppVersionHistory,
    DeviceStatusReportHistory, Participant, ParticipantActionLog, ParticipantDeletionEvent,
    PushNotificationDisabledEvent, SurveyNotificationReport)
from libs.aes import encrypt_for_server
from libs.celery_control import DebugCeleryApp
from libs.endpoint_helpers.participant_table_helpers import determine_registered_status
from libs.file_processing.utility_functions_simple import (BadTimecodeError, binify_from_timecode,
    convert_unix_to_human_readable_timestamps)
from libs.participant_purge import (confirm_deleted, get_all_file_path_prefixes,
    run_next_queued_participant_data_deletion)
from libs.s3 import BadS3PathException, decrypt_server, NoSuchKeyException, S3Storage
from libs.streaming_zip import determine_base_file_name
from libs.utils.compression import compress
from libs.utils.forest_utils import get_forest_git_hash
from libs.utils.participant_app_version_comparison import (is_this_version_gt_participants,
    is_this_version_gte_participants, is_this_version_lt_participants,
    is_this_version_lte_participants)
from tests.common import CommonTestCase


COUNT_OF_PATHS_RETURNED_FROM_GET_ALL_FILE_PATH_PREFIXES = 4


# Decorator for class instance methods that injects these three mocks, used in data purge tests.
# @patch('libs.participant_purge.s3_list_files')
# @patch('libs.participant_purge.s3_delete_many_versioned')
# @patch('libs.participant_purge.s3_list_versions')
# These patches are for the database table deletions.  s3_list_files specifically would result in an
# assertion error stating that the base s3 file path is not empty, so we patch that in the rest of
# the tests, which are database purge tests.
def data_purge_mock_s3_calls(func):
    s3_delete_many_versioned: MagicMock = patch('libs.participant_purge.s3_delete_many_versioned')
    s3_list_files: MagicMock = patch('libs.participant_purge.s3_list_files')
    s3_list_versions: MagicMock = patch('libs.participant_purge.s3_list_versions')
    s3_list_files.return_value = []
    s3_list_versions.return_value = []
    s3_delete_many_versioned.return_value = []
    def wrapper(self, *args, **kwargs):
        with s3_delete_many_versioned, s3_list_files, s3_list_versions:
            return func(self, *args, **kwargs)
    return wrapper


class TestBinifyFromTimecode(unittest.TestCase):
    def test_binify_from_timecode_short_str(self):
        # str(int(time.mktime(datetime(2023, 1, 10, 2, 13, 7, 453914, tzinfo=dateutil.tz.UTC).timetuple()))
        self.assertEqual(binify_from_timecode('1673316787'), 464810)
    
    def test_binify_from_timecode_short_bytes(self):
        self.assertEqual(binify_from_timecode(b'1673316787'), 464810)
    
    def test_binify_from_timecode_long_bytes(self):
        self.assertEqual(binify_from_timecode(b'1673316787111'), 464810)
    
    def test_binify_from_timecode_long_str(self):
        # str(int(time.mktime(datetime(2023, 1, 10, 2, 13, 7, 453914, tzinfo=dateutil.tz.UTC).timetuple()))
        self.assertEqual(binify_from_timecode('1673316787222'), 464810)
    
    def test_binify_from_timecode_too_early(self):
        # should be 1 second too early
        self.assertRaises(BadTimecodeError, binify_from_timecode, b'1406851199')
    
    def test_binify_from_timecode_too_late(self):
        self.assertRaises(BadTimecodeError, binify_from_timecode, b'9999999999')
    
    def test_binify_from_timecode_91_days(self):
        timestamp = str(int(time.mktime((datetime.utcnow() + timedelta(days=91)).timetuple())))
        self.assertRaises(BadTimecodeError, binify_from_timecode, timestamp.encode())


class TestDatabaseCriticalDetails(CommonTestCase):
    
    def test_scheduled_event_deletion_does_not_delete_archived_event(self):
        self.set_working_heartbeat_notification_fully_valid
        self.set_default_participant_all_push_notification_features
        event = self.generate_easy_absolute_scheduled_event_with_absolute_schedule(timezone.now())
        archive = self.generate_archived_event_from_scheduled_event(event)
        self.assertEqual(archive.uuid, event.uuid)
        self.assertEqual(ScheduledEvent.objects.count(), 1)
        self.assertEqual(ArchivedEvent.objects.count(), 1)
        event.delete()
        self.assertEqual(ScheduledEvent.objects.count(), 0)
        self.assertEqual(ArchivedEvent.objects.count(), 1)
    
    def test_scheduled_event_deletion_does_not_delete_archived_event_2(self):
        self.set_working_heartbeat_notification_fully_valid
        self.set_default_participant_all_push_notification_features
        event = self.generate_easy_absolute_scheduled_event_with_absolute_schedule(timezone.now())
        archive = self.generate_archived_event_from_scheduled_event(event)
        self.assertEqual(archive.uuid, event.uuid)
        self.assertEqual(ScheduledEvent.objects.count(), 1)
        self.assertEqual(ArchivedEvent.objects.count(), 1)
        ScheduledEvent.objects.all().delete()
        self.assertEqual(ScheduledEvent.objects.count(), 0)
        self.assertEqual(ArchivedEvent.objects.count(), 1)


class TestParticipantDataDeletion(CommonTestCase):
    
    def assert_default_participant_end_state(self):
        self.default_participant.refresh_from_db()
        self.assertEqual(self.default_participant.deleted, True)
        self.assertEqual(self.default_participant.easy_enrollment, False)
        self.assertEqual(self.default_participant.permanently_retired, True)
        self.assertEqual(self.default_participant.device_id, "")
        self.assertEqual(self.default_participant.os_type, "")
    
    def assert_correct_s3_parameters_called(
        self,
        s3_list_versions: MagicMock,
        s3_list_files: MagicMock,
        s3_delete_many_versioned: MagicMock,
        list_versions_count: int = COUNT_OF_PATHS_RETURNED_FROM_GET_ALL_FILE_PATH_PREFIXES,
        list_files_count: int = COUNT_OF_PATHS_RETURNED_FROM_GET_ALL_FILE_PATH_PREFIXES,
        delete_versioned_count: int = COUNT_OF_PATHS_RETURNED_FROM_GET_ALL_FILE_PATH_PREFIXES,
    ):
        # sanity checks to save our butts
        self.assertEqual(s3_list_versions._mock_name, "s3_list_versions")
        self.assertEqual(s3_list_files._mock_name, "s3_list_files")
        self.assertEqual(s3_delete_many_versioned._mock_name, "s3_delete_many_versioned")
        self.assertEqual(s3_list_versions.call_count, list_versions_count)
        self.assertEqual(s3_list_files.call_count, list_files_count)
        # tests that call this function should implement their own assertions on the number of calls
        # to and parameters to s3_delete_many_versioned.
        self.assertEqual(s3_delete_many_versioned.call_count, delete_versioned_count)
        
        path_keys, path_participant, path_chunked, path_problems = get_all_file_path_prefixes(self.default_participant)
        if list_files_count == COUNT_OF_PATHS_RETURNED_FROM_GET_ALL_FILE_PATH_PREFIXES:
            self.assertEqual(s3_list_files.call_args_list[0].args[0], path_keys)
            self.assertEqual(s3_list_files.call_args_list[1].args[0], path_participant)
            self.assertEqual(s3_list_files.call_args_list[2].args[0], path_chunked)
            self.assertEqual(s3_list_files.call_args_list[3].args[0], path_problems)
        if list_versions_count == COUNT_OF_PATHS_RETURNED_FROM_GET_ALL_FILE_PATH_PREFIXES:
            self.assertEqual(s3_list_versions.call_args_list[0].args[0], path_keys)
            self.assertEqual(s3_list_versions.call_args_list[1].args[0], path_participant)
            self.assertEqual(s3_list_versions.call_args_list[2].args[0], path_chunked)
            self.assertEqual(s3_list_versions.call_args_list[3].args[0], path_problems)
    
    def test_no_participants_at_all(self):
        self.assertFalse(Participant.objects.exists())
        run_next_queued_participant_data_deletion()
        self.assertFalse(Participant.objects.exists())
    
    def test_no_participant_but_with_a_participant_in_the_db(self):
        last_update = self.default_participant.last_updated  # create!
        self.assertEqual(Participant.objects.count(), 1)
        self.assertEqual(ParticipantDeletionEvent.objects.count(), 0)
        run_next_queued_participant_data_deletion()
        self.assertEqual(Participant.objects.count(), 1)
        self.assertEqual(ParticipantDeletionEvent.objects.count(), 0)
        self.default_participant.refresh_from_db()
        self.assertEqual(last_update, self.default_participant.last_updated)
    
    #! REMINDER: ordering of these inserts parameters is in reverse order of declaration. You can
    #  confirm the correct mock target by looking at the _mock_name (or vars) of the mock object.
    @patch('libs.participant_purge.s3_delete_many_versioned', return_value=[])
    @patch('libs.participant_purge.s3_list_files', return_value=[])
    @patch('libs.participant_purge.s3_list_versions', return_value=[])
    def test_deleting_data_for_one_empty_participant(
        self, s3_list_versions: MagicMock, s3_list_files: MagicMock, s3_delete_many_versioned: MagicMock
    ):
        self.default_participant_deletion_event  # includes default_participant creation
        self.assertEqual(Participant.objects.count(), 1)
        run_next_queued_participant_data_deletion()
        self.assertEqual(Participant.objects.count(), 1)  # we don't actually delete the db object just the data...
        self.default_participant.refresh_from_db()
        self.assert_default_participant_end_state()
        self.assert_correct_s3_parameters_called(
            s3_list_versions, s3_list_files, s3_delete_many_versioned, delete_versioned_count=0
        )
        self.default_participant_deletion_event.refresh_from_db()
        self.assertEqual(self.default_participant_deletion_event.files_deleted_count, 0)
        self.assertIsInstance(self.default_participant_deletion_event.purge_confirmed_time, datetime)
    
    @patch('libs.participant_purge.s3_delete_many_versioned')
    @patch('libs.participant_purge.s3_list_files')
    @patch('libs.participant_purge.s3_list_versions')
    def test_deleting_errors_on_list(
        self, s3_list_versions: MagicMock, s3_list_files: MagicMock, s3_delete_many_versioned: MagicMock
    ):
        # s3_list_files should result in an assertion error stating that the base s3 file path is
        # not empty. in principle this may change the exact error, as long as it fails its working.
        s3_list_files.return_value = ["some_file"]
        self.default_participant_deletion_event
        self.assertRaises(AssertionError, run_next_queued_participant_data_deletion)
        # this should fail because the participant is not marked as deleted.
        self.assertRaises(AssertionError, self.assert_default_participant_end_state)
        self.assert_correct_s3_parameters_called(
            s3_list_versions, s3_list_files, s3_delete_many_versioned, list_files_count=1, delete_versioned_count=0)
        self.default_participant_deletion_event.refresh_from_db()
        self.assertIsNone(self.default_participant_deletion_event.purge_confirmed_time)
    
    @property
    def assert_confirm_deletion_raises_then_reset_last_updated(self):
        self.default_participant_deletion_event.refresh_from_db()
        last_updated = self.default_participant_deletion_event.last_updated
        self.assertRaises(AssertionError, confirm_deleted, self.default_participant_deletion_event)
        ParticipantDeletionEvent.objects.filter(
            pk=self.default_participant_deletion_event.pk).update(last_updated=last_updated)
    
    def test_assert_confirm_deletion_raises_then_reset_last_updated_works(self):
        class GoodError(Exception): pass
        text_1 ="this test should have raised an assertion error, " \
                "all database tests for TestParticipantDataDeletion invalidated."
        text_2 = text_1 + " (second instance)"
        text_1 = text_1 + " (first instance)"
        try:
            self.assert_confirm_deletion_raises_then_reset_last_updated
            raise GoodError(text_1)
        except AssertionError:
            FALSE_IF_IT_FAILED = False
        except GoodError:
            FALSE_IF_IT_FAILED = True
        assert FALSE_IF_IT_FAILED, text_2
    
    @data_purge_mock_s3_calls
    def test_confirm_ChunkRegistry(self):
        self.default_participant_deletion_event
        self.default_chunkregistry
        self.assert_confirm_deletion_raises_then_reset_last_updated
        run_next_queued_participant_data_deletion()
        confirm_deleted(self.default_participant_deletion_event)  # errors means test failure
    
    @data_purge_mock_s3_calls
    def test_confirm_SummaryStatisticDaily(self):
        self.default_summary_statistic_daily
        self.assert_confirm_deletion_raises_then_reset_last_updated
        run_next_queued_participant_data_deletion()
        confirm_deleted(self.default_participant_deletion_event)
    
    @data_purge_mock_s3_calls
    def test_confirm_IOSDecryptionKey(self):
        IOSDecryptionKey.objects.create(
            participant=self.default_participant, base64_encryption_key="abc123", file_name="abc123"
        )
        self.assert_confirm_deletion_raises_then_reset_last_updated
        run_next_queued_participant_data_deletion()
        confirm_deleted(self.default_participant_deletion_event)
    
    @data_purge_mock_s3_calls
    def test_confirm_ForestTask(self):
        self.generate_forest_task()
        self.assert_confirm_deletion_raises_then_reset_last_updated
        run_next_queued_participant_data_deletion()
        confirm_deleted(self.default_participant_deletion_event)
    
    @data_purge_mock_s3_calls
    def test_confirm_EncryptionErrorMetadata(self):
        EncryptionErrorMetadata.objects.create(
            file_name="a", total_lines=1, number_errors=1, error_lines="a", error_types="a", participant=self.default_participant
        )
        self.assert_confirm_deletion_raises_then_reset_last_updated
        run_next_queued_participant_data_deletion()
        confirm_deleted(self.default_participant_deletion_event)
    
    @data_purge_mock_s3_calls
    def test_confirm_FileToProcess(self):
        self.generate_file_to_process("a_path")
        self.assert_confirm_deletion_raises_then_reset_last_updated
        run_next_queued_participant_data_deletion()
        confirm_deleted(self.default_participant_deletion_event)
    
    @data_purge_mock_s3_calls
    def test_confirm_PushNotificationDisabledEvent(self):
        PushNotificationDisabledEvent.objects.create(participant=self.default_participant, count=1)
        self.assert_confirm_deletion_raises_then_reset_last_updated
        run_next_queued_participant_data_deletion()
        confirm_deleted(self.default_participant_deletion_event)
    
    @data_purge_mock_s3_calls
    def test_confirm_ParticipantFCMHistory(self):
        self.default_fcm_token
        self.assert_confirm_deletion_raises_then_reset_last_updated
        run_next_queued_participant_data_deletion()
        confirm_deleted(self.default_participant_deletion_event)
    
    @data_purge_mock_s3_calls
    def test_confirm_ParticipantFieldValue(self):
        self.default_participant_field_value
        self.assert_confirm_deletion_raises_then_reset_last_updated
        run_next_queued_participant_data_deletion()
        confirm_deleted(self.default_participant_deletion_event)
    @data_purge_mock_s3_calls
    def test_confirm_UploadTracking(self):
        UploadTracking.objects.create(
            file_path=" ", file_size=0, timestamp=timezone.now(), participant=self.default_participant
        )
        self.assert_confirm_deletion_raises_then_reset_last_updated
        run_next_queued_participant_data_deletion()
        confirm_deleted(self.default_participant_deletion_event)
    
    @data_purge_mock_s3_calls
    def test_confirm_ScheduledEvent(self):
        self.generate_easy_absolute_scheduled_event_with_absolute_schedule(timezone.now())
        self.assert_confirm_deletion_raises_then_reset_last_updated
        run_next_queued_participant_data_deletion()
        confirm_deleted(self.default_participant_deletion_event)
    
    @data_purge_mock_s3_calls
    def test_confirm_ArchivedEvent(self):
        sched_event = self.generate_easy_absolute_scheduled_event_with_absolute_schedule(timezone.now())
        sched_event.archive(self.default_participant, status="deleted")
        self.assert_confirm_deletion_raises_then_reset_last_updated
        run_next_queued_participant_data_deletion()
        confirm_deleted(self.default_participant_deletion_event)
    
    @data_purge_mock_s3_calls
    def test_confirm_InterventionDate(self):
        self.default_populated_intervention_date
        self.assert_confirm_deletion_raises_then_reset_last_updated
        run_next_queued_participant_data_deletion()
        confirm_deleted(self.default_participant_deletion_event)
    
    @data_purge_mock_s3_calls
    def test_confirm_AppHeartbeats(self):
        AppHeartbeats.create(self.default_participant, timezone.now())
        self.assert_confirm_deletion_raises_then_reset_last_updated
        run_next_queued_participant_data_deletion()
        confirm_deleted(self.default_participant_deletion_event)
    
    @data_purge_mock_s3_calls
    def test_confirm_ParticipantActionLog(self):
        # this test is weird, we create an action log inside the deletion event.
        
        self.default_participant_deletion_event
        self.assertEqual(ParticipantActionLog.objects.count(), 0)
        run_next_queued_participant_data_deletion()
        self.assertEqual(ParticipantActionLog.objects.count(), 2)
    
    @data_purge_mock_s3_calls
    def test_confirm_DeviceStatusReportHistory(self):
        self.default_participant.generate_device_status_report_history("some_endpoint_path")
        self.default_participant_deletion_event
        self.assertEqual(DeviceStatusReportHistory.objects.count(), 1)
        run_next_queued_participant_data_deletion()
        self.assertEqual(DeviceStatusReportHistory.objects.count(), 0)
    
    @data_purge_mock_s3_calls
    def test_confirm_AppVersionHistory(self):
        self.default_participant.generate_app_version_history("11", "11", "11")
        self.default_participant_deletion_event
        self.assertEqual(AppVersionHistory.objects.count(), 1)
        run_next_queued_participant_data_deletion()
        self.assertEqual(AppVersionHistory.objects.count(), 0)
    
    @data_purge_mock_s3_calls
    def test_confirm_SurveyNotificationReport(self):
        SurveyNotificationReport.objects.create(
            participant=self.default_participant, notification_uuid=uuid.uuid4())
        self.default_participant_deletion_event
        self.assertEqual(SurveyNotificationReport.objects.count(), 1)
        run_next_queued_participant_data_deletion()
        self.assertEqual(SurveyNotificationReport.objects.count(), 0)
    
    def test_confirm_S3File(self):
        # this is a weird test, we can't actually test the s3 file deletion, but we can test that
        # the function runs without error.
        self.default_participant_deletion_event
        S3File.objects.create(participant=self.default_participant, path="whatever")
        run_next_queued_participant_data_deletion()
        confirm_deleted(self.default_participant_deletion_event)
    
    def test_for_all_related_fields(self):
        # This test will fail whenever there is a new related model added to the codebase.
        for model in Participant._meta.related_objects:
            model_name = model.related_model.__name__
            # but not the deletion operation that's kinda important...
            if model_name == "ParticipantDeletionEvent":
                continue
            assert hasattr(TestParticipantDataDeletion, f"test_confirm_{model_name}"), \
                f"missing test_confirm_{model_name} for {model_name}"


class TestParticipantTimeZone(CommonTestCase):
    
    def test_defaults(self):
        # test the default is applied
        self.assertEqual(self.default_participant.timezone_name, "America/New_York")
        # test that the model default is actually America/New_York
        self.assertEqual(Participant._meta.get_field("timezone_name").default, "America/New_York")
        # test that timezone returns a timezone of America/New_York
        # test that the object returned is definitely the DATEUTIL timezone object
        # THIS TEST MAY NOT PASS ON NON-LINUX COMPUTERS? Will have to test mac, we don't actually support raw windows.
        self.assertIsInstance(self.default_participant.timezone, dateutil.tz.tzfile)
        # test that the timezone is the expected object
        self.assertIs(self.default_participant.timezone, EASTERN)
    
    def test_try_null(self):
        # discovered weird behavior where a None passed into gettz returns utc.
        try:
            self.default_participant.try_set_timezone(None)
            self.fail("should have raised a TypeError")
        except TypeError:
            pass  # it should raise a TypeError
    
    def test_try_empty_string(self):
        # discovered weird behavior where the empty string passed into gettz returns utc.
        try:
            self.default_participant.try_set_timezone("")
            self.fail("should have raised a TypeError")
        except TypeError:
            pass  # it should raise a TypeError
    
    def test_try_bad_string(self):
        # the unknown_timezone flag should be false at the start and true at the end.
        p = self.default_participant
        self.assertEqual(p.timezone_name, "America/New_York")
        self.assertIs(p.timezone, EASTERN)
        self.assertEqual(p.unknown_timezone, False)  # A
        self.default_study.update(timezone_name="UTC")
        p.try_set_timezone("a bad string")
        # behavior should be to grab the study's timezone name, which for tests was unexpectedly UTC...
        self.assertEqual(p.timezone_name, "UTC")
        self.assertIs(p.timezone, UTC)
        self.assertEqual(p.unknown_timezone, True)  # A
    
    def test_try_bad_string_resets_unknown_timezone(self):
        p = self.default_participant
        p.update_only(unknown_timezone=False)  # force value to false
        self.assertEqual(p.timezone_name, "America/New_York")
        self.assertIs(p.timezone, EASTERN)
        self.assertEqual(p.unknown_timezone, False)  # A
        self.default_study.update(timezone_name="UTC")
        p.try_set_timezone("a bad string")
        self.assertEqual(p.timezone_name, "UTC")
        self.assertIs(p.timezone, UTC)
        self.assertEqual(p.unknown_timezone, True)  # B
    
    def test_same_timezone_name_still_updates_unknown_timezone_flag(self):
        p = self.default_participant
        last_update = p.last_updated
        self.assertEqual(p.timezone_name, "America/New_York")
        self.assertIs(p.timezone, EASTERN)
        self.assertEqual(p.unknown_timezone, False)  # A
        p.try_set_timezone("America/New_York")
        self.assertEqual(p.timezone_name, "America/New_York")
        self.assertIs(p.timezone, EASTERN)
        self.assertEqual(p.unknown_timezone, False)  # B
        self.assertEqual(p.last_updated, last_update)
    
    def test_valid_input(self):
        # should change both the timezone and the unknown_timezone flag
        p = self.default_participant
        last_update = p.last_updated
        self.assertEqual(p.timezone_name, "America/New_York")
        self.assertIs(p.timezone, EASTERN)
        self.assertEqual(p.unknown_timezone, False)
        p.try_set_timezone("America/Los_Angeles")
        self.assertEqual(p.timezone_name, "America/Los_Angeles")
        self.assertIs(p.timezone, gettz("America/Los_Angeles"))
        self.assertEqual(p.unknown_timezone, False)
        self.assertEqual(p.last_updated, last_update)


class TestParticipantActive(CommonTestCase):
    """ We need a test for keeping the status of "this is an active participant" up to date across
    some distinct code paths """
    
    def test_determine_registered_status(self):
        # determine_registered_status is code in an important optimized codepath for the study page,
        # it can't be factored down to a call on a Participant object because it operates on contents
        # out of a values_list query.  It also deals with creating strings and needs to know if the
        # registered field is set (which we don't care about in other places).
        annotes = determine_registered_status.__annotations__
        correct_annotations = {
            'now': datetime,
            'registered': bool,
            'permanently_retired': bool,
            'last_upload': Optional[datetime],
            'last_get_latest_surveys': Optional[datetime],
            'last_set_password': Optional[datetime],
            'last_set_fcm_token': Optional[datetime],
            'last_get_latest_device_settings': Optional[datetime],
            'last_register_user': Optional[datetime],
            'last_heartbeat_checkin': Optional[datetime],
            'return': str,
        }
        self.assertEqual(annotes, correct_annotations)
    
    def test_participant_is_active_one_week_false(self):
        # this test is self referential...
        now = timezone.now()
        more_than_a_week_ago = now - timedelta(days=8)
        p = self.default_participant
        for field_outer in ACTIVE_PARTICIPANT_FIELDS:
            for field_inner in ACTIVE_PARTICIPANT_FIELDS:
                if field_inner != field_outer:
                    setattr(p, field_inner, None)
                else:
                    setattr(p, field_inner, more_than_a_week_ago)
            self.assertFalse(p.is_active_one_week)
    
    def test_participant_is_active_one_week_true(self):
        # this test is self referential... - nope it actually caught a bug! cool.
        now = timezone.now()
        less_than_a_week_ago = now - timedelta(days=6)
        p = self.default_participant
        
        for field_outer in ACTIVE_PARTICIPANT_FIELDS:
            for field_inner in ACTIVE_PARTICIPANT_FIELDS:
                # skip permanently_retired
                if "permanently_retired" in (field_inner, field_outer):
                    continue
                
                if field_inner != field_outer:
                    setattr(p, field_inner, None)
                else:
                    setattr(p, field_inner, less_than_a_week_ago)
            self.assertTrue(p.is_active_one_week)
        
        # test that a participant is INACTIVE if all of the fields are None
        p.permanently_retired = False
        for field_name in ACTIVE_PARTICIPANT_FIELDS:
            if field_name != "permanently_retired":
                setattr(p, field_name, None)
        self.assertFalse(p.is_active_one_week)
        
        # assert that it is still false if permanently_retired is set to true
        p.permanently_retired = True
        self.assertFalse(p.is_active_one_week)
        
        # test that permanently_retired overrides all fields being valid
        for field_name in ACTIVE_PARTICIPANT_FIELDS:
            setattr(p, field_name, less_than_a_week_ago)
        p.permanently_retired = True
        self.assertFalse(p.is_active_one_week)


class TestForestHash(unittest.TestCase):
    # todo: This is junk what even is this
    def test_get_forest_git_hash(self):
        hash = get_forest_git_hash()
        self.assertNotEqual(hash, "")


IOS = IOS_API
ANDRD = ANDROID_API
ANDRD_VALID = "9"
ANDROID_VALID_LESS = "8"
IOS_VALID = "2024.21"
IOS_VALID_LESS = "2024.20"

class TestAppVersionComparison(CommonTestCase):
    """
    Tests for the is_app_version_greater_than function, 
    Android versions should always ALWAYS be a string of only digits.
    IOS versions are either a git hash, "missing", string composed of year.build_number, or None
    """
    
    ## Successes
    def test_android_junk_in_name_works(self):
        self.assertTrue(is_this_version_gt_participants(ANDRD, ANDRD_VALID, ANDROID_VALID_LESS, "junk"))
    
    def test_ios_junk_in_code_works(self):
        self.assertTrue(is_this_version_gt_participants(IOS, IOS_VALID, "junk", IOS_VALID_LESS))
    
    def test_ios_leading_zero_target(self):
        self.assertFalse(is_this_version_gt_participants(IOS, "0.1", "junk", IOS_VALID))
    
    def test_ios_leading_zero_reference(self):
        self.assertTrue(is_this_version_gt_participants(IOS, IOS_VALID, "junk", "0.1"))
    
    def test_ios_double_leading_zero_target(self):
        self.assertFalse(is_this_version_gt_participants(IOS, "001.002", "junk", IOS_VALID))
    
    def test_ios_double_leading_zero_reference(self):
        self.assertTrue(is_this_version_gt_participants(IOS, IOS_VALID, "junk", "001.002"))
    
    def test_ios_ensure_we_arent_doing_string_comparison_target(self):
        self.assertTrue(is_this_version_gt_participants(IOS, "12024.21", "junk", "2024.21"))
    
    def test_ios_ensure_we_arent_doing_string_comparison_reference(self):
        self.assertFalse(is_this_version_gt_participants(IOS, "2024.21", "junk", "12024.21"))
    
    def test_android_ensure_we_arent_doing_string_comparison_target(self):
        self.assertTrue(is_this_version_gt_participants(ANDRD, "10", "9", "junk"))
    
    def test_android_ensure_we_arent_doing_string_comparison_reference(self):
        self.assertFalse(is_this_version_gt_participants(ANDRD, "9", "10", "junk"))
    
    # and then test gt, lt, lte, and gte
    def test_gt(self):
        self.assertTrue(is_this_version_gt_participants(ANDRD, ANDRD_VALID, ANDROID_VALID_LESS, "junk"))
        self.assertFalse(is_this_version_gt_participants(ANDRD, ANDRD_VALID, ANDRD_VALID, "junk"))
    
    def test_lt(self):
        self.assertTrue(is_this_version_lt_participants(ANDRD, ANDROID_VALID_LESS, ANDRD_VALID, "junk"))
        self.assertFalse(is_this_version_lt_participants(ANDRD, ANDRD_VALID, ANDRD_VALID, "junk"))
    
    def test_lte(self):
        self.assertTrue(is_this_version_lte_participants(ANDRD, ANDRD_VALID, ANDRD_VALID, "junk"))
        self.assertTrue(is_this_version_lte_participants(ANDRD, ANDROID_VALID_LESS, ANDRD_VALID, "junk"))
    
    def test_gte(self):
        self.assertTrue(is_this_version_gte_participants(ANDRD, ANDRD_VALID, ANDRD_VALID, "junk"))
        self.assertTrue(is_this_version_gte_participants(ANDRD, ANDRD_VALID, ANDROID_VALID_LESS, "junk"))
    
    ## Errors
    
    # bad os
    def test_none_os_version(self):
        with self.assertRaises(ValueError) as e_wrapper:
            is_this_version_gt_participants(None, "shouldn't matter", "or this", "or this")
        self.assertEqual(str(e_wrapper.exception), ERR_UNKNOWN_OS_TYPE(None))
    
    def test_none_everywhere(self):
        with self.assertRaises(ValueError) as e_wrapper:
            is_this_version_gt_participants(None, None, None, None)
        self.assertEqual(str(e_wrapper.exception), ERR_TARGET_VERSION_MUST_BE_STRING(type(None)))
    
    def test_bad_os_version(self):
        with self.assertRaises(ValueError) as e_wrapper:
            is_this_version_gt_participants("junk", "shouldn't matter", "or this", "or this")
        self.assertEqual(str(e_wrapper.exception), ERR_UNKNOWN_OS_TYPE("junk"))
    
    # bad target type
    def test_android_nonstring_target(self):
        with self.assertRaises(ValueError) as e_wrapper:
            is_this_version_gt_participants(ANDRD, object(), "junk", "junk")
        self.assertEqual(str(e_wrapper.exception), ERR_TARGET_VERSION_MUST_BE_STRING(type(object())))
    
    def test_ios_nonstring_target(self):
        with self.assertRaises(ValueError) as e_wrapper:
            is_this_version_gt_participants(IOS, object(), "junk", "junk")
        self.assertEqual(str(e_wrapper.exception), ERR_TARGET_VERSION_MUST_BE_STRING(type(object())))
    
    def test_android_None_target(self):
        with self.assertRaises(ValueError) as e_wrapper:
            is_this_version_gt_participants(ANDRD, None, ANDRD_VALID, "junk")
        self.assertEqual(str(e_wrapper.exception), ERR_TARGET_VERSION_MUST_BE_STRING(type(None)))
    
    def test_ios_None_target(self):
        with self.assertRaises(ValueError) as e_wrapper:
            is_this_version_gt_participants(IOS, None, "junk", IOS_VALID)
        self.assertEqual(str(e_wrapper.exception), ERR_TARGET_VERSION_MUST_BE_STRING(type(None)))
    
    # empty string target
    def test_android_empty_string_target(self):
        with self.assertRaises(ValueError) as e_wrapper:
            is_this_version_gt_participants(ANDRD, "", "junk", "junk")
        self.assertEqual(str(e_wrapper.exception), ERR_ANDROID_TARGET_VERSION_DIGITS(""))
    
    def test_ios_empty_string_target(self):
        with self.assertRaises(ValueError) as e_wrapper:
            is_this_version_gt_participants(IOS, "", "junk", "junk")
        self.assertEqual(str(e_wrapper.exception), ERR_IOS_TARGET_VERSION_FORMAT(""))
    
    # empty string reference
    def test_android_empty_string_reference(self):
        with self.assertRaises(ValueError) as e_wrapper:
            is_this_version_gt_participants(ANDRD, ANDRD_VALID, "", "junk")
        self.assertEqual(str(e_wrapper.exception), ERR_ANDROID_REFERENCE_VERSION_CODE_DIGITS(""))
    
    def test_ios_empty_string_reference(self):
        with self.assertRaises(ValueError) as e_wrapper:
            is_this_version_gt_participants(IOS, IOS_VALID, "junk", "")
        self.assertEqual(str(e_wrapper.exception), ERR_IOS_REFERENCE_VERSION_NAME_FORMAT(""))
    
    # "missing" as a target
    def test_android_target_missing(self):
        with self.assertRaises(ValueError) as e_wrapper:
            is_this_version_gt_participants(ANDRD, "missing", "junk", "junk")
        self.assertEqual(str(e_wrapper.exception), ERR_TARGET_VERSION_CANNOT_BE_MISSING)
    
    def test_ios_target_missing(self):
        with self.assertRaises(ValueError) as e_wrapper:
            is_this_version_gt_participants(IOS, "missing", "junk", "junk")
        self.assertEqual(str(e_wrapper.exception), ERR_TARGET_VERSION_CANNOT_BE_MISSING)
    
    # junk targets
    def test_android_junk_target(self):
        with self.assertRaises(ValueError) as e_wrapper:
            is_this_version_gt_participants(ANDRD, "junk", ANDRD_VALID, "junk")
        self.assertEqual(str(e_wrapper.exception), ERR_ANDROID_TARGET_VERSION_DIGITS("junk"))
    
    def test_ios_junk_target(self):
        with self.assertRaises(ValueError) as e_wrapper:
            is_this_version_gt_participants(IOS, "junk", "junk", IOS_VALID)
        self.assertEqual(str(e_wrapper.exception), ERR_IOS_TARGET_VERSION_FORMAT("junk"))
    
    # android floats
    def test_android_float_string_target(self):
        with self.assertRaises(ValueError) as e_wrapper:
            is_this_version_gt_participants(ANDRD, "1.1", ANDRD_VALID, "junk")
        self.assertEqual(str(e_wrapper.exception), ERR_ANDROID_TARGET_VERSION_DIGITS("1.1"))
    
    def test_android_float_target(self):
        with self.assertRaises(ValueError) as e_wrapper:
            is_this_version_gt_participants(ANDRD, 1.1, ANDRD_VALID, "junk")
        self.assertEqual(str(e_wrapper.exception), ERR_TARGET_VERSION_MUST_BE_STRING(float))
    
    # ios ints
    def test_ios_int_string_target(self):
        with self.assertRaises(ValueError) as e_wrapper:
            is_this_version_gt_participants(IOS, "1", "junk", IOS_VALID)
        self.assertEqual(str(e_wrapper.exception), ERR_IOS_TARGET_VERSION_FORMAT("1"))
    
    def test_ios_int_string_reference(self):
        with self.assertRaises(ValueError) as e_wrapper:
            is_this_version_gt_participants(IOS, IOS_VALID, "junk", "1")
        self.assertEqual(str(e_wrapper.exception), ERR_IOS_REFERENCE_VERSION_NAME_FORMAT("1"))
    
    def test_ios_float_target(self):
        with self.assertRaises(ValueError) as e_wrapper:
            is_this_version_gt_participants(IOS, 1.1, "junk", IOS_VALID)
        self.assertEqual(str(e_wrapper.exception), ERR_TARGET_VERSION_MUST_BE_STRING(float))
    
    # semantic version
    def test_ios_longer_semantic_version_target(self):
        with self.assertRaises(ValueError) as e_wrapper:
            is_this_version_gt_participants(IOS, "2024.21.1", "junk", IOS_VALID)
        self.assertEqual(str(e_wrapper.exception), ERR_IOS_TARGET_VERSION_FORMAT("2024.21.1"))
    
    def test_android_longer_semantic_reference(self):
        with self.assertRaises(ValueError) as e_wrapper:
            is_this_version_gt_participants(ANDRD, "2024.21.1", ANDRD_VALID, "junk")
        self.assertEqual(str(e_wrapper.exception), ERR_ANDROID_TARGET_VERSION_DIGITS("2024.21.1"))
    
    # ios weird periods
    def test_ios_leading_period_target(self):
        with self.assertRaises(ValueError) as e_wrapper:
            is_this_version_gt_participants(IOS, ".2024", "junk", IOS_VALID)
        self.assertEqual(str(e_wrapper.exception), ERR_IOS_VERSION_COMPONENTS_DIGITS(".2024", IOS_VALID))
    
    def test_ios_trailing_period_target(self):
        with self.assertRaises(ValueError) as e_wrapper:
            is_this_version_gt_participants(IOS, "2024.", "junk", IOS_VALID)
        self.assertEqual(str(e_wrapper.exception), ERR_IOS_VERSION_COMPONENTS_DIGITS("2024.", IOS_VALID))
    
    def test_ios_trailing_period_reference(self):
        with self.assertRaises(ValueError) as e_wrapper:
            is_this_version_gt_participants(IOS, IOS_VALID, "junk", "2024.")
        self.assertEqual(str(e_wrapper.exception), ERR_IOS_VERSION_COMPONENTS_DIGITS(IOS_VALID, "2024."))
    
    def test_ios_leading_period_reference(self):
        with self.assertRaises(ValueError) as e_wrapper:
            is_this_version_gt_participants(IOS, IOS_VALID, "junk", ".2024")
        self.assertEqual(str(e_wrapper.exception), ERR_IOS_VERSION_COMPONENTS_DIGITS(IOS_VALID, ".2024"))


class TestS3Storage(CommonTestCase):
    """ Tests for the S3Storage class is libs.s3 """
    
    # due to REALLY STUPID DYNAMIC ERROR CLASS GENERATION IN BOTO3.... we have some code that checks
    # the name of class type because it cannot be imported.  to test this code we need an error class
    # that has that same name
    # class NoSuchKeyException(Exception): pass
    
    
    def hack_s3_error(self, s: str):
        from libs.s3 import NoSuchKeyException
        return NoSuchKeyException(s)
    
    COMPRESSED_SLUG = compress(b"content")
    ENCRYPTED_SLUG = encrypt_for_server(b"content", CommonTestCase.DEFAULT_ENCRYPTION_KEY_BYTES)
    COMPRESSED_ENCRYPTED_SLUG = encrypt_for_server(COMPRESSED_SLUG, CommonTestCase.DEFAULT_ENCRYPTION_KEY_BYTES)
    
    @property
    def valid_path_for_bypass_false(self):
        return "a_path"
    
    @property
    def valid_study_path(self):
        return f"{self.default_study.object_id}/a_path"
    
    @property
    def valid_non_study_path(self):
        return CHUNKS_FOLDER + "/" + self.valid_study_path
    
    @property
    def default_s3storage_with_prefix(self):
        # participant is probably the best test....
        return S3Storage(self.valid_path_for_bypass_false, self.default_participant, bypass_study_folder=False)
    
    @property
    def default_s3storage_without_prefix(self):
        return S3Storage(self.valid_non_study_path, self.default_participant, bypass_study_folder=True)
    
    ### WITH PREFIX
    
    def params_for_upload_compressed_study_prefix(self):
        return dict(
            Body=self.COMPRESSED_SLUG,
            Bucket='test_bucket',
            Key=f'{self.default_study.object_id}/a_path.zst'
        )
    
    def params_for_download_compressed_study_prefix(self):
        return dict(
            Bucket='test_bucket',
            Key=f'{self.default_study.object_id}/a_path.zst',
            ResponseContentType='string'
        )
    
    def params_for_download_UNCOMPRESSED_study_prefix(self):
        return dict(
            Bucket='test_bucket',
            Key=f'{self.default_study.object_id}/a_path',
            ResponseContentType='string'
        )
    
    def params_for_delete_UNCOMPRESSED_study_prefix(self):
        return dict(Bucket='test_bucket', Key=f'{self.default_study.object_id}/a_path')
    
    ### WITHOUT PREFIX
    
    def params_for_upload_compressed_non_study_prefix(self):
        return dict(
            Body=self.COMPRESSED_SLUG,
            Bucket='test_bucket',
            Key=self.valid_non_study_path + '.zst'
        )
    
    def params_for_download_compressed_non_study_prefix(self):
        return dict(
            Bucket='test_bucket',
            Key=self.valid_non_study_path + '.zst',
            ResponseContentType='string'
        )
    
    def params_for_download_UNCOMPRESSED_non_study_prefix(self):
        return dict(
            Bucket='test_bucket', Key=self.valid_non_study_path, ResponseContentType='string'
        )
    
    def params_for_delete_UNCOMPRESSED_non_study_prefix(self):
        return dict(Bucket='test_bucket', Key=self.valid_non_study_path)
    
    ################################################################################################
    
    def assert_hasattr(self, obj, attr):
        self.assertTrue(hasattr(obj, attr))
    
    def assert_not_hasattr(self, obj, attr):
        self.assertFalse(hasattr(obj, attr))
    
    def extract_mock_call_params(self, m: MagicMock) -> list[_Call]:
        # typing this is stupid
        return [c for c in m.method_calls]
    
    def decrypt_kwarg_Body(self, kwargs):
        kwargs["Body"] = decrypt_server(kwargs["Body"], self.DEFAULT_ENCRYPTION_KEY_BYTES)
    
    ################################################################################################
    
    # test working instantiation
    
    def test_participant_instantiation(self):
        s = S3Storage(self.valid_path_for_bypass_false, self.default_participant, bypass_study_folder=False)
        self.assertEqual(s.s3_path_zst, self.valid_study_path + ".zst")
        self.assertEqual(s.s3_path_uncompressed, self.valid_study_path)
        self.assertIs(s.smart_key_obj, self.default_participant)
        self.assert_not_hasattr(s, "uncompressed_data")
        self.assertEqual(self.default_study.encryption_key, s.encryption_key.decode())
        self.assertEqual(self.default_study.object_id, s.get_path_prefix)
    
    def test_objectid_instantiation(self):
        s = S3Storage(self.valid_path_for_bypass_false, self.default_study.object_id, bypass_study_folder=False)
        self.assertEqual(s.s3_path_zst, self.valid_study_path + ".zst")
        self.assertEqual(s.s3_path_uncompressed, self.valid_study_path)
        self.assertIs(s.smart_key_obj, self.default_study.object_id)
        self.assert_not_hasattr(s, "uncompressed_data")
        self.assertEqual(self.default_study.encryption_key, s.encryption_key.decode())
        self.assertEqual(self.default_study.object_id, s.get_path_prefix)
    
    def test_study_instantiation(self):
        s = S3Storage(self.valid_path_for_bypass_false, self.default_study, bypass_study_folder=False)
        self.assertEqual(s.s3_path_zst, self.valid_study_path + ".zst")
        self.assertEqual(s.s3_path_uncompressed, self.valid_study_path)
        self.assertIs(s.smart_key_obj, self.default_study)
        self.assert_not_hasattr(s, "uncompressed_data")
        self.assertEqual(self.default_study.encryption_key, s.encryption_key.decode())
        self.assertEqual(self.default_study.object_id, s.get_path_prefix)
    
    def test_paths_rejected(self):
        with self.assertRaises(BadS3PathException):
            S3Storage(f"{self.valid_study_path}.zst", self.default_participant, bypass_study_folder=False)
        with self.assertRaises(BadS3PathException):
            S3Storage("CHUNKED_DATA/a_path.zst", self.default_participant, bypass_study_folder=True)
        
        S3Storage("PROBLEM_UPLOADS/a_path.csv", self.default_participant, bypass_study_folder=False)
        with self.assertRaises(IOSDataRecoveryDisabledException):
            S3Storage("PROBLEM_UPLOADS/a_path.csv", self.default_participant, bypass_study_folder=True)
        
        S3Storage("CUSTOM_ONDEPLOY_SCRIPT/EB/a_path.csv", self.default_participant, bypass_study_folder=False)
        with self.assertRaises(BadS3PathException):
            S3Storage("CUSTOM_ONDEPLOY_SCRIPT/EB/a_path.csv", self.default_participant, bypass_study_folder=True)
        
        S3Storage("CUSTOM_ONDEPLOY_SCRIPT/PROCESSING/a_path.csv", self.default_participant, bypass_study_folder=False)
        with self.assertRaises(BadS3PathException):
            S3Storage("CUSTOM_ONDEPLOY_SCRIPT/PROCESSING/a_path.csv", self.default_participant, bypass_study_folder=True)
        
        S3Storage("CUSTOM_ONDEPLOY_SCRIPT/new/a_path.csv", self.default_participant, bypass_study_folder=False)
        with self.assertRaises(BadS3PathException):
            S3Storage("CUSTOM_ONDEPLOY_SCRIPT/new/a_path.csv", self.default_participant, bypass_study_folder=True)
        
        self.assertFalse(S3File.objects.exists())
    
    def test_paths_allowed(self):
        # special case NOT rejected starting with study path
        obj_id = self.default_study.object_id
        S3Storage(obj_id+"/"+"path", self.default_participant, bypass_study_folder=True)
        S3Storage(obj_id+"/"+"path", self.default_study, bypass_study_folder=True)
        S3Storage(obj_id+"/"+"path", self.default_study.object_id, bypass_study_folder=True)
        self.assertFalse(S3File.objects.exists())
    
    ## test some critical inner funcitonality
    
    def test_get_cache_encryption_key(self):
        s = self.default_s3storage_with_prefix
        self.assert_not_hasattr(s, "_encryption_key")
        self.assertEqual(s.encryption_key, self.DEFAULT_ENCRYPTION_KEY_BYTES)
        self.assert_hasattr(s, "_encryption_key")
    
    def test_set_pop_uncompressed_file_contents(self):
        s = self.default_s3storage_with_prefix
        self.assert_not_hasattr(s, "uncompressed_data")
        self.assert_not_hasattr(s, "compressed_data")
        self.assertRaises(TypeError, s.set_file_content_uncompressed, "content")
        s.set_file_content_uncompressed(b"content")
        self.assertEqual(s.pop_uncompressed_file_content(), b"content")
        self.assert_not_hasattr(s, "uncompressed_data")
        self.assert_not_hasattr(s, "compressed_data")
        self.assertFalse(S3File.objects.exists())
    
    def test_compress_data_and_clear_uncompressed(self):
        s = self.default_s3storage_with_prefix
        s.set_file_content_uncompressed(b"content")
        s.compress_data_and_clear_uncompressed()
        self.assert_not_hasattr(s, "uncompressed_data")
        self.assert_hasattr(s, "compressed_data")
        self.assertEqual(s.compressed_data, self.COMPRESSED_SLUG)
        self.assertFalse(S3File.objects.exists())
    
    def test_set_pop_file_content_compressed(self):
        s = self.default_s3storage_with_prefix
        self.assert_not_hasattr(s, "compressed_data")
        self.assert_not_hasattr(s, "uncompressed_data")
        s.set_file_content_compressed(self.COMPRESSED_SLUG)
        self.assert_not_hasattr(s, "uncompressed_data")
        self.assert_hasattr(s, "compressed_data")
        self.assertEqual(s.compressed_data, self.COMPRESSED_SLUG)
        self.assertFalse(S3File.objects.exists())
        # pop
        s.pop_compressed_file_content()
        self.assert_not_hasattr(s, "compressed_data")
    
    # S3Storage tests, requires a with prefix and without prefix version of each test.
    
    ## push_to_storage_and_clear_everything
    
    @patch("libs.s3.conn")
    def test_compress_and_push_to_storage_and_clear_memory_with_prefix(self, conn=MagicMock()):
        # this is a sufficient test of compress_and_push_to_storage_retaining_compressed because
        # compress_and_push_to_storage_and_clear_memory_with_prefix calls it directly.
        s = self.default_s3storage_with_prefix
        s.set_file_content_uncompressed(b"content")
        self.assertFalse(S3File.objects.exists())
        s.compress_and_push_to_storage_and_clear_memory()
        self.assert_not_hasattr(s, "uncompressed_data")
        self.assert_not_hasattr(s, "compressed_data")
        self.assertEqual(len(conn.method_calls), 1)
        call = self.extract_mock_call_params(conn)[0]
        self.assertIn("call.put_object(", str(call))
        self.decrypt_kwarg_Body(call.kwargs)
        self.assertEqual(call.kwargs, self.params_for_upload_compressed_study_prefix())
        
        s3_file = S3File.objects.get()
        self.assertEqual(s3_file.path, self.valid_study_path + ".zst")
        self.assert_correct_uploaded_s3file(s3_file)
    
    @patch("libs.s3.conn")
    def test_compress_and_push_to_storage_and_clear_memory_without_prefix(self, conn=MagicMock()):
        s = self.default_s3storage_without_prefix
        s.set_file_content_uncompressed(b"content")
        s.compress_and_push_to_storage_and_clear_memory()
        self.assert_not_hasattr(s, "uncompressed_data")
        self.assert_not_hasattr(s, "compressed_data")
        self.assertEqual(len(conn.method_calls), 1)
        call = self.extract_mock_call_params(conn)[0]
        self.assertIn("call.put_object(", str(call))
        self.decrypt_kwarg_Body(call.kwargs)
        self.assertEqual(call.kwargs, self.params_for_upload_compressed_non_study_prefix())
        
        s3_file = S3File.objects.get()
        self.assertEqual(s3_file.path, self.valid_non_study_path + ".zst")
        self.assert_correct_uploaded_s3file(s3_file)
    
    def test_compress_data_and_clear_uncompressed_without_data(self):
        s = self.default_s3storage_with_prefix
        e = self.assertRaisesRegex(
            AssertionError,
            UNCOMPRESSED_DATA_MISSING_AT_COMPRESSION,
            s.compress_data_and_clear_uncompressed,
            
        )
    
    def test_compress_and_push_to_storage_and_clear_memory_without_data(self):
        s = self.default_s3storage_without_prefix
        self.assertRaisesRegex(
            AssertionError,
            UNCOMPRESSED_DATA_MISSING_AT_COMPRESSION,
            s.compress_and_push_to_storage_and_clear_memory,
        )
        s.set_file_content_compressed(self.COMPRESSED_SLUG)
        self.assertRaisesRegex(
            AssertionError,
            UNCOMPRESSED_DATA_MISSING_AT_COMPRESSION,
            s.compress_and_push_to_storage_and_clear_memory,
        )
    
    def test_precompressed_upload__compress_and_push_to_storage_and_clear_memory(self):
        s = self.default_s3storage_with_prefix
        s.set_file_content_compressed(self.COMPRESSED_SLUG)
        self.assertFalse(S3File.objects.exists())
        self.assertRaisesRegex(
            AssertionError,
            UNCOMPRESSED_DATA_MISSING_AT_COMPRESSION,
            s.compress_and_push_to_storage_and_clear_memory,
        )
        self.assertRaisesRegex(
            AssertionError,
            COMPRESSED_DATA_PRESENT_ON_ASSIGNMENT,
            s.set_file_content_uncompressed,
            self.COMPRESSED_SLUG
        )
        s.pop_compressed_file_content()
        s.set_file_content_uncompressed(self.COMPRESSED_SLUG)
        s.compress_and_push_to_storage_and_clear_memory()
    
    def test_COMPRESSED_DATA_PRESENT_AT_COMPRESSION(self):
        s = self.default_s3storage_with_prefix
        s.set_file_content_compressed(self.COMPRESSED_SLUG)
        s.uncompressed_data = b"fake"
        self.assertRaisesRegex(
            AssertionError,
            COMPRESSED_DATA_PRESENT_AT_COMPRESSION,
            s.compress_data_and_clear_uncompressed,
        )
    
    def test_UNCOMPRESSED_DATA_MISSING_ON_POP(self):
        s = self.default_s3storage_with_prefix
        self.assertRaisesRegex(
            AssertionError,
            UNCOMPRESSED_DATA_MISSING_ON_POP,
            s.pop_uncompressed_file_content,
        )
    
    def test_COMPRESSED_DATA_MISSING_ON_POP(self):
        s = self.default_s3storage_with_prefix
        self.assertRaisesRegex(
            AssertionError,
            COMPRESSED_DATA_MISSING_ON_POP,
            s.pop_compressed_file_content,
        )
    
    def test_COMPRESSED_DATA_MISSING_AT_UPLOAD(self):
        s = self.default_s3storage_with_prefix
        self.assertRaisesRegex(
            AssertionError,
            COMPRESSED_DATA_MISSING_AT_UPLOAD,
            s.push_to_storage_already_compressed_and_clear_memory,
        )
    
    def test_UNCOMPRESSED_DATA_PRESENT_WRONG_AT_UPLOAD(self):
        s = self.default_s3storage_with_prefix
        s.set_file_content_compressed(self.COMPRESSED_SLUG)
        s.uncompressed_data = b"fake, illegal"
        self.assertRaisesRegex(
            AssertionError,
            UNCOMPRESSED_DATA_PRESENT_WRONG_AT_UPLOAD.replace("(", r"\(").replace(")", r"\)"),  # has
            s.push_to_storage_already_compressed_and_clear_memory,
        )
    
    @patch("libs.s3.conn")
    def test_UNCOMPRESSED_DATA_PRESENT_ON_DOWNLOAD(self, conn=MagicMock()):
        conn.get_object = do_retrieve = Mock()
        do_retrieve.side_effect = [
            {"Body": BytesIO(self.COMPRESSED_ENCRYPTED_SLUG)},
            # self.hack_s3_error("waka waka 2"),
        ]
        s = self.default_s3storage_with_prefix
        s.set_file_content_uncompressed(b"content")
        self.assertRaisesRegex(
            AssertionError,
            UNCOMPRESSED_DATA_PRESENT_ON_DOWNLOAD.replace("(", r"\(").replace(")", r"\)"),  # has
            s.download_no_decompress,
        )
        self.assertRaisesRegex(
            AssertionError,
            UNCOMPRESSED_DATA_PRESENT_ON_DOWNLOAD.replace("(", r"\(").replace(")", r"\)"),  # has
            s.download
        )
    
    @patch("libs.s3.conn")
    def test_COMPRESSED_DATA_PRESENT_ON_DOWNLOAD(self, conn=MagicMock()):
        conn.get_object = do_retrieve = Mock()
        do_retrieve.side_effect = [
            {"Body": BytesIO(self.COMPRESSED_ENCRYPTED_SLUG)},
            # self.hack_s3_error("waka waka 2"),
        ]
        s = self.default_s3storage_with_prefix
        s.set_file_content_compressed(self.COMPRESSED_SLUG)
        # s.uncompressed_data = b"fake, illegal"
        self.assertRaisesRegex(
            AssertionError,
            COMPRESSED_DATA_PRESENT_ON_DOWNLOAD.replace("(", r"\(").replace(")", r"\)"),  # has
            s.download_no_decompress,
        )
        self.assertRaisesRegex(
            AssertionError,
            COMPRESSED_DATA_PRESENT_ON_DOWNLOAD.replace("(", r"\(").replace(")", r"\)"),  # has
            s.download
        )
    
    def test_UNCOMPRESSED_DATA_PRESENT_ON_ASSIGNMENT(self):
        s = self.default_s3storage_with_prefix
        s.set_file_content_uncompressed(b"content")
        self.assertRaisesRegex(
            AssertionError,
            UNCOMPRESSED_DATA_PRESENT_ON_ASSIGNMENT,
            s.set_file_content_compressed,
            self.COMPRESSED_SLUG
        )
    
    @patch("libs.s3.conn")
    def test_precompressed_upload___push_to_storage_already_compressed(self, conn=MagicMock()):
        s = self.default_s3storage_with_prefix
        s.set_file_content_compressed(self.COMPRESSED_SLUG)
        self.assertFalse(S3File.objects.exists())
        s.push_to_storage_already_compressed_and_clear_memory()
        
        self.assert_not_hasattr(s, "uncompressed_data")
        self.assert_not_hasattr(s, "compressed_data")
        
        self.assertEqual(len(conn.method_calls), 1)  # validate only one call
        call = self.extract_mock_call_params(conn)[0]
        self.assertIn("call.put_object(", str(call))  # it was put on on s3`
        self.decrypt_kwarg_Body(call.kwargs)
        self.assertEqual(call.kwargs, self.params_for_upload_compressed_study_prefix())
        
        s3_file = S3File.objects.get()
        self.assertEqual(s3_file.path, self.valid_study_path + ".zst")
        self.assert_correct_uploaded_s3file_already_compressed(s3_file)
    
    ## download
    
    @patch("libs.s3.conn")
    def test_download_with_prefix(self, conn=MagicMock()):
        # I thought I would need to use this extra mock object but.... no it just works and inserts
        # that value as the return to conn.get_object.  (cool)
        conn.get_object = MagicMock(return_value={"Body": BytesIO(self.COMPRESSED_ENCRYPTED_SLUG)})
        s = self.default_s3storage_with_prefix
        self.assertFalse(S3File.objects.exists())
        
        s.download()  # this fully downloads the file and decompresses it.
        
        self.assertEqual(len(conn.method_calls), 1)
        call = self.extract_mock_call_params(conn)[0]
        self.assertIn("call.get_object(", str(call))
        self.assertEqual(call.kwargs, self.params_for_download_compressed_study_prefix())
        self.assertEqual(s.uncompressed_data, b"content")
        self.assert_not_hasattr(s, "compressed_data")
        
        s3_file = S3File.objects.get()
        self.assertEqual(s3_file.path, self.valid_study_path + ".zst")
        self.assert_correct_downloaded_s3file(s3_file)
    
    @patch("libs.s3.conn")
    def test_download_without_prefix(self, conn=MagicMock()):
        conn.get_object = MagicMock(return_value={"Body": BytesIO(self.COMPRESSED_ENCRYPTED_SLUG)})
        s = self.default_s3storage_without_prefix
        s.download()
        
        self.assertEqual(len(conn.method_calls), 1)
        call = self.extract_mock_call_params(conn)[0]
        self.assertIn("call.get_object(", str(call))
        self.assertEqual(call.kwargs, self.params_for_download_compressed_non_study_prefix())
        self.assertEqual(s.uncompressed_data, b"content")
        self.assert_not_hasattr(s, "compressed_data")
        
        s3_file = S3File.objects.get()
        self.assertEqual(s3_file.path, self.valid_non_study_path + ".zst")
        self.assert_correct_downloaded_s3file(s3_file)
    
    ## download does not exist at all
    
    @patch("libs.s3.conn")
    def test_download_does_not_exist(self, conn: Mock):
        conn.get_object = do_retrieve = Mock()
        do_retrieve.side_effect = [
            self.hack_s3_error("waka waka 1"),
            self.hack_s3_error("waka waka 2"),
        ]
        s = self.default_s3storage_with_prefix
        self.assertRaises(NoSuchKeyException, s.download)
        kwarg_1 = {'Bucket': 'test_bucket', 'Key': self.valid_study_path+".zst", 'ResponseContentType': 'string'}
        kwarg_2 = {'Bucket': 'test_bucket', 'Key': self.valid_study_path, 'ResponseContentType': 'string'}
        self.assertEqual(conn.method_calls[0].kwargs, kwarg_1)
        self.assertEqual(conn.method_calls[1].kwargs, kwarg_2)
        self.assertEqual(len(conn.method_calls), 2)
        self.assertFalse(S3File.objects.exists())
    
    def test_download_does_not_exist_and_delete_s3file(self):
        self.valid_study_path
        S3File(path=self.valid_study_path+".zst").save()
        self.test_download_does_not_exist()
    
    ## download compressed does not exist
    
    @patch("libs.s3.conn")
    def test_download_compressed_does_not_exist_with_prefix(self, conn: Mock):
        conn.get_object = do_retrieve = Mock()
        # conn.put_object = _do_upload = MagicMock()  # ok interesting, don't need
        
        # hits retrieve [compressed, fail]
        # retrieve [uncompressed, success]
        # upload [compressed, success]   (no side effects)
        # delete [uncompressed, success]   (no side effects)
        do_retrieve.side_effect = [
            self.hack_s3_error("waka waka 1"),
            {"Body": BytesIO(self.ENCRYPTED_SLUG)},
        ]
        
        s = self.default_s3storage_with_prefix
        s.download()
        self.assertEqual(len(conn.method_calls), 4)
        call_1 = self.extract_mock_call_params(conn)[0]
        call_2 = self.extract_mock_call_params(conn)[1]
        call_3 = self.extract_mock_call_params(conn)[2]
        call_4 = self.extract_mock_call_params(conn)[3]
        # absolutely cannot work out how to access the name of method called in any other way.
        self.assertIn("call.get_object(", str(call_1))
        self.assertEqual(call_1.kwargs, self.params_for_download_compressed_study_prefix())
        self.assertIn("call.get_object(", str(call_2))
        self.assertEqual(call_2.kwargs, self.params_for_download_UNCOMPRESSED_study_prefix())
        self.assertIn("call.put_object(", str(call_3))
        self.decrypt_kwarg_Body(call_3.kwargs)
        self.assertEqual(call_3.kwargs, self.params_for_upload_compressed_study_prefix())
        self.assertEqual(call_4.kwargs, self.params_for_delete_UNCOMPRESSED_study_prefix())
        self.assertIn("call.delete_object(", str(call_4))
        
        # there should be one s3 file, it is an upload
        s3_file = S3File.objects.get()
        self.assertEqual(s3_file.path, self.valid_study_path + ".zst")
        self.assert_correct_uploaded_s3file(s3_file)
    
    @patch("libs.s3.conn")
    def test_download_compressed_does_not_exist_without_prefix(self, conn: Mock):
        conn.get_object = do_retrieve = Mock()
        
        do_retrieve.side_effect = [
            self.hack_s3_error("waka waka 1"),
            {"Body": BytesIO(self.ENCRYPTED_SLUG)},
        ]
        
        s = self.default_s3storage_without_prefix
        s.download()
        self.assertEqual(len(conn.method_calls), 4)
        call_1 = self.extract_mock_call_params(conn)[0]
        call_2 = self.extract_mock_call_params(conn)[1]
        call_3 = self.extract_mock_call_params(conn)[2]
        call_4 = self.extract_mock_call_params(conn)[3]
        # absolutely cannot work out how to access the name of method called in any other way.
        self.assertIn("call.get_object(", str(call_1))
        self.assertEqual(call_1.kwargs, self.params_for_download_compressed_non_study_prefix())
        self.assertIn("call.get_object(", str(call_2))
        self.assertEqual(call_2.kwargs, self.params_for_download_UNCOMPRESSED_non_study_prefix())
        self.assertIn("call.put_object(", str(call_3))
        self.decrypt_kwarg_Body(call_3.kwargs)
        self.assertEqual(call_3.kwargs, self.params_for_upload_compressed_non_study_prefix())
        self.assertEqual(call_4.kwargs, self.params_for_delete_UNCOMPRESSED_non_study_prefix())
        self.assertIn("call.delete_object(", str(call_4))
        
        s3_file = S3File.objects.get()
        self.assertEqual(s3_file.path, self.valid_non_study_path + ".zst")
        self.assert_correct_uploaded_s3file(s3_file)
    
    # S3File assertions
    
    def assert_correct_downloaded_s3file(self, s3_file: S3File):
        self.assertIsNotNone(s3_file.path)
        self.assertEqual(s3_file.size_uncompressed, len(b"content"))
        self.assertEqual(s3_file.size_compressed, len(self.COMPRESSED_SLUG))
        self.assertEqual(s3_file.study, self.default_study)
        self.assertEqual(s3_file.participant, self.default_participant)
        self.assertIsNone(s3_file.compression_time_ns)
        self.assertIsNone(s3_file.encryption_time_ns)
        self.assertIsNone(s3_file.upload_time_ns)
        self.assertIsNotNone(s3_file.decrypt_time_ns)
        self.assertIsNotNone(s3_file.download_time_ns)
        self.assertIsNotNone(s3_file.decompression_time_ns)
        self.assertIsNone(s3_file.sha1)
    
    def assert_correct_uploaded_s3file(self, s3_file: S3File):
        self.assertIsNotNone(s3_file.path)
        self.assertEqual(s3_file.size_uncompressed, len(b"content"))
        self.assertEqual(s3_file.size_compressed, len(self.COMPRESSED_SLUG))
        self.assertEqual(s3_file.study, self.default_study)
        self.assertEqual(s3_file.participant, self.default_participant)
        self.assertIsNotNone(s3_file.compression_time_ns)
        self.assertIsNotNone(s3_file.encryption_time_ns)
        self.assertIsNotNone(s3_file.upload_time_ns)
        self.assertIsNone(s3_file.decrypt_time_ns)
        self.assertIsNone(s3_file.download_time_ns)
        self.assertIsNone(s3_file.decompression_time_ns)
        self.assertEqual(s3_file.sha1, hashlib.sha1(b"content").digest())
    
    def assert_correct_uploaded_s3file_already_compressed(self, s3_file: S3File):
        # participant_id, study_id, size_compressed, encryption_time_ns, upload_time_ns, last_updated
        self.assertIsNotNone(s3_file.path)
        self.assertIsNotNone(s3_file.participant_id)
        self.assertIsNotNone(s3_file.study_id)
        self.assertIsNotNone(s3_file.size_compressed)
        self.assertIsNotNone(s3_file.encryption_time_ns)
        self.assertIsNotNone(s3_file.upload_time_ns)
        self.assertIsNotNone(s3_file.last_updated)
        self.assertEqual(s3_file.size_compressed, len(self.COMPRESSED_SLUG))
        self.assertEqual(s3_file.study, self.default_study)
        self.assertIsNone(s3_file.compression_time_ns)
        self.assertIsNone(s3_file.decompression_time_ns)
        self.assertIsNone(s3_file.decrypt_time_ns)
        self.assertIsNone(s3_file.download_time_ns)
        self.assertIsNone(s3_file.sha1)
        self.assertIsNone(s3_file.size_uncompressed)


class TestCeleryAtLeastImports(CommonTestCase):
    
    def test_data_processing(self):
        from services import celery_data_processing
        for attr in celery_data_processing.__dict__.values():
            if isinstance(attr, DebugCeleryApp):
                return
        raise AssertionError("Celery app not found in celery_data_processing, the correct one must be present in the namespace.")
    
    def test_push_notifications(self):
        from services import celery_push_notifications
        for attr in celery_push_notifications.__dict__.values():
            if isinstance(attr, DebugCeleryApp):
                return
        raise AssertionError("Celery app not found in celery_push_notifications, the correct one must be present in the namespace.")
    
    def test_forest(self):
        from services import celery_forest
        for attr in celery_forest.__dict__.values():
            if isinstance(attr, DebugCeleryApp):
                return
        raise AssertionError("Celery app not found in celery_forest, the correct one must be present in the namespace.")
    
    def test_script_runner(self):
        from services import scripts_runner
        for attr in scripts_runner.__dict__.values():
            if isinstance(attr, DebugCeleryApp):
                return
        raise AssertionError("Celery app not found in scripts_runner, the correct one must be present in the namespace.")


class TestDetermineFileName(CommonTestCase):
    
    # time_as_unix_int = "1524857988384"
    THE_TIME = datetime.fromtimestamp(1524857988.384).astimezone(UTC)
    # T_STR_VALID= "2018-04-27 19_39_48.384000+00_00"
    # 123456789012345678901234  # needs to be 24 chars long
    # reference real name for audio file on s3:
    # 5873fe38644ad7557b168e43/q41aozrx/voiceRecording/587442edf7321c14da193487/1524857988384.wav
    @property
    def empty_dict_with_the_keys(self) -> dict[str, str|None]:
        self.most_recent_now = timezone.now().strftime(API_TIME_FORMAT)
        # these are all the valid keys
        return {
            "chunk_path": None,
            "data_type": None,
            "participant__patient_id": "steve",
            "study_id": "STUDY",
            "survey__object_id": None,
            "time_bin": self.THE_TIME,  # type: ignore
        }
    
    def updated_dict(self, **kwargs):
        ret = self.empty_dict_with_the_keys
        for key in kwargs:
            if key not in ret:
                raise ValueError(f"Invalid key: {key}")
        ret.update(kwargs)
        return ret
    
    # audio recordings
    def test_survey_id_present_in_audio_survey_with_param(self):
        d = self.updated_dict(
            chunk_path="5873fe38644ad7557b168e43/steve/voiceRecording/123456789012345678901234/1524857988384.wav",
            survey__object_id="thesurveyobjectidvalue",
            data_type=VOICE_RECORDING,
        )
        self.assertEqual(
            determine_base_file_name(d),
            "steve/audio_recordings/thesurveyobjectidvalue/2018-04-27 19_39_48.384000+00_00.wav"
        )
    
    def test_survey_id_present_in_audio_survey_with_param_as_mp4(self):
        d = self.updated_dict(
            chunk_path="5873fe38644ad7557b168e43/steve/voiceRecording/123456789012345678901234/1524857988384.mp4",
            survey__object_id="thesurveyobjectidvalue",
            data_type=VOICE_RECORDING,
        )
        self.assertEqual(
            determine_base_file_name(d),
            "steve/audio_recordings/thesurveyobjectidvalue/2018-04-27 19_39_48.384000+00_00.mp4"
        )
    
    def test_survey_id_present_in_audio_survey_without_param(self):
        d = self.updated_dict(
            chunk_path="5873fe38644ad7557b168e43/steve/voiceRecording/123456789012345678901234/1524857988384.wav",
            data_type=VOICE_RECORDING,
        )
        self.assertEqual(
            determine_base_file_name(d),
            "steve/audio_recordings/123456789012345678901234/2018-04-27 19_39_48.384000+00_00.wav"
        )
    
    def test_survey_id_present_in_without_param_or_file_path(self):
        d = self.updated_dict(
            chunk_path="5873fe38644ad7557b168e43/steve/voiceRecording/1524857988384.wav",
            data_type=VOICE_RECORDING,
        )
        self.assertEqual(
            determine_base_file_name(d),
            "steve/audio_recordings/unknown_survey_id/2018-04-27 19_39_48.384000+00_00.wav"
        )
    
    # survey answers
    def test_survey_id_present_in_survey_answers_with_param(self):
        d = self.updated_dict(
            chunk_path="5873fe38644ad7557b168e43/steve/surveyAnswers/123456789012345678901234/1524857988384.csv",
            survey__object_id="thesurveyobjectidvalue",
            data_type=SURVEY_ANSWERS,
        )
        self.assertEqual(
            determine_base_file_name(d),
            "steve/survey_answers/thesurveyobjectidvalue/2018-04-27 19_39_48.384000+00_00.csv"
        )
    
    def test_survey_id_present_in_survey_answers_without_param(self):
        d = self.updated_dict(
            chunk_path="5873fe38644ad7557b168e43/steve/surveyAnswers/123456789012345678901234/1524857988384.csv",
            data_type=SURVEY_ANSWERS,
        )
        self.assertEqual(
            determine_base_file_name(d),
            "steve/survey_answers/123456789012345678901234/2018-04-27 19_39_48.384000+00_00.csv"
        )
    
    def test_survey_id_present_in_survey_answers_without_param_or_file_path(self):
        d = self.updated_dict(
            chunk_path="5873fe38644ad7557b168e43/steve/surveyAnswers/1524857988384.csv",
            data_type=SURVEY_ANSWERS,
        )
        self.assertEqual(
            determine_base_file_name(d),
            "steve/survey_answers/unknown_survey_id/2018-04-27 19_39_48.384000+00_00.csv"
        )
    
    # survey timings
    
    def test_survey_id_present_in_survey_timings_with_param(self):
        d = self.updated_dict(
            chunk_path="5873fe38644ad7557b168e43/steve/surveyTimings/123456789012345678901234/1524857988384.csv",
            survey__object_id="thesurveyobjectidvalue",
            data_type=SURVEY_TIMINGS,
        )
        self.assertEqual(
            determine_base_file_name(d),
            "steve/survey_timings/thesurveyobjectidvalue/2018-04-27 19_39_48.384000+00_00.csv"
        )
    
    def test_survey_id_present_in_survey_timings_without_param(self):
        d = self.updated_dict(
            chunk_path="5873fe38644ad7557b168e43/steve/surveyTimings/123456789012345678901234/1524857988384.csv",
            data_type=SURVEY_TIMINGS,
        )
        self.assertEqual(
            determine_base_file_name(d),
            "steve/survey_timings/123456789012345678901234/2018-04-27 19_39_48.384000+00_00.csv"
        )
    
    def test_survey_id_present_in_survey_timings_without_param_or_file_path(self):
        d = self.updated_dict(
            chunk_path="5873fe38644ad7557b168e43/steve/surveyTimings/1524857988384.csv",
            data_type=SURVEY_TIMINGS,
        )
        self.assertEqual(
            determine_base_file_name(d),
            "steve/survey_timings/unknown_survey_id/2018-04-27 19_39_48.384000+00_00.csv"
        )


class TestFileProcessingUnittests(CommonTestCase):
    
    def test_convert_unix_to_human_readable_timestamps(self):
        rows = [
            [b"1", b"content"],
            [b"2", b"more content"],
        ]
        header = convert_unix_to_human_readable_timestamps(b"something,anything", rows)
        self.assertEqual(header, b"something,UTC time,anything")
        self.assertEqual(rows, [
            [b"1", b"1970-01-01T00:00:00.001", b"content"],
            [b"2", b"1970-01-01T00:00:00.002", b"more content"],
        ])
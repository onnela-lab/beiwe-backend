# trunk-ignore-all(bandit/B101,bandit/B106,ruff/B018,ruff/E701)
import time
import unittest
import uuid
from datetime import datetime, timedelta
from typing import Optional
from unittest.mock import MagicMock, patch

import dateutil
from dateutil.tz import gettz
from django.utils import timezone

from constants.message_strings import (ACCOUNT_NOT_FOUND, CONNECTION_ABORTED,
    ERR_ANDROID_REFERENCE_VERSION_CODE_DIGITS, ERR_ANDROID_TARGET_VERSION_DIGITS,
    ERR_IOS_REFERENCE_VERSION_NAME_FORMAT, ERR_IOS_TARGET_VERSION_FORMAT,
    ERR_IOS_VERSION_COMPONENTS_DIGITS, ERR_TARGET_VERSION_CANNOT_BE_MISSING,
    ERR_TARGET_VERSION_MUST_BE_STRING, ERR_UNKNOWN_OS_TYPE, FAILED_TO_ESTABLISH_CONNECTION,
    UNEXPECTED_SERVICE_RESPONSE, UNKNOWN_REMOTE_ERROR)
from constants.user_constants import ACTIVE_PARTICIPANT_FIELDS, ANDROID_API, IOS_API
from database.data_access_models import IOSDecryptionKey
from database.profiling_models import EncryptionErrorMetadata, LineEncryptionError, UploadTracking
from database.schedule_models import ArchivedEvent
from database.user_models_participant import (AppHeartbeats, AppVersionHistory,
    DeviceStatusReportHistory, Participant, ParticipantActionLog, ParticipantDeletionEvent,
    PushNotificationDisabledEvent, SurveyNotificationReport)
from libs.endpoint_helpers.participant_table_helpers import determine_registered_status
from libs.file_processing.utility_functions_simple import BadTimecodeError, binify_from_timecode
from libs.participant_purge import (confirm_deleted, get_all_file_path_prefixes,
    run_next_queued_participant_data_deletion)
from libs.utils.forest_utils import get_forest_git_hash
from libs.utils.participant_app_version_comparison import (is_this_version_gt_participants,
    is_this_version_gte_participants, is_this_version_lt_participants,
    is_this_version_lte_participants)
from services.celery_push_notifications import failed_send_survey_handler
from tests.common import CommonTestCase


# timezones should be compared using the 'is' operator
THE_ONE_TRUE_TIMEZONE = gettz("America/New_York")
THE_OTHER_ACCEPTABLE_TIMEZONE = gettz("UTC")

COUNT_OF_PATHS_RETURNED_FROM_GET_ALL_FILE_PATH_PREFIXES = 4

# Decorator for class instance methods that injects these three mocks, used in data purge tests.
# @patch('libs.participant_purge.s3_list_files')
# @patch('libs.participant_purge.s3_delete_many_versioned')
# @patch('libs.participant_purge.s3_list_versions')
# These patches are for the database table deletions.  s3_list_files specifically would result in an
# assertion error stating that the base s3 file path is not empty, so we patch that in the rest of
# the tests, which are database purge tests.
def data_purge_mock_s3_calls(func):
    s3_delete_many_versioned = patch('libs.participant_purge.s3_delete_many_versioned')
    s3_list_files = patch('libs.participant_purge.s3_list_files')
    s3_list_versions = patch('libs.participant_purge.s3_list_versions')
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
    def test_confirm_LineEncryptionError(self):
        LineEncryptionError.objects.create(
            base64_decryption_key="abc123",
            participant=self.default_participant,
            type=LineEncryptionError.PADDING_ERROR
        )
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
        self.populate_default_fcm_token
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
        self.generate_a_real_weekly_schedule_event_with_schedule()
        self.assert_confirm_deletion_raises_then_reset_last_updated
        run_next_queued_participant_data_deletion()
        confirm_deleted(self.default_participant_deletion_event)
    
    @data_purge_mock_s3_calls
    def test_confirm_ArchivedEvent(self):
        # its easiest to use a scheduled event to create an archived event...
        sched_event = self.generate_a_real_weekly_schedule_event_with_schedule()[0]
        sched_event.archive(True, self.default_participant, status="deleted", created_on=timezone.now())
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
        self.assertIs(self.default_participant.timezone, THE_ONE_TRUE_TIMEZONE)
    
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
        # the unknown_timezone flag should be true at the start and the end.
        p = self.default_participant
        self.assertEqual(p.timezone_name, "America/New_York")
        self.assertIs(p.timezone, THE_ONE_TRUE_TIMEZONE)
        self.assertEqual(p.unknown_timezone, True)  # A
        self.default_study.update(timezone_name="UTC")
        p.try_set_timezone("a bad string")
        # behavior should be to grab the study's timezone name, which for tests was unexpectedly UTC...
        self.assertEqual(p.timezone_name, "UTC")
        self.assertIs(p.timezone, THE_OTHER_ACCEPTABLE_TIMEZONE)
        self.assertEqual(p.unknown_timezone, True)  # A
    
    def test_try_bad_string_resets_unknown_timezone(self):
        p = self.default_participant
        p.update_only(unknown_timezone=False)  # force value to false
        self.assertEqual(p.timezone_name, "America/New_York")
        self.assertIs(p.timezone, THE_ONE_TRUE_TIMEZONE)
        self.assertEqual(p.unknown_timezone, False)  # A
        self.default_study.update(timezone_name="UTC")
        p.try_set_timezone("a bad string")
        self.assertEqual(p.timezone_name, "UTC")
        self.assertIs(p.timezone, THE_OTHER_ACCEPTABLE_TIMEZONE)
        self.assertEqual(p.unknown_timezone, True)  # B
    
    def test_same_timezone_name_still_updates_unknown_timezone_flag(self):
        p = self.default_participant
        last_update = p.last_updated
        self.assertEqual(p.timezone_name, "America/New_York")
        self.assertIs(p.timezone, THE_ONE_TRUE_TIMEZONE)
        self.assertEqual(p.unknown_timezone, True)  # A
        p.try_set_timezone("America/New_York")
        self.assertEqual(p.timezone_name, "America/New_York")
        self.assertIs(p.timezone, THE_ONE_TRUE_TIMEZONE)
        self.assertEqual(p.unknown_timezone, False)  # B
        self.assertEqual(p.last_updated, last_update)
    
    def test_valid_input(self):
        # should change both the timezone and the unknown_timezone flag
        p = self.default_participant
        last_update = p.last_updated
        self.assertEqual(p.timezone_name, "America/New_York")
        self.assertIs(p.timezone, THE_ONE_TRUE_TIMEZONE)
        self.assertEqual(p.unknown_timezone, True)
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
        }
        self.assertDictEqual(annotes, correct_annotations)
    
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


# these errors are taken directly from live servers with mild details purged
PUSH_NOTIFICATION_OBSCURE_HTML_ERROR_CONTENT = """
Unexpected HTTP response with status: 502; body: <!DOCTYPE html>
<html lang=en>
  <meta charset=utf-8>
  <meta name=viewport content="initial-scale=1, minimum-scale=1, width=device-width">
  <title>Error 502 (Server Error)!!1</title>
  <style>
    *{margin:0;
    padding:0}html,code{font:15px/22px arial,sans-serif}html{background:#fff;
    color:#222;
    padding:15px}body{margin:7% auto 0;
    max-width:390px;
    min-height:180px;
    padding:30px 0 15px}* > body{background:url(//www.google.com/images/errors/robot.png) 100% 5px no-repeat;
    padding-right:205px}p{margin:11px 0 22px;
    overflow:hidden}ins{color:#777;
    text-decoration:none}a img{border:0}@media screen and (max-width:772px){body{background:none;
    margin-top:0;
    max-width:none;
    padding-right:0}}#logo{background:url(//www.google.com/images/branding/googlelogo/1x/googlelogo_color_150x54dp.png) no-repeat;
    margin-left:-5px}@media only screen and (min-resolution:192dpi){#logo{background:url(//www.google.com/images/branding/googlelogo/2x/googlelogo_color_150x54dp.png) no-repeat 0% 0%/100% 100%;
    -moz-border-image:url(//www.google.com/images/branding/googlelogo/2x/googlelogo_color_150x54dp.png) 0}}@media only screen and (-webkit-min-device-pixel-ratio:2){#logo{background:url(//www.google.com/images/branding/googlelogo/2x/googlelogo_color_150x54dp.png) no-repeat;
    -webkit-background-size:100% 100%}}#logo{display:inline-block;
    height:54px;
    width:150px}
  </style>
  <a href=//www.google.com/><span id=logo aria-label=Google></span></a>
  <p><b>502.</b> <ins>That’s an error.</ins>
  <p>The server encountered a temporary error and could not complete your request.<p>Please try again in 30 seconds.  <ins>That’s all we know.</ins>
""".strip()

PUSH_NOTIFICATION_ERROR_INVALID_LENGTH = 'Unknown error while making a remote service call: ("Connection broken: InvalidChunkLength(got length b\'\', 0 bytes read)", InvalidChunkLength(got length b\'\', 0 bytes read))'
PUSH_NOTIFICATION_ERROR_CONNECTION_POOL = "Failed to establish a connection: HTTPSConnectionPool(host='fcm.googleapis.com', port=443): Max retries exceeded with url: /v1/projects/beiwe-20592/messages:send (Caused by ProtocolError('Connection aborted.', RemoteDisconnected('Remote end closed connection without response')))"
PUSH_NOTIFICATION_ERROR_ABORTED = "('Connection aborted.', RemoteDisconnected('Remote end closed connection without response'))"
PUSH_NOTIFICATION_INVALID_GRANT = "('invalid_grant: Invalid grant: account not found', {'error': 'invalid_grant', 'error_description': 'Invalid grant: account not found'})"


class TestFailedSendHandler(CommonTestCase):
    
    def test_weird_html_502_error(self):
        failed_send_survey_handler(
            participant=self.default_participant,
            fcm_token="a",
            error_message=PUSH_NOTIFICATION_OBSCURE_HTML_ERROR_CONTENT,
            schedules=[self.generate_a_real_weekly_schedule_event_with_schedule(0,0,0)[0]],
            debug=False,
        )
        archive = ArchivedEvent.objects.get()
        self.assertEqual(archive.status, UNEXPECTED_SERVICE_RESPONSE)
    
    def test_error_invalid_length(self):
        failed_send_survey_handler(
            participant=self.default_participant,
            fcm_token="a",
            error_message=PUSH_NOTIFICATION_ERROR_INVALID_LENGTH,
            schedules=[self.generate_a_real_weekly_schedule_event_with_schedule(0,0,0)[0]],
            debug=False,
        )
        archive = ArchivedEvent.objects.get()
        self.assertEqual(archive.status, UNKNOWN_REMOTE_ERROR)
    
    def test_error_connection_pool(self):
        failed_send_survey_handler(
            participant=self.default_participant,
            fcm_token="a",
            error_message=PUSH_NOTIFICATION_ERROR_CONNECTION_POOL,
            schedules=[self.generate_a_real_weekly_schedule_event_with_schedule(0,0,0)[0]],
            debug=False,
        )
        archive = ArchivedEvent.objects.get()
        self.assertEqual(archive.status, FAILED_TO_ESTABLISH_CONNECTION)
    
    def test_error_aborted(self):
        failed_send_survey_handler(
            participant=self.default_participant,
            fcm_token="a",
            error_message=PUSH_NOTIFICATION_ERROR_ABORTED,
            schedules=[self.generate_a_real_weekly_schedule_event_with_schedule(0,0,0)[0]],
            debug=False,
        )
        archive = ArchivedEvent.objects.get()
        self.assertEqual(archive.status, CONNECTION_ABORTED)
    
    def test_invalid_grant(self):
        failed_send_survey_handler(
            participant=self.default_participant,
            fcm_token="a",
            error_message=PUSH_NOTIFICATION_INVALID_GRANT,
            schedules=[self.generate_a_real_weekly_schedule_event_with_schedule(0,0,0)[0]],
            debug=False,
        )
        archive = ArchivedEvent.objects.get()
        self.assertEqual(archive.status, ACCOUNT_NOT_FOUND)


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

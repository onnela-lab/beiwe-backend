import time
import uuid
from datetime import datetime, timedelta
from typing import Optional
from unittest.mock import MagicMock, patch

import dateutil
from dateutil.tz import gettz
from django.utils import timezone

from constants.common_constants import EASTERN, UTC
from constants.data_stream_constants import (ACCELEROMETER, ALL_DATA_STREAMS, ANDROID_LOG_FILE,
    AUDIO_RECORDING, BLUETOOTH, CALL_LOG, DEVICEMOTION, GPS, GYRO, IDENTIFIERS, IOS_LOG_FILE,
    MAGNETOMETER, POWER_STATE, PROXIMITY, REACHABILITY, SURVEY_ANSWERS, SURVEY_TIMINGS, TEXTS_LOG,
    WIFI)
from constants.user_constants import ACTIVE_PARTICIPANT_FIELDS
from database.models import (AppHeartbeats, AppVersionHistory, ArchivedEvent, DeviceSettings,
    DeviceStatusReportHistory, EncryptionErrorMetadata, Participant, ParticipantActionLog,
    ParticipantDeletionEvent, PushNotificationDisabledEvent, S3File, ScheduledEvent,
    SurveyNotificationReport, UploadTracking)
from libs.endpoint_helpers.participant_table_helpers import determine_registered_status
from libs.file_processing.utility_functions_simple import (BadTimecodeError, binify_from_timecode,
    clean_java_timecode, convert_unix_to_human_readable_timestamps, ensure_sorted_by_timestamp,
    normalize_s3_file_path, resolve_survey_id_from_file_name, s3_file_path_to_data_type)
from libs.participant_purge import (confirm_deleted, get_all_file_path_prefixes,
    run_next_queued_participant_data_deletion)
from tests.common import CommonTestCase


#
## Don't add tests here, the file got so big that IDEs had problems
#


COUNT_OF_PATHS_RETURNED_FROM_GET_ALL_FILE_PATH_PREFIXES = 4


# Decorator for class instance methods that injects these three mocks, used in data purge tests.
# @patch('libs.participant_purge.s3_list_files')
# @patch('libs.participant_purge.s3_delete_many_versioned')
# @patch('libs.participant_purge.s3_list_versions')
# These patches are for the database table deletions.  s3_list_files specifically would result in an
# assertion error stating that the base s3 file path is not empty, so we patch that in the rest of
# the tests, which are database purge tests.
def data_purge_mock_s3_calls(func):
    s3_delete_many_versioned: MagicMock = patch('libs.participant_purge.s3_delete_many_versioned')  # type: ignore
    s3_list_files: MagicMock = patch('libs.participant_purge.s3_list_files')  # type: ignore
    s3_list_versions: MagicMock = patch('libs.participant_purge.s3_list_versions')  # type: ignore
    s3_list_files.return_value = []
    s3_list_versions.return_value = []
    s3_delete_many_versioned.return_value = []
    
    def wrapper(self, *args, **kwargs):
        with s3_delete_many_versioned, s3_list_files, s3_list_versions:
            return func(self, *args, **kwargs)
    return wrapper


class TestBinifyFromTimecode(CommonTestCase):
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


# AI generated, reviewed, made decisions based off this to change behavior
class TestEnsureSortedByTimestamp(CommonTestCase):
    def test_ensure_sorted_by_timestamp_sorts_in_place(self):
        rows = [[b"3"], [b"1"], [b"2"]]
        original_id = id(rows)
        ensure_sorted_by_timestamp(rows)
        self.assertEqual(id(rows), original_id)
        self.assertEqual(rows, [[b"1"], [b"2"], [b"3"]])
    
    def test_ensure_sorted_by_timestamp_keeps_sorted_order(self):
        rows = [[b"1"], [b"2"], [b"3"]]
        ensure_sorted_by_timestamp(rows)
        self.assertEqual(rows, [[b"1"], [b"2"], [b"3"]])
    
    def test_ensure_sorted_by_timestamp_mixed_numeric_widths(self):
        """Test sorting with timestamps of varying digit lengths"""
        rows = [[b"1000000"], [b"999"], [b"1000"], [b"99"], [b"10000"]]
        ensure_sorted_by_timestamp(rows)
        self.assertEqual(rows, [[b"99"], [b"999"], [b"1000"], [b"10000"], [b"1000000"]])
    
    def test_ensure_sorted_by_timestamp_large_timestamps(self):
        """Test with realistic Unix millisecond timestamps"""
        rows = [[b"1770358450000"], [b"1770358145197"], [b"1770358250000"]]
        ensure_sorted_by_timestamp(rows)
        self.assertEqual(rows, [[b"1770358145197"], [b"1770358250000"], [b"1770358450000"]])
    
    def test_ensure_sorted_by_timestamp_single_row(self):
        """Test with single row"""
        rows = [[b"12345"]]
        ensure_sorted_by_timestamp(rows)
        self.assertEqual(rows, [[b"12345"]])
    
    def test_ensure_sorted_by_timestamp_empty_list(self):
        """Test with empty list"""
        rows = []
        ensure_sorted_by_timestamp(rows)
        self.assertEqual(rows, [])
    
    def test_ensure_sorted_by_timestamp_reverse_order(self):
        """Test reverse-ordered timestamps get sorted correctly"""
        rows = [[b"5"], [b"4"], [b"3"], [b"2"], [b"1"]]
        ensure_sorted_by_timestamp(rows)
        self.assertEqual(rows, [[b"1"], [b"2"], [b"3"], [b"4"], [b"5"]])
    
    # cases with data mutations:
    
    def test_ensure_sorted_by_timestamp_doesnt_mutate_leading_zeros(self):
        # this is condition we just do not care about
        rows = [[b"10"], [b"02"], [b"1"]]
        ensure_sorted_by_timestamp(rows)
        self.assertEqual(rows, [[b"1"], [b"02"], [b"10"]])
    
    def test_ensure_sorted_by_timestamp_all_invalid_rows_removed(self):
        rows = [[b"abc"], [b"xyz"], [b"invalid"]]
        with self.assertRaises(ValueError):
            ensure_sorted_by_timestamp(rows)
        self.assertEqual(rows, [[b"abc"], [b"xyz"], [b"invalid"]])
    
    def test_custom_string1(self):
        # This is a string that got passed in in production and caused an error. it should do
        # exactly that, data should be cleaned beforehand.
        x = b'1771452000058,2026-02-18T22:00:00.058,1,0.4707878530025482,-0.8183453679084778,9.6783695220947270'
        rows1 = [[x]]
        # with self.assertRaises(ValueError):
        ensure_sorted_by_timestamp(rows1)
        
        rows2 = [x]
        with self.assertRaises(ValueError):
            ensure_sorted_by_timestamp(rows2)  # type: ignore
    
    def test_ensure_sorted_by_timestamp_does_not_remove_invalid_rows(self):
        """Test that invalid non-numeric timestamp rows are removed"""
        rows = [[b"100"], [b"illegal"], [b"50"], [b"not_a_number"], [b"200"]]
        with self.assertRaises(ValueError):
            ensure_sorted_by_timestamp(rows)
        
        self.assertEqual(rows, [[b"100"], [b"illegal"], [b"50"], [b"not_a_number"], [b"200"]])


# AI generated, reviewed
class TestNormalizeS3FilePath(CommonTestCase):
    def test_normalize_s3_file_path_removes_duplicate_suffix(self):
        path = "study/participant/powerState/1234567890.csv-duplicate-abc123"
        result = normalize_s3_file_path(path)
        self.assertEqual(result, "study/participant/powerState/1234567890.csv")
    
    def test_normalize_s3_file_path_returns_unchanged_if_no_duplicate(self):
        path = "study/participant/powerState/1234567890.csv"
        result = normalize_s3_file_path(path)
        self.assertEqual(result, path)
    
    def test_normalize_s3_file_path_removes_long_duplicate_suffix(self):
        path = "study/participant/gps/9876543210.csv-duplicate-xyzabc123def456789"
        result = normalize_s3_file_path(path)
        self.assertEqual(result, "study/participant/gps/9876543210.csv")
    
    def test_normalize_s3_file_path_empty_string(self):
        result = normalize_s3_file_path("")
        self.assertEqual(result, "")
    
    def test_normalize_s3_file_path_multiple_duplicate_keywords(self):
        # Test that only first -duplicate occurrence is used as split point
        path = "study/participant/powerState/1234567890.csv-duplicate-abc-duplicate-def"
        result = normalize_s3_file_path(path)
        self.assertEqual(result, "study/participant/powerState/1234567890.csv")


# AI generated, reviewed, rewrote
class TestS3FilePathToDataType(CommonTestCase):
    
    # def test_meta_self_test_names(self):
    #     test_names = [name for name in vars(self.__class__) if name.startswith("test")]
    #     for stream in ALL_DATA_STREAMS:
    #         count = sum(1 for test_name in test_names if stream in test_name)
    #         assert count > 0, f"no test found for stream {stream}, " \
    #             f" add a test to TestS3FilePathToDataType with `{stream}` in the name that " \
    #             "checks the correct extraction of that stream from the file path."
    
    def test_the_one(self):
        # the ai chat log isn't actually uploaded
        ai_set = set(["ambient_audio"])  # noqa
        all_data_streams_set = set(ALL_DATA_STREAMS)
        acceptable_streams = all_data_streams_set - ai_set  # type: ignore
        
        test_cases = [
            (ACCELEROMETER, "study/participant/accel/1234567890.csv"),
            # (AI_CHAT_LOGS, "study/participant/ai_chat_logs/1234567890.csv"),
            (ANDROID_LOG_FILE, "study/participant/logFile/1234567890.csv"),
            (AUDIO_RECORDING, "study/participant/voiceRecording/1234567890.csv"),
            (BLUETOOTH, "study/participant/bluetoothLog/1234567890.csv"),
            (CALL_LOG, "study/participant/callLog/1234567890.csv"),
            (DEVICEMOTION, "study/participant/devicemotion/1234567890.csv"),
            (IDENTIFIERS, "study/participant/identifiers/1234567890.csv"),
            (GPS, "study/participant/gps/1234567890.csv"),
            (GYRO, "study/participant/gyro/1234567890.csv"),
            (MAGNETOMETER, "study/participant/magnetometer/1234567890.csv"),
            (POWER_STATE, "study/participant/powerState/1234567890.csv"),
            (IOS_LOG_FILE, "study/participant/ios/log/1234567890.csv"),
            (IOS_LOG_FILE, "study/participant/ios_log/1234567890.csv"),
            (SURVEY_ANSWERS, "study/participant/surveyAnswers/1234567890.csv"),
            (SURVEY_TIMINGS, "study/participant/surveyTimings/1234567890.csv"),
            (PROXIMITY, "study/participant/proximity/1234567890.csv"),
            (REACHABILITY, "study/participant/reachability/1234567890.csv"),
            (TEXTS_LOG, "study/participant/textsLog/1234567890.csv"),
            (WIFI, "study/participant/wifiLog/1234567890.csv"),
        ]
        self.assertEqual(
            {stream for stream, _ in test_cases},
            acceptable_streams,
            "Test cases should cover all data streams defined in ALL_DATA_STREAMS except AI_CHAT_LOGS")
        
        for expected_stream, file_path in test_cases:
            self.assertEqual(s3_file_path_to_data_type(file_path), expected_stream)
    
    def test_s3_file_path_to_data_type_unknown_raises_exception(self):
        with self.assertRaises(Exception) as context:
            s3_file_path_to_data_type("study/participant/unknown_type/1234567890.csv")
        self.assertIn("data type unknown", str(context.exception))


class TestResolveSurveyIdFromFileName(CommonTestCase):
    
    def test_resolve_survey_id_from_file_name_basic(self):
        result = resolve_survey_id_from_file_name("study/participant/survey_abc123/1234567890.csv")
        self.assertEqual(result, "survey_abc123")
    
    def test_resolve_survey_id_from_file_name_with_duplicate_suffix(self):
        result = resolve_survey_id_from_file_name(
            "study/participant/survey_xyz789/1234567890.csv-duplicate-abc")
        self.assertEqual(result, "survey_xyz789")
    
    def test_resolve_survey_id_from_file_name_different_ids(self):
        result = resolve_survey_id_from_file_name("study/participant/my_survey/1234567890.csv")
        self.assertEqual(result, "my_survey")
    
    def test_resolve_survey_id_from_file_name_numeric_id(self):
        result = resolve_survey_id_from_file_name("study/participant/98765/1234567890.csv")
        self.assertEqual(result, "98765")


# scope issue, has to be outside class or it is an inaccessible attribute
DataStreamsEnabled_invalid = ANDROID_LOG_FILE, IDENTIFIERS, IOS_LOG_FILE, SURVEY_ANSWERS, SURVEY_TIMINGS


class TestDataStreamsEnabled(CommonTestCase):
    toggleable = [stream for stream in ALL_DATA_STREAMS if stream not in DataStreamsEnabled_invalid]
    toggleable.sort()
    
    def test1(self):
        for stream in self.toggleable:
            self.assertIn(stream, DeviceSettings.ENABLEMENT_FIELD_NAMES)
    
    def test2(self):
        for stream in DeviceSettings.ENABLEMENT_FIELD_NAMES:
            self.assertIn(stream, self.toggleable)
    
    def test3(self):
        for stream in DataStreamsEnabled_invalid:
            self.assertNotIn(stream, DeviceSettings.ENABLEMENT_FIELD_NAMES)
    
    def test_output_works_basics(self):
        ds = self.default_study.device_settings
        for stream in DeviceSettings.ENABLEMENT_FIELD_NAMES.values():
            setattr(ds, stream, True)
        ds.save()
        ds.refresh_from_db()
        
        for stream in DeviceSettings.ENABLEMENT_FIELD_NAMES.values():
            self.assertTrue(getattr(ds, stream))
        
        assert sorted(ds.enabled_data_streams()) == self.toggleable
        
        # go one by one to check if each toggle turns off the correct stream
        toggleable_mutable = [*self.toggleable]  # its sorted so this all works
        
        for stream in self.toggleable:
            setattr(ds, DeviceSettings.ENABLEMENT_FIELD_NAMES[stream], False)
            ds.save()
            ds.refresh_from_db()
            toggleable_mutable.remove(stream)
            self.assertEqual(sorted(ds.enabled_data_streams()), toggleable_mutable)


class TestConvertUnixToHumanReadableTimestamps(CommonTestCase):
    def test_convert_unix_to_human_readable_timestamps_milliseconds(self):
        """Test conversion with millisecond timestamps"""
        rows = [[b"1768928568332", b"data1"], [b"1768928568500", b"data2"]]
        header = convert_unix_to_human_readable_timestamps(b"timestamp,value", rows)
        
        self.assertEqual(header, b"timestamp,UTC time,value")
        # Verify timestamps are inserted at index 1
        self.assertEqual(rows[0][1], b"2026-01-20T17:02:48.332")
        self.assertEqual(rows[1][1], b"2026-01-20T17:02:48.500")
        # Verify data shifted to index 2
        self.assertEqual(rows[0][2], b"data1")
        self.assertEqual(rows[1][2], b"data2")
    
    def test_convert_unix_to_human_readable_timestamps_empty_rows(self):
        """Test with empty rows list"""
        rows = []
        header = convert_unix_to_human_readable_timestamps(b"timestamp,value", rows)
        self.assertEqual(header, b"timestamp,UTC time,value")
        self.assertEqual(rows, [])
    
    def test_convert_unix_to_human_readable_timestamps_multiple_columns(self):
        """Test with multiple data columns"""
        rows = [[b"1000", b"col1", b"col2", b"col3"]]
        header = convert_unix_to_human_readable_timestamps(b"ts,a,b,c", rows)
        
        self.assertEqual(header, b"ts,UTC time,a,b,c")
        self.assertEqual(len(rows[0]), 5)
        self.assertEqual(rows[0][0], b"1000")
        self.assertEqual(rows[0][2], b"col1")
        self.assertEqual(rows[0][3], b"col2")
        self.assertEqual(rows[0][4], b"col3")
    
    def test_convert_unix_to_human_readable_timestamps_zero_timestamp(self):
        """Test with zero timestamp (epoch)"""
        rows = [[b"0", b"data"]]
        header = convert_unix_to_human_readable_timestamps(b"timestamp,value", rows)
        
        # Verify timestamp index 1 has the epoch formatted time
        self.assertIn(b"1970", rows[0][1])
        self.assertEqual(rows[0][1], b"1970-01-01T00:00:00.000")
    
    def test_convert_unix_to_human_readable_timestamps_large_timestamp(self):
        """Test with very large timestamp"""
        rows = [[b"9999999999999", b"data"]]
        header = convert_unix_to_human_readable_timestamps(b"timestamp,value", rows)
        
        # Should handle large timestamps
        self.assertEqual(len(rows[0]), 3)
        self.assertIsNotNone(rows[0][1])
    
    def test_convert_unix_to_human_readable_timestamps_single_column_header(self):
        """Test with single column in header"""
        rows = [[b"1000", b"data"]]
        header = convert_unix_to_human_readable_timestamps(b"timestamp", rows)
        
        self.assertEqual(header, b"timestamp,UTC time")
        self.assertEqual(len(rows[0]), 3)


class TestCleanJavaTimecode(CommonTestCase):
    def test_clean_java_timecode_unix_seconds(self):
        """Test with Unix timestamp in seconds (10 digits)"""
        result = clean_java_timecode(b"1673316787")
        self.assertEqual(result, 1673316787)
    
    def test_clean_java_timecode_unix_milliseconds(self):
        """Test with Unix timestamp in milliseconds (13 digits) - extracts first 10"""
        result = clean_java_timecode(b"1673316787111")
        self.assertEqual(result, 1673316787)
    
    def test_clean_java_timecode_exact_10_digits(self):
        """Test with exactly 10 digit timestamp"""
        result = clean_java_timecode(b"1700000000")
        self.assertEqual(result, 1700000000)
    
    def test_clean_java_timecode_less_than_10_digits_raises_error(self):
        """Test with less than 10 digits - is considered too early"""
        with self.assertRaises(BadTimecodeError) as context:
            clean_java_timecode(b"123456789")
        self.assertIn("data too early", str(context.exception))
    
    def test_clean_java_timecode_non_numeric_raises_error(self):
        """Test that non-numeric timestamp raises BadTimecodeError"""
        with self.assertRaises(BadTimecodeError):
            clean_java_timecode(b"not_a_number")
    
    def test_clean_java_timecode_too_early_raises_error(self):
        """Test that timestamp too early (before 2014-07-31) raises error"""
        # 1406851199 is 1 second before EARLIEST_POSSIBLE_DATA_TIMESTAMP
        with self.assertRaises(BadTimecodeError) as context:
            clean_java_timecode(b"1406851199")
        self.assertIn("data too early", str(context.exception))
    
    def test_clean_java_timecode_too_late_raises_error(self):
        """Test that timestamp too late raises error"""
        # 9999999999 is far in the future beyond the 90-day limit
        with self.assertRaises(BadTimecodeError) as context:
            clean_java_timecode(b"9999999999")
        self.assertIn("data too late", str(context.exception))
    
    def test_clean_java_timecode_bytes_input(self):
        """Test that bytes input works correctly"""
        result = clean_java_timecode(b"1673316787123")
        self.assertEqual(result, 1673316787)
    
    def test_clean_java_timecode_exactly_at_earliest_boundary(self):
        """Test with timestamp exactly at earliest possible (2014-07-31 00:00:00 UTC)"""
        result = clean_java_timecode(b"1406851200")
        self.assertEqual(result, 1406851200)


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

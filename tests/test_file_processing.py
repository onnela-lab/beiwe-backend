import hashlib
from collections import defaultdict
from datetime import datetime
from io import BytesIO
from unittest.mock import Mock, patch

from cronutils import ErrorHandler, null_error_handler
from pyzstd import decompress

from config.settings import FILE_PROCESS_PAGE_SIZE
from constants.common_constants import CHUNKS_FOLDER, UTC
from constants.data_processing_constants import (AllBinifiedData, BinifyKey,
    CHUNK_TIMESLICE_QUANTUM, REFERENCE_CHUNKREGISTRY_HEADERS)
from constants.data_stream_constants import (ACCELEROMETER, ALL_DATA_STREAMS,
    ANDROID_LOG_FILE, AUDIO_RECORDING, BLUETOOTH, CALL_LOG, DEVICEMOTION, GPS, GYRO, IDENTIFIERS,
    IOS_LOG_FILE, MAGNETOMETER, POWER_STATE, PROXIMITY, REACHABILITY, SURVEY_ANSWERS,
    SURVEY_TIMINGS, TEXTS_LOG, WIFI)
from constants.user_constants import ANDROID_API, IOS_API
from database.models import ChunkRegistry, FileToProcess, S3File, Survey
from libs.aes import decrypt_server
from libs.file_processing.csv_merger import construct_s3_chunk_path, CsvMerger
from libs.file_processing.file_for_processing import FileForProcessing
from libs.file_processing.file_processing_core import easy_run, FileProcessingTracker
from libs.file_processing.utility_functions_csvs import construct_csv_as_bytes
from libs.file_processing.utility_functions_simple import (binify_from_timecode,
    convert_unix_to_human_readable_timestamps)
from tests.common import CommonTestCase
from tests.helpers import DatabaseHelperMixin


class FakeException(Exception): pass


input_power_state_content = b"""
timestamp,event,level
1768928568332,Locked,0.7
1768928682951,Unlocked,0.7
1768928682959,Unlocked,0.7
1768928688711,Locked,0.7
1768928691432,Unlocked,0.7
1768928695913,Locked,0.7
1768928851120,Unlocked,0.7
1768928851123,Unlocked,0.7
1768928853355,Locked,0.7
1768929245717,Unlocked,0.7
1768929245724,Unlocked,0.7
1768929248038,Locked,0.7
1768929342940,Unlocked,0.7
1768929345412,Locked,0.7
1768929356668,Unlocked,0.7
1768929356672,Unlocked,0.7
1768929363492,Locked,0.7
1768929466339,Unlocked,0.7
1768929504207,Locked,0.7
1768929613228,Unlocked,0.7
1768929613231,Unlocked,0.7
1768929627703,Locked,0.7
1768929645999,Unlocked,0.7
1768929646000,Unlocked,0.7
1768929652277,Locked,0.7
1768929724115,Unlocked,0.7
1768929724116,Unlocked,0.7
1768929730573,Locked,0.7
1768929753161,Unlocked,0.7
1768929753162,Unlocked,0.7
1768929763499,Locked,0.7
1768929780590,Unlocked,0.7
1768929836950,Locked,0.7
1768930324639,Unlocked,0.7
1768930324647,Unlocked,0.7
1768930331338,Locked,0.7
1768930484003,Unlocked,0.7
1768930484004,Unlocked,0.7
1768930506683,Locked,0.7
1768930979030,Unlocked,0.7
1768930979032,Unlocked,0.7
1768930993382,Locked,0.7
1768931057865,Unlocked,0.7
1768931065455,Locked,0.7
1768931100088,Unplugged,0.65
1768931160663,Unlocked,0.65
1768931165250,Locked,0.65
1768931191962,Unlocked,0.65
1768931191963,Unlocked,0.65
1768931213690,Locked,0.65
1768931230609,Unlocked,0.65
1768931230610,Unlocked,0.65
1768931241521,Locked,0.65
1768931297188,Unlocked,0.65
1768931305925,Locked,0.65
1768931363842,Unlocked,0.65
1768931363843,Unlocked,0.65
1768931397712,Locked,0.65
1768931450527,Unlocked,0.65
1768931467984,Locked,0.65
1768931737263,Unlocked,0.65
1768931737264,Unlocked,0.65
1768931746802,Locked,0.65
1768931835078,Unlocked,0.65
1768931835079,Unlocked,0.65
1768932073255,Locked,0.65
1768933104051,Unlocked,0.65
1768933104067,Unlocked,0.65
1768933123242,Locked,0.65
1768933525845,Unlocked,0.65
1768933525850,Unlocked,0.65
1768933858447,Locked,0.65
1768933880016,Unlocked,0.65
1768933880030,Unlocked,0.65
1768933973173,Unplugged,0.6
1768934119252,Locked,0.6
1768934150893,Unlocked,0.6
1768934150894,Unlocked,0.6
1768934153066,Locked,0.6
1768934529962,Unlocked,0.6
1768934529967,Unlocked,0.6
1768934542431,Locked,0.6
1768934562076,Unlocked,0.6
1768934562079,Unlocked,0.6
1768934575304,Locked,0.6
1768934577930,Unlocked,0.6
1768934622795,Locked,0.6
1768934740560,Unlocked,0.6
1768934740561,Unlocked,0.6
1768934854354,Locked,0.6
1768934948383,Unlocked,0.6
1768934948386,Unlocked,0.6
1768935004367,Locked,0.6
1768935009572,Unlocked,0.6
1768935044094,Locked,0.6
1768935102531,Unlocked,0.6
1768935198264,Locked,0.6
1768935273065,Unlocked,0.6
1768935273068,Unlocked,0.6
1768935319180,Locked,0.6
1768935370394,Unlocked,0.6
1768935370396,Unlocked,0.6
1768935452397,Locked,0.6
1768935486834,Unlocked,0.6
1768935531057,Locked,0.6
1768935685405,Unlocked,0.6
1768935685414,Unlocked,0.6
1768935804346,Locked,0.6
1768935819147,Unplugged,0.55
1768935833427,Unlocked,0.55
1768936094683,Locked,0.55
1768936132759,Unlocked,0.55
1768936132761,Unlocked,0.55
1768936143085,Locked,0.55
1768936152386,Unlocked,0.55
1768936192069,Locked,0.55
1768936230442,Unlocked,0.55
1768936230445,Unlocked,0.55
1768936279860,Locked,0.55
1768936291350,Unlocked,0.55
1768936291351,Unlocked,0.55
1768936332642,Locked,0.55
1768936389017,Unlocked,0.55
1768936389021,Unlocked,0.55
1768936402839,Locked,0.55
1768936403869,Unlocked,0.55
1768936468863,Locked,0.55
1768936470354,Unlocked,0.55
1768936520521,Locked,0.55
1768936605847,Unlocked,0.55
1768936605856,Unlocked,0.55
1768936642460,Locked,0.55
1768936666643,Unlocked,0.55
1768936666644,Unlocked,0.55
1768936787612,Locked,0.55
1768936869820,Unlocked,0.55
1768936869823,Unlocked,0.55
1768936987016,Locked,0.55
1768937006266,Unlocked,0.55
1768937012581,Locked,0.55
1768937015365,Unlocked,0.55
1768937029479,Locked,0.55
1768937040984,Unlocked,0.55
1768937040987,Unlocked,0.55
1768937049711,Locked,0.55
1768937397574,Unlocked,0.55
1768937397577,Unlocked,0.55
1768937441131,Locked,0.55
1768937498943,Unlocked,0.55
1768937498955,Unlocked,0.55
1768937635155,Unplugged,0.5
1768938323755,Locked,0.5
1768938409108,Unlocked,0.5
1768938409127,Unlocked,0.5
1768938726047,Unplugged,0.45
""".strip()


def setup_conn_retrieve_mock(mock: Mock, data: bytes):
    encrypted = DatabaseHelperMixin.true_default_s3_form(data)
    mock.get_object.return_value = {"Body": BytesIO(encrypted)}
    return encrypted


class TestFileProcessing(CommonTestCase):
    raw_fp_good = "1234567890ABCDEFGHIJKMNO/patient1/powerState/1768928568332.csv"
    raw_fp_bad = "1234567890ABCDEFGHIJKMNO/patient1/power_state/1768928568332.csv"
    
    type_unknown_error = "data type unknown: 1234567890ABCDEFGHIJKMNO/patient1/power_state/1768928568332.csv"
    
    def test_convert_unix_to_human_readable_timestamps(self):
        rows = [[b"1", b"content"], [b"2", b"more content"]]
        header = convert_unix_to_human_readable_timestamps(b"something,anything", rows)
        self.assertEqual(header, b"something,UTC time,anything")
        self.assertEqual(rows, [
            [b"1", b"1970-01-01T00:00:00.001", b"content"],
            [b"2", b"1970-01-01T00:00:00.002", b"more content"],
        ])
    
    def test_binify_from_timecode(self):
        # Test with a known timestamp: 1768928568332 milliseconds
        # This should be divided by 1000 to get seconds, then by CHUNK_TIMESLICE_QUANTUM (3600)
        timestamp_ms = b"1768928568332"
        expected_bin = 1768928568 // CHUNK_TIMESLICE_QUANTUM  # = 491369
        
        result = binify_from_timecode(timestamp_ms)
        self.assertEqual(result, expected_bin)
        
        # Test another timestamp in the same hour
        timestamp_ms_2 = b"1768928700000"
        result_2 = binify_from_timecode(timestamp_ms_2)
        self.assertEqual(result_2, expected_bin)  # Should be in same bin
        
        # Test a timestamp in a different hour
        timestamp_ms_3 = b"1768932200000"  # About 1 hour later
        expected_bin_3 = 1768932200 // CHUNK_TIMESLICE_QUANTUM
        result_3 = binify_from_timecode(timestamp_ms_3)
        self.assertEqual(result_3, expected_bin_3)
        self.assertNotEqual(result_3, expected_bin)  # Should be in different bin
    
    @patch("libs.s3.conn")
    def test_binification_of_power_state_data(self, conn: Mock):
        """Test that binification correctly groups data points by time bins"""
        setup_conn_retrieve_mock(conn, input_power_state_content)
        ftp = self.generate_file_to_process(path=self.raw_fp_good, os_type=ANDROID_API)
        
        ffp = FileForProcessing(ftp, self.default_study)
        ffp.prepare_data()
        
        # Now binify the data manually to test the binification logic
        binified: dict[int, list] = defaultdict(list)
        assert ffp.file_lines is not None, "file_lines should be populated by now"
        
        # Group rows by time bin
        for row in ffp.file_lines:
            if row and row[0]:
                binified[binify_from_timecode(row[0])].append(row)
        
        # Verify we have multiple time bins (the data spans ~3 hours)
        self.assertGreater(len(binified), 0, "Should have at least one time bin")
        
        # The data should be in time bins around 491369-491370 (based on timestamps 1768928568332)
        # Let's verify the bins are reasonable
        for time_bin in binified.keys():
            self.assertGreater(time_bin, 400000, "Time bin should be a reasonable value")
            self.assertLess(time_bin, 600000, "Time bin should be a reasonable value")
        
        # Verify each bin has rows
        for time_bin, rows in binified.items():
            self.assertGreater(len(rows), 0, f"Time bin {time_bin} should have at least one row")
            
            # Verify all rows in a bin have timestamps in the same hour
            for row in rows:
                row_time_bin = binify_from_timecode(row[0])
                self.assertEqual(row_time_bin, time_bin,
                    f"Row with timestamp {row[0]} should be in bin {time_bin}, not {row_time_bin}")
        
        # Count total rows
        total_rows = sum(len(rows) for rows in binified.values())
        self.assertEqual(total_rows, len(ffp.file_lines),
            "All rows should be accounted for in binified data")
        
        # Verify the structure matches what CsvMerger expects
        study_id, patient_id = ftp.study.object_id, ftp.participant.patient_id
        assert isinstance(ffp.header, bytes)
        
        for time_bin, _rows in binified.items():
            # BinifyKey = tuple[StudyObjectjID, PatientID, DataSteaam, TimecodeInt, CSVHeader]
            binify_key: BinifyKey = (study_id, patient_id, POWER_STATE, time_bin, ffp.header)
            
            # Verify the key components are valid
            self.assertIsInstance(binify_key[0], str)  # study_object_id
            self.assertIsInstance(binify_key[1], str)  # patient_id
            self.assertIsInstance(binify_key[2], str)  # data_type
            self.assertIsInstance(binify_key[3], int)  # time_bin
            self.assertIsInstance(binify_key[4], bytes)  # header
    
    @patch("libs.s3.conn")
    def test_file_for_processing_instantiation_raises_data_type_unknowwn(self, conn: Mock):
        setup_conn_retrieve_mock(conn, input_power_state_content)
        
        self.using_default_participant()
        ftp = self.generate_file_to_process(path=self.raw_fp_bad, os_type=ANDROID_API)
        with self.assertRaisesMessage(Exception, self.type_unknown_error):
            FileForProcessing(ftp, self.default_study)
    
    @patch("libs.s3.conn")
    def test_basic_file_for_processing_instantiation(self, conn: Mock):
        setup_conn_retrieve_mock(conn, input_power_state_content)
        ftp = self.generate_file_to_process(path=self.raw_fp_good, os_type=ANDROID_API)
        
        ffp = FileForProcessing(ftp, self.default_study)
        self.assertEqual(ffp.file_contents, input_power_state_content)
        
        # Test basic attribute instantiation
        self.assertEqual(ffp.data_type, POWER_STATE)
        self.assertTrue(ffp.chunkable, "POWER_STATE should be in CHUNKABLE_FILES")
        self.assertEqual(ffp.file_contents, input_power_state_content)
        self.assertIsNone(ffp.file_lines, "file_lines should not be populated until prepare_data is called")
        self.assertIsNone(ffp.header, "header should not be populated until prepare_data is called")
        self.assertIsNone(ffp.exception, "exception should be None on successful instantiation")
        self.assertIsNone(ffp.traceback, "traceback should be None on successful instantiation")
        self.assertIs(ffp.file_to_process, ftp, "file_to_process should be the same object passed in")
    
    @patch("libs.s3.conn")
    def test_clear_file_content_sets_none(self, conn: Mock):
        setup_conn_retrieve_mock(conn, input_power_state_content)
        ftp = self.generate_file_to_process(path=self.raw_fp_good, os_type=ANDROID_API)
        ffp = FileForProcessing(ftp, self.default_study)
        self.assertIsNotNone(ffp.file_contents)  # Verify file_contents is populated
        ffp.clear_file_content()  # Clear it
        self.assertIsNone(ffp.file_contents)  # Verify it's now None
    
    @patch("libs.s3.conn")
    def test_clear_file_content_raises_on_already_cleared(self, conn: Mock):
        setup_conn_retrieve_mock(conn, input_power_state_content)
        ftp = self.generate_file_to_process(path=self.raw_fp_good, os_type=ANDROID_API)
        ffp = FileForProcessing(ftp, self.default_study)
        
        ffp.clear_file_content()
        with self.assertRaises(AssertionError):
            ffp.clear_file_content()  # Calling again should raise AssertionError
    
    @patch("libs.s3.conn")
    def test_raw_csv_to_line_list_multiline(self, conn: Mock):
        header = b'timestamp,event,level'
        setup_conn_retrieve_mock(conn, input_power_state_content)
        ftp = self.generate_file_to_process(path=self.raw_fp_good, os_type=ANDROID_API)
        ffp = FileForProcessing(ftp, self.default_study)
        
        ffp.raw_csv_to_line_list()        # Process the CSV
        self.assertIsNotNone(ffp.header)  # header was extracted
        self.assertEqual(ffp.header, header)
        self.assertIsNotNone(ffp.file_lines)  # file_lines were populated
        self.assertGreater(len(ffp.file_lines), 0)
        
        # each line was split into columns
        assert ffp.file_lines is not None  # linter type assertion
        first_line = ffp.file_lines[0]
        self.assertEqual(len(first_line), 3)  # "timestamp, event, level"
        self.assertEqual(first_line[0], b'1768928568332')
        self.assertEqual(first_line[1], b'Locked')
        self.assertEqual(first_line[2], b'0.7')
        self.assertEqual(len(ffp.file_lines), 155)
        self.assertIsNone(ffp.file_contents)  # file_contents was cleared
    
    @patch("libs.s3.conn")
    def test_raw_csv_to_line_list_single_line(self, conn: Mock):
        single_line_content = b'timestamp,event,level'
        setup_conn_retrieve_mock(conn, single_line_content)
        
        ftp = self.generate_file_to_process(path=self.raw_fp_good, os_type=ANDROID_API)
        ffp = FileForProcessing(ftp, self.default_study)
        ffp.raw_csv_to_line_list()
        self.assertEqual(ffp.header, single_line_content)  # header was set to the entire content
        self.assertEqual(ffp.file_lines, [])  # file_lines is an empty list
        self.assertIsNone(ffp.file_contents)  # file_contents was cleared
    
    @patch("libs.s3.conn")
    def test_raw_csv_to_line_list_requires_file_contents(self, conn: Mock):
        setup_conn_retrieve_mock(conn, input_power_state_content)
        
        ftp = self.generate_file_to_process(path=self.raw_fp_good, os_type=ANDROID_API)
        ffp = FileForProcessing(ftp, self.default_study)
        ffp.clear_file_content()
        
        with self.assertRaises(AssertionError):
            ffp.raw_csv_to_line_list()
    
    @patch("libs.s3.conn")
    def test_prepare_data_processes_plus_cleans_header(self, conn: Mock):
        setup_conn_retrieve_mock(conn, input_power_state_content)
        ftp = self.generate_file_to_process(path=self.raw_fp_good, os_type=ANDROID_API)
        ffp = FileForProcessing(ftp, self.default_study)
        ffp.prepare_data()
        assert ffp.file_lines is not None  # linter type assertions
        assert ffp.header is not None
        
        self.assertIsNotNone(ffp.file_lines)  # Verify file_lines were populated
        self.assertGreater(len(ffp.file_lines), 0)
        
        self.assertIsNotNone(ffp.header)  # header was populated - quotes removed, spaces stripped
        self.assertNotIn(b' ', ffp.header)  # header whitespace stripped from each column
        self.assertIsNone(ffp.file_contents)  # file_contents was cleared during processing
    
    @patch("libs.s3.conn")
    def test_clear_file_lines_clears_file_lines(self, conn: Mock):
        setup_conn_retrieve_mock(conn, input_power_state_content)
        ftp = self.generate_file_to_process(path=self.raw_fp_good, os_type=ANDROID_API)
        ffp = FileForProcessing(ftp, self.default_study)
        ffp.prepare_data()
        
        self.assertIsNotNone(ffp.file_lines)  # check populated
        self.assertIsNotNone(ffp.header)
        ffp.clear_file_lines()
        self.assertIsNone(ffp.file_lines)
        self.assertIsNone(ffp.header)
    
    @patch("libs.s3.conn")
    def test_clear_file_lines_raises_on_already_cleared(self, conn: Mock):
        setup_conn_retrieve_mock(conn, input_power_state_content)
        ftp = self.generate_file_to_process(path=self.raw_fp_good, os_type=ANDROID_API)
        ffp = FileForProcessing(ftp, self.default_study)
        
        ffp.prepare_data()
        ffp.clear_file_lines()
        with self.assertRaises(AssertionError):
            ffp.clear_file_lines()
    
    @patch("libs.file_processing.file_for_processing.s3_retrieve")
    def test_download_file_contents_success(self, s3_retrieve: Mock):
        s3_retrieve.return_value = input_power_state_content
        ftp = self.generate_file_to_process(path=self.raw_fp_good, os_type=ANDROID_API)
        ffp = FileForProcessing(ftp, self.default_study)
        
        # download_file_contents is called in __init__, so just verify the result
        self.assertEqual(ffp.file_contents, input_power_state_content)
        self.assertIsNone(ffp.exception)
        self.assertIsNone(ffp.traceback)
        s3_retrieve.assert_called_once_with(ftp.s3_file_path, self.default_study, raw_path=True)


# timestamp 1
T1_BYTESTR = b"1770358145197"
T1_UTC = b"2026-02-06T06:09:05.197"
# Bin 1 (not related to the T1)
BIN_1 = 491000
BIN_2 = 491001
BIN_1_DT = datetime.fromtimestamp(BIN_1 * CHUNK_TIMESLICE_QUANTUM, UTC)

# Sample CSV data for testing - use realistic timestamps (milliseconds)
# Current timestamp is ~1770358145197, let's use something in that range
POWER_STATE_HEADER_ANDROID = REFERENCE_CHUNKREGISTRY_HEADERS[POWER_STATE][ANDROID_API]
POWER_STATE_HEADER_IOS = REFERENCE_CHUNKREGISTRY_HEADERS[POWER_STATE][IOS_API]

# = b'timestamp,UTC time,event,level'
POWER_STATE_ROWS_1 = [
    [T1_BYTESTR, b'Locked'],
    [b'1770358250000', b'Unlocked'],
]
POWER_STATE_ROWS_2 = [
    [b'1770358350000', b'Locked'],
    [b'1770358450000', b'Unlocked'],
]
POWERSTATE_OUT_LINE_1 = b"1770358145197,2026-02-06T06:09:05.197,Locked"
POWERSTATE_OUT_LINE_2 = b"1770358250000,2026-02-06T06:10:50.000,Unlocked"


class TestCsvMerger(CommonTestCase):
    
    def setUp(self):
        self.using_default_survey()  # not
        return super().setUp()
    
    @property
    def binified_and_handler(self) -> tuple[AllBinifiedData, ErrorHandler]:
        # extremely common configuration step
        binified_data: AllBinifiedData = defaultdict(lambda: ([], []))
        # return a null error handler so that errors actually get raised when they run for testing.
        return binified_data, null_error_handler()  # type: ignore[return-value]
    
    @property
    def bin_start(self) -> tuple[str, str, str]:
        # just makes some code less verbose
        return self.default_study.object_id, self.default_participant.patient_id, POWER_STATE
    
    def create_chunk(self):
        chunk_path = construct_s3_chunk_path(*self.bin_start, BIN_1, None)
        time_bin_datetime = datetime.fromtimestamp(BIN_1 * CHUNK_TIMESLICE_QUANTUM, UTC)
        ChunkRegistry.objects.create(
            study_id=self.default_participant.study_id,
            participant_id=self.default_participant.id,
            data_type=POWER_STATE,
            chunk_path=chunk_path,
            chunk_hash="old_hash",
            time_bin=time_bin_datetime,
            file_size=1000,
            is_chunkable=True,
        )
        return chunk_path
    
    ## Tests!
    
    # trivial
    
    def test_construct_s3_chunk_path(self):
        time_bin = 491000
        time_bin_str = "2026-01-05T08:00:00"
        base = f"{CHUNKS_FOLDER}/study_id/patient_id"
        survey_id = "survey_id"
        
        all_data_streams = {
            ACCELEROMETER:    f"{base}/{ACCELEROMETER}/{time_bin_str}.csv",
            ANDROID_LOG_FILE: f"{base}/{ANDROID_LOG_FILE}/{time_bin_str}.csv",
            BLUETOOTH:        f"{base}/{BLUETOOTH}/{time_bin_str}.csv",
            CALL_LOG:         f"{base}/{CALL_LOG}/{time_bin_str}.csv",
            DEVICEMOTION:     f"{base}/{DEVICEMOTION}/{time_bin_str}.csv",
            GPS:              f"{base}/{GPS}/{time_bin_str}.csv",
            GYRO:             f"{base}/{GYRO}/{time_bin_str}.csv",
            IDENTIFIERS:      f"{base}/{IDENTIFIERS}/{time_bin_str}.csv",
            IOS_LOG_FILE:     f"{base}/{IOS_LOG_FILE}/{time_bin_str}.csv",
            MAGNETOMETER:     f"{base}/{MAGNETOMETER}/{time_bin_str}.csv",
            POWER_STATE:      f"{base}/{POWER_STATE}/{time_bin_str}.csv",
            PROXIMITY:        f"{base}/{PROXIMITY}/{time_bin_str}.csv",
            REACHABILITY:     f"{base}/{REACHABILITY}/{time_bin_str}.csv",
            TEXTS_LOG:        f"{base}/{TEXTS_LOG}/{time_bin_str}.csv",
            WIFI:             f"{base}/{WIFI}/{time_bin_str}.csv",
            # these don't have canonical paths because they are downloaded raw
            # AUDIO_RECORDING: SURVEY_ANSWERS, AI_CHAT_LOGS
            # these have survey ids
            SURVEY_TIMINGS:   f"{base}/{SURVEY_TIMINGS}/{survey_id}/{time_bin_str}.csv",
        }
        
        for stream in all_data_streams:
            if stream == SURVEY_TIMINGS:
                
                with self.assertRaises(ValueError):
                    construct_s3_chunk_path("study_id", "patient_id", stream, time_bin, None)
                
                path = construct_s3_chunk_path("study_id", "patient_id", stream, time_bin, survey_id)
                self.assertEqual(path, all_data_streams[stream])
            
            else:
                
                with self.assertRaises(ValueError):
                    construct_s3_chunk_path("study_id", "patient_id", stream, time_bin, survey_id)
                
                path = construct_s3_chunk_path("study_id", "patient_id", stream, time_bin, None)
                self.assertEqual(path, all_data_streams[stream])
        
        actual_data_streams = set(ALL_DATA_STREAMS)
        actual_data_streams.remove(SURVEY_ANSWERS)
        actual_data_streams.remove(AUDIO_RECORDING)
        actual_data_streams.remove("ambient_audio")
        # actual_data_streams.remove(AI_CHAT_LOGS)
        self.assertEqual(set(all_data_streams), actual_data_streams)
    
    def test_csv_merger_initialization_with_empty_data(self):
        merger = CsvMerger(*self.binified_and_handler, self.default_participant, None, None)
        self.assertEqual(merger.failed_ftps, set())  # Verify empty state
        self.assertEqual(merger.ftps_to_retire, set())
        self.assertEqual(merger.upload_these, [])
        self.assertIsNone(merger.earliest_time_bin)
        self.assertIsNone(merger.latest_time_bin)
        self.assertIsNone(merger.survey_object_id)
        
        merger = CsvMerger(
            *self.binified_and_handler, self.default_participant, self.default_survey.object_id,
            self.default_survey.pk,
        )
        self.assertEqual(merger.failed_ftps, set())  # Verify empty state
        self.assertEqual(merger.ftps_to_retire, set())
        self.assertEqual(merger.upload_these, [])
        self.assertIsNone(merger.earliest_time_bin)
        self.assertIsNone(merger.latest_time_bin)
        self.assertEqual(merger.survey_object_id, self.default_survey.object_id)
    
    def test_csv_merger_get_retirees(self):
        data_bin: BinifyKey = (*self.bin_start, BIN_1, POWER_STATE_HEADER_ANDROID)
        binified_data, null_handler = self.binified_and_handler
        binified_data[data_bin] = (POWER_STATE_ROWS_1, [1, 2])
        
        merger = CsvMerger(binified_data, null_handler, self.default_participant, None, None)
        succeeded, failed, earliest, latest = merger.get_retirees()  # Get retirees
        self.assertEqual(len(merger.upload_these), 1)  # sanity check
        self.assertEqual(succeeded, {1, 2})  # All FTPs should be in succeeded set since no errors occurred
        self.assertEqual(failed, set())
        self.assertEqual(earliest, BIN_1)
        self.assertEqual(latest, BIN_1)
    
    @patch("libs.file_processing.csv_merger.s3_retrieve")
    def test_csv_merger_get_retirees_with_network_failures(self, s3_retrieve: Mock):
        binified_data: AllBinifiedData = defaultdict(lambda: ([], []))
        
        # First data bin will succeed, second will fail due to mocked S3 retrieval failure
        data_bin_1: BinifyKey = (*self.bin_start, BIN_1, POWER_STATE_HEADER_ANDROID)
        data_bin_2: BinifyKey = (*self.bin_start, BIN_2, POWER_STATE_HEADER_ANDROID)
        
        # Create a chunk registry for the second bin so S3 retrieval will be attempted
        chunk_path_2 = construct_s3_chunk_path(*self.bin_start, BIN_2, None)
        time_bin_2_datetime = datetime.fromtimestamp(BIN_2 * CHUNK_TIMESLICE_QUANTUM, UTC)
        ChunkRegistry.objects.create(
            study_id=self.default_participant.study_id,
            participant_id=self.default_participant.id,
            data_type=POWER_STATE,
            chunk_path=chunk_path_2,
            chunk_hash="old_hash",
            time_bin=time_bin_2_datetime,
            file_size=1000,
            is_chunkable=True,
        )
        
        # Mock S3 to fail only for the second chunk
        def s3_retrieve_side_effect(*args, **kwargs):
            raise FakeException("S3 retrieval failed for chunk 2")
        s3_retrieve.side_effect = s3_retrieve_side_effect
        
        binified_data[data_bin_1] = (POWER_STATE_ROWS_1, [1, 2])  # These should succeed
        binified_data[data_bin_2] = (POWER_STATE_ROWS_2, [3, 4])  # These should fail
        
        error_handler = ErrorHandler()  # have to use a real error handler on this one
        merger = CsvMerger(binified_data, error_handler, self.default_participant, None, None)
        
        tb = list(error_handler.errors)[0]  # <- the stack trace as a string. yes it is stupid.
        assert 'raise FakeException("S3 retrieval failed for chunk 2")' in tb
        
        # Get retirees
        succeeded, failed, earliest, latest = merger.get_retirees()
        
        # FTPs 1 and 2 should succeed, 3 and 4 should fail
        self.assertEqual(succeeded, {1, 2})
        self.assertEqual(failed, {3, 4})
        
        # Time bins should reflect both attempts
        self.assertEqual(earliest, BIN_1)
        self.assertEqual(latest, BIN_2)
    
    @patch("libs.file_processing.csv_merger.s3_retrieve")
    def test_csv_merger_error_handling_marks_failed_ftps(self, s3_retrieve: Mock):
        binified_data: AllBinifiedData = defaultdict(lambda: ([], []))
        
        # Create a scenario that will cause an error: missing chunk in S3
        chunk_path = construct_s3_chunk_path(*self.bin_start, BIN_1, None)
        ChunkRegistry.objects.create(  # Create chunk registry but S3 will fail to retrieve it
            study_id=self.default_participant.study_id,
            participant_id=self.default_participant.id,
            data_type=POWER_STATE,
            chunk_path=chunk_path,
            chunk_hash="old_hash",
            time_bin=BIN_1_DT,
            file_size=1000,
            is_chunkable=True,
        )
        
        # Mock S3 to raise an exception
        s3_retrieve.side_effect = Exception("S3 retrieval failed")
        
        # Set up binified data
        reference_header = REFERENCE_CHUNKREGISTRY_HEADERS[POWER_STATE][ANDROID_API]
        data_bin: BinifyKey = (*self.bin_start, BIN_1, reference_header)
        binified_data[data_bin] = (POWER_STATE_ROWS_1, [1, 2])
        
        # This should complete without raising, but mark FTPs as failed
        error_handler = ErrorHandler()  # this one needs a real error handler to record the error
        merger = CsvMerger(binified_data, error_handler, self.default_participant, None, None)
        
        # Verify FTPs were marked as failed
        succeeded, failed, _, _ = merger.get_retirees()
        self.assertEqual(failed, {1, 2})
        self.assertEqual(succeeded, set())
    
    # header validation logic
    
    def test_csv_merger_validate_one_header_matching(self):
        merger = CsvMerger(*self.binified_and_handler, self.default_participant, None, None)
        
        # Get the reference header, validate
        reference_header = REFERENCE_CHUNKREGISTRY_HEADERS[POWER_STATE][self.default_participant.os_type]
        result = merger.validate_one_header(reference_header, POWER_STATE)
        
        # Should return the reference header unchanged
        self.assertEqual(result, reference_header)
    
    def test_csv_merger_validate_one_header_mismatch(self):
        merger = CsvMerger(*self.binified_and_handler, self.default_participant, None, None)
        
        # Create a bad header, validate
        bad_header = b'timestamp,bad,header,columns'
        result = merger.validate_one_header(bad_header, POWER_STATE)
        
        # Should return the reference header, not the bad one
        reference_header = REFERENCE_CHUNKREGISTRY_HEADERS[POWER_STATE][self.default_participant.os_type]
        self.assertEqual(result, reference_header)
        self.assertNotEqual(result, bad_header)
    
    def test_csv_merger_validate_two_headers_identical(self):
        merger = CsvMerger(*self.binified_and_handler, self.default_participant, None, None)
        reference_header = REFERENCE_CHUNKREGISTRY_HEADERS[POWER_STATE][self.default_participant.os_type]
        
        # Both headers are the same
        result = merger.validate_two_headers(reference_header, reference_header, POWER_STATE)
        
        self.assertEqual(result, reference_header)  # Should return the reference header
    
    def test_csv_merger_validate_two_headers_both_match_reference(self):
        merger = CsvMerger(*self.binified_and_handler, self.default_participant, None, None)
        reference_header = REFERENCE_CHUNKREGISTRY_HEADERS[POWER_STATE][self.default_participant.os_type]
        
        # Both headers match reference
        result = merger.validate_two_headers(reference_header, reference_header, POWER_STATE)
        self.assertEqual(result, reference_header)  # Should return the reference header
    
    def test_csv_merger_validate_two_headers_one_matches_reference(self):
        merger = CsvMerger(*self.binified_and_handler, self.default_participant, None, None)
        reference_header = REFERENCE_CHUNKREGISTRY_HEADERS[POWER_STATE][self.default_participant.os_type]
        bad_header = b'timestamp,bad,header'
        
        # One header matches, one doesn't
        result = merger.validate_two_headers(reference_header, bad_header, POWER_STATE)
        self.assertEqual(result, reference_header)  # Should return the reference header
    
    def test_csv_merger_validate_two_headers_neither_matches(self):
        merger = CsvMerger(*self.binified_and_handler, self.default_participant, None, None)
        bad_header_1 = b'timestamp,bad,header1'
        bad_header_2 = b'timestamp,bad,header2'
        
        # Neither matches reference
        result = merger.validate_two_headers(bad_header_1, bad_header_2, POWER_STATE)
        
        reference_header = REFERENCE_CHUNKREGISTRY_HEADERS[POWER_STATE][self.default_participant.os_type]
        self.assertEqual(result, reference_header)  # Should still return the reference header
    
    # merging logic
    
    def test_csv_merger_chunk_not_exists_case(self):
        # Create test data
        binified_data, null_handler = self.binified_and_handler
        data_bin: BinifyKey = (*self.bin_start, BIN_1, POWER_STATE_HEADER_ANDROID)
        binified_data[data_bin] = (POWER_STATE_ROWS_1, [1, 2])  # file_to_process PKs
        merger = CsvMerger(binified_data, null_handler, self.default_participant, None, None)
        
        # Verify that the chunk was processed
        self.assertEqual(len(merger.upload_these), 1)
        self.assertIn(1, merger.ftps_to_retire)  # two successes
        self.assertIn(2, merger.ftps_to_retire)
        self.assertEqual(merger.earliest_time_bin, BIN_1)  # time bins for summarystatistics
        self.assertEqual(merger.latest_time_bin, BIN_1)  # time bins for summarystatistics
        
        # Verify upload data structure
        chunk_params, chunk_path, new_contents, sha1_hash, size_uncompressed, is_new = merger.upload_these[0]
        decompressed = decompress(new_contents)
        self.assertTrue(is_new)
        self.assertIn(POWER_STATE, chunk_path)
        self.assertIsNotNone(new_contents)
        self.assertEqual(sha1_hash, hashlib.sha1(decompressed).digest())
        self.assertEqual(size_uncompressed, len(decompressed))
        
        lines = decompressed.splitlines()
        self.assertEqual(lines[0], POWER_STATE_HEADER_ANDROID)
        self.assertEqual(lines[1], POWERSTATE_OUT_LINE_1)
        self.assertEqual(lines[2], POWERSTATE_OUT_LINE_2)
        self.assertIsNotNone(chunk_params)
        self.assertEqual(chunk_params['data_type'], POWER_STATE)
    
    def test_csv_merger_two_identical_lines_are_merged(self):
        duplicated_row = [T1_BYTESTR, T1_UTC, b'Locked']
        duplicate_rows = [duplicated_row, duplicated_row]  # Same row twice
        
        data_bin: BinifyKey = (*self.bin_start, BIN_1, POWER_STATE_HEADER_ANDROID)
        binified_data, null_handler = self.binified_and_handler
        binified_data[data_bin] = (duplicate_rows, [1, 2])
        merger = CsvMerger(binified_data, null_handler, self.default_participant, None, None)
        
        # Verify that the chunk was processed
        self.assertEqual(len(merger.upload_these), 1)
        self.assertIn(1, merger.ftps_to_retire)
        self.assertIn(2, merger.ftps_to_retire)
        
        # Verify upload data structure
        chunk_params, chunk_path, new_contents, sha1_hash, size_uncompressed, is_new = merger.upload_these[0]
        decompressed = decompress(new_contents)
        
        # Should have header + 2 identical data lines
        lines = decompressed.splitlines()
        self.assertEqual(len(lines), 2)  # header + 1 uniqueified data line
        
        self.assertEqual(lines[0], POWER_STATE_HEADER_ANDROID)
        self.assertEqual(lines[1], b','.join(duplicated_row))
    
    def test_csv_merger_three_identical_lines_are_merged(self):
        duplicated_row = [T1_BYTESTR, b'Locked']
        duplicate_rows = [duplicated_row, duplicated_row, duplicated_row]  # Same row three times
        
        data_bin: BinifyKey = (*self.bin_start, BIN_1, POWER_STATE_HEADER_ANDROID)
        binified_data, null_handler = self.binified_and_handler
        binified_data[data_bin] = (duplicate_rows, [1, 2, 3])
        merger = CsvMerger(binified_data, null_handler, self.default_participant, None, None)
        
        # Verify that the chunk was processed
        self.assertEqual(len(merger.upload_these), 1)
        self.assertIn(1, merger.ftps_to_retire)
        self.assertIn(2, merger.ftps_to_retire)
        self.assertIn(3, merger.ftps_to_retire)
        
        # Verify upload data structure
        chunk_params, chunk_path, new_contents, sha1_hash, size_uncompressed, is_new = merger.upload_these[0]
        decompressed = decompress(new_contents)
        
        # Should have header + 3 identical data lines
        lines = decompressed.splitlines()
        self.assertEqual(len(lines), 2)  # header + 1 uniqueified data lines
        self.assertEqual(lines[0], POWER_STATE_HEADER_ANDROID)
        self.assertEqual(lines[1], b','.join(duplicated_row))
    
    def test_csv_merger_two_lines_identical_timestamp_different_values(self):
        # Create test data with same timestamp but different events/values
        row_1 = [T1_BYTESTR, b'Locked']
        row_2 = [T1_BYTESTR, b'Unlocked']  # Different event and level
        rows = [row_1, row_2]
        row_out_1 = b','.join([T1_BYTESTR, T1_UTC, b'Locked'])
        row_out_2 = b','.join([T1_BYTESTR, T1_UTC, b'Unlocked'])
        
        data_bin: BinifyKey = (*self.bin_start, BIN_1, POWER_STATE_HEADER_ANDROID)
        binified_data, null_handler = self.binified_and_handler
        binified_data[data_bin] = (rows, [1, 2])
        
        merger = CsvMerger(binified_data, null_handler, self.default_participant, None, None)
        
        # Verify that the chunk was processed
        self.assertEqual(len(merger.upload_these), 1)
        self.assertIn(1, merger.ftps_to_retire)
        self.assertIn(2, merger.ftps_to_retire)
        
        # Verify upload data structure
        chunk_params, chunk_path, new_contents, sha1_hash, size_uncompressed, is_new = merger.upload_these[0]
        decompressed = decompress(new_contents)
        
        # Should have header + 2 data lines
        lines = decompressed.splitlines()
        self.assertEqual(len(lines), 3)  # header + 2 data lines
        
        self.assertEqual(lines[0], POWER_STATE_HEADER_ANDROID)
        self.assertEqual(lines[1], row_out_1)
        self.assertEqual(lines[2], row_out_2)
    
    def test_csv_merger_two_lines_exactly_one_time_bin_separate(self):
        binified_data, null_handler = self.binified_and_handler
        
        # Row in first time bin, second is in time bin (one hour later)
        row_1 = [T1_BYTESTR, b'Locked']
        row_2 = [b'1770361745197', b'Unlocked']
        
        data_bin_1: BinifyKey = (*self.bin_start, BIN_1, POWER_STATE_HEADER_ANDROID)
        data_bin_2: BinifyKey = (*self.bin_start, BIN_2, POWER_STATE_HEADER_ANDROID)
        
        binified_data[data_bin_1] = ([row_1], [1])
        binified_data[data_bin_2] = ([row_2], [2])
        merger = CsvMerger(binified_data, null_handler, self.default_participant, None, None)
        
        # Verify both chunks were processed as separate chunks
        self.assertEqual(len(merger.upload_these), 2)
        self.assertIn(1, merger.ftps_to_retire)
        self.assertIn(2, merger.ftps_to_retire)
        
        # Verify time bins are exactly one apart
        self.assertEqual(merger.earliest_time_bin, BIN_1)
        self.assertEqual(merger.latest_time_bin, BIN_2)
        self.assertEqual(BIN_2 - BIN_1, 1)
        
        # Each chunk should be a new chunk
        for chunk_params, chunk_path, new_contents, sha1_hash, size_uncompressed, is_new in merger.upload_these:
            self.assertTrue(is_new)
            decompressed = decompress(new_contents)
            self.assertEqual(size_uncompressed, len(decompressed))
            self.assertEqual(sha1_hash, hashlib.sha1(decompressed).digest())
    
    
    def test_csv_merger_timestamp_exactly_at_time_bin_boundary_are_in_the_bin_that_matches(self):
        binified_data, null_handler = self.binified_and_handler
        
        # Timestamp exactly at BIN_1 and bin2 boundaries: BIN_1
        timestamp_at_bin1_boundary = str(BIN_1 * CHUNK_TIMESLICE_QUANTUM * 1000).encode()
        timestamp_at_bin2_boundary = str(BIN_2 * CHUNK_TIMESLICE_QUANTUM * 1000).encode()
        row_bin1 = [timestamp_at_bin1_boundary, b'Locked']
        row_bin2 = [timestamp_at_bin2_boundary, b'Unlocked']
        
        data_bin_1: BinifyKey = (*self.bin_start, BIN_1, POWER_STATE_HEADER_ANDROID)
        data_bin_2: BinifyKey = (*self.bin_start, BIN_2, POWER_STATE_HEADER_ANDROID)
        binified_data[data_bin_1] = ([row_bin1], [1])
        binified_data[data_bin_2] = ([row_bin2], [2])
        
        merger = CsvMerger(binified_data, null_handler, self.default_participant, None, None)
        
        # Verify both chunks were processed as separate chunks
        self.assertEqual(len(merger.upload_these), 2)
        self.assertIn(1, merger.ftps_to_retire)
        self.assertIn(2, merger.ftps_to_retire)
        
        # Verify time bins are exactly one apart
        self.assertEqual(merger.earliest_time_bin, BIN_1)
        self.assertEqual(merger.latest_time_bin, BIN_2)
        
        # Verify each chunk is new
        for chunk_params, chunk_path, new_contents, sha1_hash, size_uncompressed, is_new in merger.upload_these:
            self.assertTrue(is_new)
            decompressed = decompress(new_contents)
            self.assertEqual(size_uncompressed, len(decompressed))
            self.assertEqual(sha1_hash, hashlib.sha1(decompressed).digest())
            
            # Verify the timestamps are in the decompressed output
            decompressed = decompress(new_contents)
            lines = decompressed.splitlines()
            self.assertEqual(lines[0], POWER_STATE_HEADER_ANDROID)
            self.assertGreaterEqual(len(lines), 2)  # Header + at least one data line
    
    def test_csv_merger_timestamps_out_of_chronological_order(self):
        binified_data, null_handler = self.binified_and_handler
        
        # Create rows with timestamps out of chronological order (all within same bin)
        # Most recent first, then older
        row_newest = [b"1770358450000", b"Unlocked"]  # Most recent
        row_middle = [b"1770358250000", b"Locked"]    # Middle
        row_oldest = [b"1770358145197", b"Locked"]    # Oldest
        rows = [row_newest, row_middle, row_oldest]  # Out of order (reverse chronological)
        OUTOFORDER_LINE_NEWEST_OUT = b"1770358450000,2026-02-06T06:14:10.000,Unlocked"
        OUTOFORDER_LINE_MIDDLE_OUT = b"1770358250000,2026-02-06T06:10:50.000,Locked"
        OUTOFORDER_LINE_OLDEST_OUT = b"1770358145197,2026-02-06T06:09:05.197,Locked"
        
        data_bin: BinifyKey = (*self.bin_start, BIN_1, POWER_STATE_HEADER_ANDROID)
        binified_data[data_bin] = (rows, [1, 2, 3])
        merger = CsvMerger(binified_data, null_handler, self.default_participant, None, None)
        
        # Verify chunk was processed
        self.assertEqual(len(merger.upload_these), 1)
        self.assertEqual(len(merger.ftps_to_retire), 3)
        for ftp_int in [1, 2, 3]:
            self.assertIn(ftp_int, merger.ftps_to_retire)
        
        # Verify upload data
        chunk_params, chunk_path, new_contents, sha1_hash, size_uncompressed, is_new = merger.upload_these[0]
        decompressed = decompress(new_contents)
        
        self.assertTrue(is_new)
        self.assertEqual(size_uncompressed, len(decompressed))
        self.assertEqual(sha1_hash, hashlib.sha1(decompressed).digest())
        
        lines = decompressed.splitlines()
        # Should have header + 3 data lines
        self.assertEqual(len(lines), 4)
        self.assertEqual(lines[0], POWER_STATE_HEADER_ANDROID)
        self.assertEqual(lines[1], OUTOFORDER_LINE_OLDEST_OUT)
        self.assertEqual(lines[2], OUTOFORDER_LINE_MIDDLE_OUT)
        self.assertEqual(lines[3], OUTOFORDER_LINE_NEWEST_OUT)
    
    def test_csv_merger_timestamps_out_of_chronological_order_within_different_hours(self):
        # Expected output lines for this test
        OUTOFORDER_BIN2_LINE = b"1770361745197,2026-02-06T07:09:05.197,Unlocked"
        OUTOFORDER_BIN1_LINE = b"1770358145197,2026-02-06T06:09:05.197,Locked"
        binified_data, null_handler = self.binified_and_handler
        
        # Create rows from BIN_2 first, then BIN_1 (reverse chronological across bins)
        row_bin2_newest = [b'1770361745197', b'Unlocked']  # From BIN_2, most recent
        row_bin1_oldest = [b'1770358145197', b'Locked']    # From BIN_1, oldest
        
        data_bin_1: BinifyKey = (*self.bin_start, BIN_1, POWER_STATE_HEADER_ANDROID)
        data_bin_2: BinifyKey = (*self.bin_start, BIN_2, POWER_STATE_HEADER_ANDROID)
        
        # Present in reverse order (BIN_2 before BIN_1)
        binified_data[data_bin_2] = ([row_bin2_newest], [2])
        binified_data[data_bin_1] = ([row_bin1_oldest], [1])
        
        merger = CsvMerger(binified_data, null_handler, self.default_participant, None, None)
        
        # Verify both chunks were processed
        self.assertEqual(len(merger.upload_these), 2)
        self.assertIn(1, merger.ftps_to_retire)
        self.assertIn(2, merger.ftps_to_retire)
        
        # Verify time bins are tracked correctly
        self.assertEqual(merger.earliest_time_bin, BIN_1)
        self.assertEqual(merger.latest_time_bin, BIN_2)
        chunk_params1, chunk_path1, new_contents1, sha1_hash1, size1, is_new1 = merger.upload_these[0]
        chunk_params2, chunk_path2, new_contents2, sha1_hash2, size2, is_new2 = merger.upload_these[1]
        
        # Verify BIN_1 chunk exists and is correct
        decompressed1 = decompress(new_contents1)
        self.assertEqual(hashlib.sha1(decompressed1).digest(), sha1_hash1)
        self.assertEqual(size1, len(decompressed1))
        bin1_lines = decompressed1.splitlines()
        self.assertEqual(len(bin1_lines), 2)  # Header + 1 data line
        self.assertEqual(bin1_lines[0], POWER_STATE_HEADER_ANDROID)
        self.assertEqual(bin1_lines[1], OUTOFORDER_BIN1_LINE)
        
        # Verify BIN_2 chunk exists and is correct
        decompressed2 = decompress(new_contents2)
        self.assertEqual(hashlib.sha1(decompressed2).digest(), sha1_hash2)
        self.assertEqual(size2, len(decompressed2))
        bin2_lines = decompressed2.splitlines()
        self.assertEqual(len(bin2_lines), 2)  # Header + 1 data line
        self.assertEqual(bin2_lines[0], POWER_STATE_HEADER_ANDROID)
        self.assertEqual(bin2_lines[1], OUTOFORDER_BIN2_LINE)
    
    def test_csv_merger_one_millisecond_before_next_bin(self):
        # Timestamp: just before BIN_2 boundary - should stay in BIN_1
        timestamp_before_boundary = str(BIN_1 * CHUNK_TIMESLICE_QUANTUM * 1000 + 3599999).encode()
        expected_output = b"1767603599999,2026-01-05T08:59:59.999,Locked"
        binified_data, null_handler = self.binified_and_handler
        row = [timestamp_before_boundary, b'Locked']
        
        data_bin: BinifyKey = (*self.bin_start, BIN_1, POWER_STATE_HEADER_ANDROID)
        binified_data[data_bin] = ([row], [1])
        
        merger = CsvMerger(binified_data, null_handler, self.default_participant, None, None)
        
        # Verify single chunk was processed
        self.assertEqual(len(merger.upload_these), 1)
        self.assertIn(1, merger.ftps_to_retire)
        self.assertEqual(merger.earliest_time_bin, BIN_1)
        self.assertEqual(merger.latest_time_bin, BIN_1)
        self._test_millisecond_common(merger, expected_output)
    
    def test_csv_merger_one_millisecond_after_bin_start(self):
        # Timestamp: just after BIN_1 boundary - should go into BIN_2
        timestamp_after_boundary = str(BIN_1 * CHUNK_TIMESLICE_QUANTUM * 1000 + 3600001).encode()
        expected_line = b"1767603600001,2026-01-05T09:00:00.001,Unlocked"
        binified_data, null_handler = self.binified_and_handler
        
        data_bin: BinifyKey = (*self.bin_start, BIN_2, POWER_STATE_HEADER_ANDROID)
        row = [timestamp_after_boundary, b'Unlocked']
        binified_data[data_bin] = ([row], [1])
        
        merger = CsvMerger(binified_data, null_handler, self.default_participant, None, None)
        
        # Verify single chunk was processed
        self.assertEqual(len(merger.upload_these), 1)
        self.assertIn(1, merger.ftps_to_retire)
        self.assertEqual(merger.earliest_time_bin, BIN_2)
        self.assertEqual(merger.latest_time_bin, BIN_2)
        self._test_millisecond_common(merger, expected_line)
    
    def _test_millisecond_common(self, merger: CsvMerger, expected_line: bytes):
        # Verify upload data
        chunk_params, chunk_path, new_contents, sha1_hash, size_uncompressed, is_new = merger.upload_these[0]
        decompressed = decompress(new_contents)
        
        self.assertTrue(is_new)
        self.assertEqual(size_uncompressed, len(decompressed))
        
        lines = decompressed.splitlines()
        self.assertEqual(len(lines), 2)  # Header + 1 data line
        self.assertEqual(lines[0], POWER_STATE_HEADER_ANDROID)
        self.assertEqual(lines[1], expected_line)
    
    def test_csv_merger_separate_time_bins(self):
        binified_data, null_handler = self.binified_and_handler
        data_bin_1: BinifyKey = (*self.bin_start, BIN_1, POWER_STATE_HEADER_ANDROID)
        data_bin_2: BinifyKey = (*self.bin_start, BIN_2, POWER_STATE_HEADER_ANDROID)
        binified_data[data_bin_1] = (POWER_STATE_ROWS_1, [1])
        binified_data[data_bin_2] = (POWER_STATE_ROWS_2, [2])
        merger = CsvMerger(binified_data, null_handler, self.default_participant, None, None)
        
        # Verify both chunks were processed
        self.assertEqual(len(merger.upload_these), 2)
        self.assertEqual(merger.earliest_time_bin, BIN_1)
        self.assertEqual(merger.latest_time_bin, BIN_2)
        
        # Verify all FTPs were processed
        self.assertIn(1, merger.ftps_to_retire)
        self.assertIn(2, merger.ftps_to_retire)
    
    @patch("libs.s3.conn")
    def test_csv_merger_chunk_exists_case(self, conn: Mock):
        binified_data, null_handler = self.binified_and_handler
        chunk_path = self.create_chunk()
        
        # Mock S3 to return existing data
        existing_data = construct_csv_as_bytes(POWER_STATE_HEADER_ANDROID, POWER_STATE_ROWS_1)
        setup_conn_retrieve_mock(conn, existing_data)
        
        # Set up binified data - use header with UTC time column since that's what convert_unix_to_human_readable_timestamps returns
        data_bin: BinifyKey = (*self.bin_start, BIN_1, POWER_STATE_HEADER_ANDROID)
        binified_data[data_bin] = (POWER_STATE_ROWS_2, [1, 2])
        
        merger = CsvMerger(binified_data, null_handler, self.default_participant, None, None)
        
        # Verify that the chunk was merged (not created new)
        self.assertEqual(len(merger.upload_these), 1)
        chunk_params, returned_chunk_path, new_contents, size_uncompressed, sha1_hash, is_new = merger.upload_these[0]
        
        # is_new should be False because we're updating an existing chunk
        self.assertFalse(is_new)
        self.assertEqual(returned_chunk_path, chunk_path)
        
        # Verify FTPs were processed
        succeeded, failed, _, _ = merger.get_retirees()
        self.assertEqual(succeeded, {1, 2})
        self.assertEqual(failed, set())
    
    def test_csv_merger_deduplicates_within_new_files(self):
        """Test that duplicate rows within new files are deduplicated"""
        binified_data, null_handler = self.binified_and_handler
        
        # Create duplicate rows - make separate list objects with same content
        # (not references to the same list, as that would cause mutation issues)
        rows = [
            [T1_BYTESTR, b"Locked"],
            [T1_BYTESTR, b"Locked"],  # Duplicate (same content, different list)
            [b"1770358250000", b"Unlocked"],
            [T1_BYTESTR, b"Locked"],  # Another duplicate
        ]
        
        data_bin: BinifyKey = (*self.bin_start, BIN_1, POWER_STATE_HEADER_ANDROID)
        binified_data[data_bin] = (rows, [1, 2, 3])
        
        merger = CsvMerger(binified_data, null_handler, self.default_participant, None, None)
        
        # Verify chunk was processed
        self.assertEqual(len(merger.upload_these), 1)
        chunk_params, chunk_path, new_contents, sha1_hash, size_uncompressed, is_new = merger.upload_these[0]
        
        #
        ## inserting manual hardcoded details here to sanity check this test during review.
        #
        known_correct_path = "CHUNKED_DATA/1234567890ABCDEFGHIJKMNO/patient1/power_state/2026-01-05T08:00:00.csv"
        self.assertEqual(
            chunk_params, {
                "study_id": self.default_study.id,
                "participant_id": self.default_participant.id,
                "data_type": "power_state",
                "chunk_path": known_correct_path,
                "chunk_hash": "uVriQ+TWRPtRJbjpRc23EQ==",
                "time_bin": 491000,
                "survey_id": None,
                "file_size": 116,
            }
        )
        
        decompressed_contents = decompress(new_contents)
        lines = decompressed_contents.splitlines()
        self.assertEqual(chunk_path, known_correct_path)
        self.assertEqual(size_uncompressed, len(decompressed_contents))
        self.assertEqual(
            [b"timestamp,UTC time,event",
             b"1770358145197,2026-02-06T06:09:05.197,Locked",
             b"1770358250000,2026-02-06T06:10:50.000,Unlocked"],
            lines,
        )
        self.assertTrue(is_new)
        
        # Should only have 3 lines: header + 2 unique data lines (not 4)
        # We had 4 rows with 3 duplicates of the first row, so should have 2 unique rows
        self.assertEqual(len(lines), 3, f"Should have header + 2 unique rows, got {len(lines)}")
        self.assertEqual(lines[0], POWER_STATE_HEADER_ANDROID)
        
        # Verify the duplicate row appears only once
        # After conversion, the row will have UTC time inserted
        expected_duplicate_line = b"1770358145197,2026-02-06T06:09:05.197,Locked"
        expected_line_2 = b"1770358250000,2026-02-06T06:10:50.000,Unlocked"
        
        duplicate_count = sum(1 for line in lines if line == expected_duplicate_line)
        self.assertEqual(duplicate_count, 1, "Duplicate row should appear only once")
        # Verify both unique rows are present
        self.assertIn(expected_duplicate_line, lines)
        self.assertIn(expected_line_2, lines)
    
    @patch("libs.s3.conn")
    def test_csv_merger_deduplicates_new_against_existing_s3_data(self, conn: Mock):
        """Test that duplicate rows between existing S3 data and new files are deduplicated"""
        binified_data, null_handler = self.binified_and_handler
        self.create_chunk()
        
        # Existing S3 data already has UTC time column (it"s been processed before)
        # Contains row 1 and row 2
        existing_rows_with_utc = [
            [T1_BYTESTR, T1_UTC, b"Locked"],
            [b"1770358250000", b"2026-02-06T06:10:50.000", b"Unlocked"],
        ]
        self.assertEqual(  # Sanity check
            binify_from_timecode(T1_BYTESTR), binify_from_timecode(existing_rows_with_utc[1][0])
        )
        
        existing_data = construct_csv_as_bytes(POWER_STATE_HEADER_ANDROID, existing_rows_with_utc)
        setup_conn_retrieve_mock(conn, existing_data)
        
        # New data does NOT have UTC time column yet (raw from device)
        # Contains row 1 (duplicate) and row 3 (new)
        new_rows_without_utc = [
            [T1_BYTESTR, b"Locked"],  # Duplicate of existing data
            [b"1770358350000", b"Locked"],  # New data
        ]
        
        data_bin: BinifyKey = (*self.bin_start, BIN_1, POWER_STATE_HEADER_ANDROID)
        binified_data[data_bin] = (new_rows_without_utc, [1, 2])
        
        merger = CsvMerger(binified_data, null_handler, self.default_participant, None, None)
        
        # Verify merge occurred  -  not manually rechecking all of these, only did that once
        self.assertEqual(len(merger.upload_these), 1)
        chunk_params, returned_chunk_path, new_contents, sha1_hash, size_uncompressed, is_new = merger.upload_these[0]
        
        self.assertFalse(is_new)
        decompressed = decompress(new_contents)
        lines = decompressed.splitlines()
        
        # Should have header + 3 unique data lines (not 4)
        # Existing: row1, row2
        # New: row1 (dup), row3
        # Result: row1, row2, row3 (3 unique rows)
        self.assertEqual(len(lines), 4, f"Should have header + 3 unique rows, got {len(lines)} lines: {lines}")
        self.assertEqual(lines[0], POWER_STATE_HEADER_ANDROID)
        
        # Verify row 1 appears only once despite being in both old and new data
        expected_duplicate_line = b"1770358145197,2026-02-06T06:09:05.197,Locked"
        duplicate_count = sum(1 for line in lines if line == expected_duplicate_line)
        self.assertEqual(duplicate_count, 1, "Duplicate row from existing data should appear only once")
        
        # Verify all 3 unique rows are present
        expected_lines = [
            expected_duplicate_line,                            # row 1
            b"1770358250000,2026-02-06T06:10:50.000,Unlocked",  # row 2 (from existing)
            b"1770358350000,2026-02-06T06:12:30.000,Locked",    # row 3 (new)
        ]
        self.assertEqual(lines[1:], expected_lines)
    
    @patch("libs.s3.conn")
    def test_csv_merger_multiple_duplicates_across_old_and_new(self, conn: Mock):
        binified_data, null_handler = self.binified_and_handler
        self.create_chunk()
        
        T_A, T_B, T_C = b"1770358250000", b"1770358350000", b"1770358450000"
        
        # Existing data already has UTC time: row1, row2, row3
        existing_rows_with_utc = [
            [T1_BYTESTR, b"2026-02-06T06:09:05.197", b"Locked"],
            [T_A, b"2026-02-06T06:10:50.000", b"Unlocked"],
            [T_B, b"2026-02-06T06:12:30.000", b"Locked"],
        ]
        
        existing_data = construct_csv_as_bytes(POWER_STATE_HEADER_ANDROID, existing_rows_with_utc)
        setup_conn_retrieve_mock(conn, existing_data)
        
        # New data without UTC time: row1 (dup), row2 (dup), row4 (new), row1 again (dup within new)
        new_rows_without_utc = [
            [T1_BYTESTR, b"Locked"],  # Duplicate from existing
            [T_A, b"Unlocked"],  # Duplicate from existing
            [T_C, b"Unlocked"],  # New unique row
            [T1_BYTESTR, b"Locked"],  # Duplicate within new data
        ]
        
        data_bin: BinifyKey = (*self.bin_start, BIN_1, POWER_STATE_HEADER_ANDROID)
        binified_data[data_bin] = (new_rows_without_utc, [1, 2, 3])
        
        merger = CsvMerger(binified_data, null_handler, self.default_participant, None, None)
        
        # Verify merge occurred
        self.assertEqual(len(merger.upload_these), 1)
        chunk_params, returned_chunk_path, new_contents, sha1_hash, size_uncompressed, is_new = merger.upload_these[0]
        
        decompressed = decompress(new_contents)
        lines = decompressed.splitlines()
        
        # Should have header + 4 unique rows total
        # Existing: row1, row2, row3
        # New: row1 (dup), row2 (dup), row4 (new), row1 (dup)
        # Result: row1, row2, row3, row4 (4 unique)
        self.assertEqual(len(lines), 5, f"Should have header + 4 unique rows, got {len(lines)} lines: {lines}")
        self.assertEqual(lines[0], POWER_STATE_HEADER_ANDROID)
        
        # Verify all expected unique rows are present
        expected_lines = [
            b"1770358145197,2026-02-06T06:09:05.197,Locked",
            b"1770358250000,2026-02-06T06:10:50.000,Unlocked",
            b"1770358350000,2026-02-06T06:12:30.000,Locked",
            b"1770358450000,2026-02-06T06:14:10.000,Unlocked",
        ]
        self.assertEqual(lines[1:], expected_lines)
        
        # Verify each row appears exactly once
        for expected_line in expected_lines:
            count = sum(1 for line in lines if line == expected_line)
            self.assertEqual(count, 1, f"Row {expected_line} should appear exactly once")


# AI generated, manually review, minor tweaks
class TestFileProcessingTracker(CommonTestCase):
    """Tests for the FileProcessingTracker class"""
    
    @property
    def study_participant_start(self):
        return f"{self.DEFAULT_STUDY_OBJECT_ID}/{self.default_participant.patient_id}"
    
    def test_init(self):
        tracker = FileProcessingTracker(self.default_participant)
        
        # Verify basic attributes
        self.assertEqual(tracker.participant, self.default_participant)
        self.assertEqual(tracker.study, self.default_participant.study)
        self.assertEqual(tracker.study_object_id, self.default_participant.study.object_id)
        self.assertEqual(tracker.patient_id, self.default_participant.patient_id)
        self.assertEqual(tracker.page_size, FILE_PROCESS_PAGE_SIZE)
        
        # Verify data structures are initialized
        self.assertEqual(len(tracker.all_binified_data), 0)
        self.assertIsNone(tracker.survey_object_id)
        self.assertEqual(len(tracker.buggy_files), 0)
    
    def test_init_with_custom_page_size(self):
        custom_page_size = 50
        tracker = FileProcessingTracker(self.default_participant, page_size=custom_page_size)
        self.assertEqual(tracker.page_size, custom_page_size)
    
    # file pagination
    
    def test_get_paginated_files_to_process_empty(self):
        tracker = FileProcessingTracker(self.default_participant, page_size=10)
        
        with patch("libs.file_processing.file_processing_core.logd"):
            pages = list(tracker.get_paginated_files_to_process())
        
        # Should return one empty page
        self.assertEqual(len(pages), 1)
        self.assertEqual(len(pages[0]), 0)
    
    def test_get_paginated_files_to_process_single_page(self):
        """Test get_paginated_files_to_process with files fitting in one page"""
        
        # Create 5 files to process
        for i in range(5):
            self.generate_file_to_process(
                path=f"study/participant/powerState/{i}.csv",
                participant=self.default_participant,
                os_type=ANDROID_API,
            )
        
        tracker = FileProcessingTracker(self.default_participant, page_size=10)
        with patch("libs.file_processing.file_processing_core.logd"):
            pages = list(tracker.get_paginated_files_to_process())
        
        # Should return one page with 5 files
        self.assertEqual(len(pages), 1)
        self.assertEqual(len(pages[0]), 5)
    
    def test_get_paginated_files_to_process_multiple_pages(self):
        """Test get_paginated_files_to_process with pagination"""
        
        # Create 25 files to process
        for i in range(25):
            self.generate_file_to_process(
                path=f"study/participant/powerState/{i}.csv",
                participant=self.default_participant,
                os_type=ANDROID_API,
            )
        
        tracker = FileProcessingTracker(self.default_participant, page_size=10)
        
        with patch("libs.file_processing.file_processing_core.logd"):
            pages = list(tracker.get_paginated_files_to_process())
        
        # Should return 3 pages: 10, 10, 5
        self.assertEqual(len(pages), 3)
        self.assertEqual(len(pages[0]), 10)
        self.assertEqual(len(pages[1]), 10)
        self.assertEqual(len(pages[2]), 5)
    
    def test_get_paginated_files_to_process_excludes_deleted(self):
        """Test that get_paginated_files_to_process excludes deleted files"""
        
        # Create 5 regular files and 3 deleted files
        for i in range(5):
            self.generate_file_to_process(
                path=f"study/participant/powerState/{i}.csv",
                participant=self.default_participant,
                os_type=ANDROID_API,
            )
        
        for i in range(5, 8):
            self.generate_file_to_process(
                path=f"study/participant/powerState/{i}.csv",
                participant=self.default_participant,
                os_type=ANDROID_API,
                deleted=True,
            )
        
        tracker = FileProcessingTracker(self.default_participant, page_size=10)
        
        with patch("libs.file_processing.file_processing_core.logd"):
            pages = list(tracker.get_paginated_files_to_process())
        
        # Should only return the 5 non-deleted files
        self.assertEqual(len(pages), 1)
        self.assertEqual(len(pages[0]), 5)
    
    # binify
    
    def test_binify_csv_rows(self):
        tracker = FileProcessingTracker(self.default_participant)
        
        # Create some test rows with different timestamps
        rows = [
            [b"1768928568332", b"Locked", b"0.7"],  # Time bin 491369
            [b"1768928682951", b"Unlocked", b"0.7"],  # Same bin
            [b"1768932200000", b"Locked", b"0.7"],  # Different bin 491370
        ]
        
        header = b"timestamp,event,level"
        result = tracker.binify_csv_rows(rows, POWER_STATE, header)
        result = dict(result)  # easy to screw up when still a defaultdict``
        
        # Should have 2 bins
        self.assertEqual(len(result), 2)
        items = iter(result.items())
        
        # Verify bin structure
        key, bin_rows = next(items)
        study_id, patient_id, data_type, time_bin, bin_header = key
        self.assertEqual(study_id, tracker.study_object_id)
        self.assertEqual(patient_id, tracker.patient_id)
        self.assertEqual(data_type, POWER_STATE)
        self.assertEqual(bin_header, header)
        self.assertEqual(time_bin, 491369)
        self.assertEqual(len(bin_rows), 2)
        
        key, bin_rows = next(items)
        study_id, patient_id, data_type, time_bin, bin_header = key
        self.assertEqual(study_id, tracker.study_object_id)
        self.assertEqual(patient_id, tracker.patient_id)
        self.assertEqual(data_type, POWER_STATE)
        self.assertEqual(bin_header, header)
        self.assertEqual(time_bin, 491370)
        self.assertEqual(len(bin_rows), 1)
    
    def test_binify_csv_rows_skips_empty_rows(self):
        tracker = FileProcessingTracker(self.default_participant)
        
        rows = [
            [b"1768928568332", b"Locked", b"0.7"],  # bin 491369
            [b""],  # Empty row
            [],  # Another empty row
            [b"1768928682951", b"Unlocked", b"0.7"],  # bin 491369
        ]
        
        header = b"timestamp,event,level"
        result = tracker.binify_csv_rows(rows, POWER_STATE, header)
        result = dict(result)
        
        # Should only have rows with valid timestamps
        items = iter(result.items())
        key, bin_rows = next(items)
        self.assertEqual(len(bin_rows), 2)
        self.assertEqual(bin_rows[0], [b"1768928568332", b"Locked", b"0.7"])
        self.assertEqual(bin_rows[1], [b"1768928682951", b"Unlocked", b"0.7"])
    
    def test_binify_csv_rows_skips_bad_timecode(self):
        tracker = FileProcessingTracker(self.default_participant)
        rows = [
            [b"1768928568332", b"Locked", b"0.7"],  # Valid
            [b"invalid", b"Unlocked", b"0.7"],  # Invalid timecode
            [b"1768928682951", b"Unlocked", b"0.7"],  # Valid
        ]
        
        header = b"timestamp,event,level"
        result = tracker.binify_csv_rows(rows, POWER_STATE, header)
        
        # Should only have 2 valid rows
        total_rows = sum(len(bin_rows) for bin_rows in result.values())
        self.assertEqual(total_rows, 2)
    
    def test_append_binified_csvs(self):
        tracker = FileProcessingTracker(self.default_participant)
        
        # Create a file to process
        ftp = self.generate_file_to_process(
            path=f"{self.study_participant_start}/powerState/1.csv",
            participant=self.default_participant,
            os_type=ANDROID_API,
        )
        
        # Create some binified data
        time_bin = 491369
        header = b"timestamp,event,level"
        data_bin: BinifyKey = (
            tracker.study_object_id,
            tracker.patient_id,
            POWER_STATE,
            time_bin,
            header,
        )
        
        new_binified_rows = {data_bin: [[b"1768928568332", b"Locked", b"0.7"]]}
        
        # Append the data
        tracker.append_binified_csvs(new_binified_rows, ftp)  # type: ignore - do not care about invariant
        
        # Verify data was appended
        self.assertIn(data_bin, tracker.all_binified_data)
        
        rows, ftps = tracker.all_binified_data[data_bin]
        self.assertEqual(len(rows), 1)
        self.assertEqual(ftps, [ftp.pk])
        
        self.assertEqual(len(tracker.all_binified_data), 1)
    
    def test_append_binified_csvs_multiple_appends(self):
        tracker = FileProcessingTracker(self.default_participant)
        
        ftp1 = self.generate_file_to_process(
            path=f"{self.study_participant_start}/powerState/1.csv",
            participant=self.default_participant,
            os_type=ANDROID_API,
        )
        ftp2 = self.generate_file_to_process(
            path=f"{self.study_participant_start}/powerState/2.csv",
            participant=self.default_participant,
            os_type=ANDROID_API,
        )
        
        time_bin = 491369
        header = b"timestamp,event,level"
        data_bin: BinifyKey = (
            tracker.study_object_id, tracker.patient_id, POWER_STATE, time_bin, header
        )
        
        # First append
        new_binified_rows_1 = {data_bin: [[b"1768928568332", b"Locked", b"0.7"]]}
        tracker.append_binified_csvs(new_binified_rows_1, ftp1)
        
        # Second append to same bin
        new_binified_rows_2 = {data_bin: [[b"1768928682951", b"Unlocked", b"0.7"]]}
        tracker.append_binified_csvs(new_binified_rows_2, ftp2)
        
        # Verify both were appended
        rows, ftps = tracker.all_binified_data[data_bin]
        self.assertEqual(len(rows), 2)
        self.assertEqual(ftps, [ftp1.pk, ftp2.pk])
        self.assertEqual(len(tracker.all_binified_data), 1)
    
    @patch("libs.s3.conn")
    def test_process_csv_data(self, conn: Mock):
        setup_conn_retrieve_mock(conn, input_power_state_content)
        
        tracker = FileProcessingTracker(self.default_participant)
        ftp = self.generate_file_to_process(
            path=f"{self.study_participant_start}/powerState/1768928568332.csv",
            participant=self.default_participant,
            os_type=ANDROID_API,
        )
        
        ffp = FileForProcessing(ftp, self.default_study)
        binified_data = tracker.process_csv_data(ffp)
        
        # Should return binified data
        self.assertIsNotNone(binified_data)
        self.assertIsInstance(binified_data, dict)
        assert binified_data is not None
        self.assertGreater(len(binified_data), 0)
    
    @patch("libs.s3.conn")
    def test_process_csv_data_empty_file(self, conn: Mock):
        setup_conn_retrieve_mock(conn, b"timestamp,event,level")
        
        tracker = FileProcessingTracker(self.default_participant)
        ftp = self.generate_file_to_process(
            path=f"{self.study_participant_start}/powerState/1768928568332.csv",
            participant=self.default_participant,
            os_type=ANDROID_API,
        )
        
        ffp = FileForProcessing(ftp, self.default_study)
        binified_data = tracker.process_csv_data(ffp)
        self.assertIsNone(binified_data)
    
    @patch("libs.s3.conn")
    def test_process_unchunkable_file(self, conn: Mock):
        setup_conn_retrieve_mock(conn, b"some audio recording data")  # unchunkable
        
        tracker = FileProcessingTracker(self.default_participant)
        ftp = self.generate_file_to_process(
            path=f"{self.DEFAULT_STUDY_OBJECT_ID}/{self.default_participant.patient_id}/voiceRecording/1768928568332.wav",
            participant=self.default_participant,
            os_type=ANDROID_API,
        )
        ffp = FileForProcessing(ftp, self.default_study)
        tracker.process_unchunkable_file(ffp)
        
        # Verify ChunkRegistry was created
        chunk_registries = ChunkRegistry.objects.filter(
            participant=self.default_participant, data_type=AUDIO_RECORDING
        )
        self.assertEqual(chunk_registries.count(), 1)
        self.assertFalse(FileToProcess.objects.filter(pk=ftp.pk).exists())  # Verify FTP was deleted
    
    @patch("libs.s3.conn")
    def test_process_chunkable_file(self, conn: Mock):
        setup_conn_retrieve_mock(conn, input_power_state_content)
        
        tracker = FileProcessingTracker(self.default_participant)
        ftp = self.generate_file_to_process(
            path=f"{self.study_participant_start}/powerState/1768928568332.csv",
            participant=self.default_participant,
            os_type=ANDROID_API,
        )
        
        ffp = FileForProcessing(ftp, self.default_study)
        tracker.process_chunkable_file(ffp)
        
        # Verify binified data exists
        self.assertGreater(len(tracker.all_binified_data), 0)
        
        # Verify FTP was not deleted yet (happens later in the pipeline)
        self.assertTrue(FileToProcess.objects.filter(pk=ftp.pk).exists())
    
    @patch("libs.s3.conn")
    def test_process_chunkable_file_empty(self, conn: Mock):
        setup_conn_retrieve_mock(conn, b"timestamp,event,level")
        
        tracker = FileProcessingTracker(self.default_participant)
        ftp = self.generate_file_to_process(
            path=f"{self.study_participant_start}/powerState/1768928568332.csv",
            participant=self.default_participant,
            os_type=ANDROID_API,
        )
        
        ffp = FileForProcessing(ftp, self.default_study)
        tracker.process_chunkable_file(ffp)
        self.assertEqual(len(tracker.all_binified_data), 0)  # Verify no binified data was populated
        self.assertFalse(FileToProcess.objects.filter(pk=ftp.pk).exists())  # FTP was deleted for empty file
    
    @patch("libs.s3.conn")
    def test_process_one_file_chunkable(self, conn: Mock):
        setup_conn_retrieve_mock(conn, input_power_state_content)
        
        tracker = FileProcessingTracker(self.default_participant)
        ftp = self.generate_file_to_process(
            path=f"{self.study_participant_start}/powerState/1768928568332.csv",
            participant=self.default_participant,
            os_type=ANDROID_API,
        )
        
        ffp = FileForProcessing(ftp, self.default_study)
        tracker.process_one_file(ffp)
        
        # Should have binified data
        self.assertGreater(len(tracker.all_binified_data), 0)
    
    @patch("libs.s3.conn")
    def test_process_one_file_unchunkable(self, conn: Mock):
        setup_conn_retrieve_mock(conn, b"audio recording data")
        
        self.assertEqual(ChunkRegistry.objects.count(), 0)
        tracker = FileProcessingTracker(self.default_participant)
        ftp = self.generate_file_to_process(
            path=f"{self.study_participant_start}/voiceRecording/1768928568332.wav",
            participant=self.default_participant,
            os_type=ANDROID_API,
        )
        
        ffp = FileForProcessing(ftp, self.default_study)
        tracker.process_one_file(ffp)
        self.assertEqual(ChunkRegistry.objects.count(), 1)
    
    @patch("libs.s3.conn")
    def test_full_easy_run_with_valid_data_processing_power_state(self, conn: Mock):
        # - CSV files with realistic power state data spanning multiple time bins  (2)
        # - Verifies the entire processing pipeline works end-to-end
        conn.put_object = Mock()  # Mock S3 upload to track calls and parameters
        # Expected outcome:
        # - All FileToProcess records are successfully deleted
        # - ChunkRegistry entries are created for each time bin
        # - Binified data is properly merged and uploaded
        
        # multiple power state CSV files with data spanning multiple hours
        # These will end up in different time bins based on their timestamps
        p = self.default_participant
        # Setup mock S3 responses - configure side_effect to return encrypted data for each call
        encrypted_responses = [
            self.true_default_s3_form(FILE_DATA1),
            self.true_default_s3_form(FILE_DATA2),
            self.true_default_s3_form(FILE_DATA3),
        ]
        
        # Use side_effect to return encrypted BytesIO objects on successive calls
        conn.get_object.side_effect = [
            {"Body": BytesIO(encrypted_responses[0])},
            {"Body": BytesIO(encrypted_responses[1])},
            {"Body": BytesIO(encrypted_responses[2])}
        ]
        
        # generate core FileToProcessesesses
        t1, t2, t3 = "1768928568332", "1768929245717", "1768932200000"
        self.assertEqual(binify_from_timecode(t1), 491369)  # 2 time bins
        self.assertEqual(binify_from_timecode(t2), 491369)
        self.assertEqual(binify_from_timecode(t3), 491370)
        
        base_str = f"{self.study_participant_start}/powerState"
        path = f"{base_str}/{t1}.csv"
        S3File(path=path + ".zst", sha1=path.encode()[:16]).save()
        self.generate_file_to_process(path=path, os_type=ANDROID_API)
        path = f"{base_str}/{t2}.csv"
        S3File(path=path + ".zst", sha1=path.encode()[:16]).save()
        self.generate_file_to_process(path=path, os_type=ANDROID_API)
        path = f"{base_str}/{t3}.csv"
        S3File(path=path + ".zst", sha1=path.encode()[:16]).save()
        self.generate_file_to_process(path=path, os_type=ANDROID_API)
        self.assertEqual(FileToProcess.objects.count(), 3)
        
        with patch("libs.file_processing.file_processing_core.logd"):  # suppress logging....
            easy_run(self.default_participant)
        
        # All FileToProcess records should be deleted
        self.assertEqual(FileToProcess.objects.count(), 0)
        
        # 2 calls to s3 upload, 2 ChunkRegistries (one per time bin)
        chunk_registries = ChunkRegistry.objects.filter(participant=p, data_type=POWER_STATE)
        self.assertEqual(chunk_registries.count(), 2)
        self.assertEqual(conn.put_object.call_count, 2)
        
        # Verify each chunk registry has correct data
        for chunk in chunk_registries:
            self.assertEqual(chunk.data_type, POWER_STATE)
            self.assertEqual(chunk.participant.id, p.id)
            self.assertIsNotNone(chunk.chunk_path)
            self.assertIn("power_state", chunk.chunk_path)
            self.assertIsNotNone(chunk.chunk_hash)
            self.assertGreater(chunk.file_size, 0)
            self.assertTrue(chunk.is_chunkable)
        
        self.assertEqual(len(conn.put_object.call_args_list), 2)
        
        # ug this order is not guaranteed
        if "2026-01-20T17:00:00.csv" in conn.put_object.call_args_list[0][1]["Key"]:
            upload1_body = conn.put_object.call_args_list[0][1]["Body"]
            upload2_body = conn.put_object.call_args_list[1][1]["Body"]
        else:
            upload2_body = conn.put_object.call_args_list[0][1]["Body"]
            upload1_body = conn.put_object.call_args_list[1][1]["Body"]
        
        # decrypt and decompress both uploaded files
        upload1_body = decrypt_server(upload1_body, self.default_study.encryption_key.encode())
        upload2_body = decrypt_server(upload2_body, self.default_study.encryption_key.encode())
        upload1_body = decompress(upload1_body)
        upload2_body = decompress(upload2_body)
        
        content1 = FILE_DATA1.splitlines()[1:]  # skip header
        content2 = FILE_DATA2.splitlines()[1:]
        content3 = FILE_DATA3.splitlines()[1:]
        
        # the lines get broken up with the utc timestamp inserted, testing for the raw timestamp is sufficient
        for line in content1:
            t = line.split(b",")[0]
            self.assertIn(t, upload1_body)
            self.assertNotIn(t, upload2_body)
        for line in content2:
            t = line.split(b",")[0]
            self.assertIn(t, upload1_body)
            self.assertNotIn(t, upload2_body)
        for line in content3:
            t = line.split(b",")[0]
            self.assertIn(t, upload2_body)
            self.assertNotIn(t, upload1_body)
    
    @patch("libs.s3.conn")
    def test_easy_run_with_survey_timings_multiple_mixed_surveys(self, conn: Mock):
        # - Two different survey timings data
        # - Both surveys have data in shared time bins (same hour)
        # - Both surveys have data in separate time bins (different hours)
        # - Verifies timings-specific processing and chunking
        
        # Expected outcome:
        # - All FileToProcess records are successfully deleted
        # - ChunkRegistry entries are created per survey and time bin
        # - Survey IDs are properly separated in chunk paths
        # - Binified data is properly merged and uploaded per survey
        
        # validate the data in the files corresponds to the expected time bins - yes this matches
        # the descriptions
        ln1, ln2 = SURVEY1_FILE1_DATA.splitlines()[1:]
        self.assertEqual(ln1.split(b",")[0], t1 := b"1768928568332")
        self.assertEqual(ln2.split(b",")[0], t2 := b"1768928682951")
        assert (binify_from_timecode(t1), binify_from_timecode(t2)) == (491369, 491369)
        ln1, ln2 = SURVEY1_FILE2_DATA.splitlines()[1:]
        self.assertEqual(ln1.split(b",")[0], t1 := b"1768929245717")
        self.assertEqual(ln2.split(b",")[0], t2 := b"1768929248038")
        assert (binify_from_timecode(t1), binify_from_timecode(t2)) == (491369, 491369)
        ln1, ln2 = SURVEY1_FILE3_DATA.splitlines()[1:]
        self.assertEqual(ln1.split(b",")[0], t1 := b"1768932200000")
        self.assertEqual(ln2.split(b",")[0], t2 := b"1768932350000")
        assert (binify_from_timecode(t1), binify_from_timecode(t2)) == (491370, 491370)
        ln1, ln2 = SURVEY2_FILE1_DATA.splitlines()[1:]
        self.assertEqual(ln1.split(b",")[0], t1 := b"1768928700000")
        self.assertEqual(ln2.split(b",")[0], t2 := b"1768928800000")
        assert (binify_from_timecode(t1), binify_from_timecode(t2)) == (491369, 491369)
        ln1, ln2 = SURVEY2_FILE2_DATA.splitlines()[1:]
        self.assertEqual(ln1.split(b",")[0], t1 := b"1768932100000")
        self.assertEqual(ln2.split(b",")[0], t2 := b"1768932400000")
        assert (binify_from_timecode(t1), binify_from_timecode(t2)) == (491370, 491370)
        ln1, ln2 = SURVEY2_FILE3_DATA.splitlines()[1:]
        self.assertEqual(ln1.split(b",")[0], t1 := b"1768935800000")
        self.assertEqual(ln2.split(b",")[0], t2 := b"1768935900000")
        assert (binify_from_timecode(t1), binify_from_timecode(t2)) == (491371, 491371)
        
        ##########################################################################################
        
        p = self.default_participant
        
        survey1_obj_id = "survey_abc12300000000000"  # 24 character requirement
        survey2_obj_id = "survey_xyz78900000000000"
        survey1 = self.generate_survey(self.default_study, Survey.TRACKING_SURVEY, survey1_obj_id)
        survey2 = self.generate_survey(self.default_study, Survey.TRACKING_SURVEY, survey2_obj_id)
        
        # Setup mock S3 responses - configure side_effect to return encrypted data for each call
        encrypted_responses = [
            DatabaseHelperMixin.true_default_s3_form(SURVEY1_FILE1_DATA),
            DatabaseHelperMixin.true_default_s3_form(SURVEY1_FILE2_DATA),
            DatabaseHelperMixin.true_default_s3_form(SURVEY1_FILE3_DATA),
            DatabaseHelperMixin.true_default_s3_form(SURVEY2_FILE1_DATA),
            DatabaseHelperMixin.true_default_s3_form(SURVEY2_FILE2_DATA),
            DatabaseHelperMixin.true_default_s3_form(SURVEY2_FILE3_DATA),
        ]
        # Use side_effect to return encrypted BytesIO objects on successive calls
        conn.get_object.side_effect = [
            {"Body": BytesIO(resp)} for resp in encrypted_responses
        ]
        
        
        # Create FileToProcess records for survey1 and survey2
        base_str1 = f"{self.study_participant_start}/surveyTimings/{survey1_obj_id}"
        base_str2 = f"{self.study_participant_start}/surveyTimings/{survey2_obj_id}"
        path = f"{base_str1}/1768928568332.csv"
        S3File(path=path + ".zst", sha1=path.encode()[:16]).save()
        self.generate_file_to_process(path=path, os_type=ANDROID_API)
        path = f"{base_str1}/1768929245717.csv"
        S3File(path=path + ".zst", sha1=path.encode()[:16]).save()
        self.generate_file_to_process(path=path, os_type=ANDROID_API)
        path = f"{base_str1}/1768932200000.csv"
        S3File(path=path + ".zst", sha1=path.encode()[:16]).save()
        self.generate_file_to_process(path=path, os_type=ANDROID_API)
        path = f"{base_str2}/1768928700000.csv"
        S3File(path=path + ".zst", sha1=path.encode()[:16]).save()
        self.generate_file_to_process(path=path, os_type=ANDROID_API)
        path = f"{base_str2}/1768932100000.csv"
        S3File(path=path + ".zst", sha1=path.encode()[:16]).save()
        self.generate_file_to_process(path=path, os_type=ANDROID_API)
        path = f"{base_str2}/1768935800000.csv"
        S3File(path=path + ".zst", sha1=path.encode()[:16]).save()
        self.generate_file_to_process(path=path, os_type=ANDROID_API)
        
        
        # Verify initial state
        self.assertEqual(FileToProcess.objects.count(), 6)
        self.assertEqual(ChunkRegistry.objects.count(), 0)
        
        with patch("libs.file_processing.file_processing_core.logd"):  # please don't print stuff
            easy_run(p)
        
        self.assertEqual(FileToProcess.objects.count(), 0)  # should be cleared
        
        # ChunkRegistry entries should be created for survey timings
        all_survey_chunks = p.chunk_registries.order_by("time_bin", "survey__object_id")
        
        self.assertEqual(all_survey_chunks.count(), 5)   # new chunks
        self.assertEqual(conn.put_object.call_count, 5)  # uploads
        
        # altarnate check
        # this is the output files as they should exist
        # exists = list(all_survey_chunks.vlist("chunk_path"))
        # correct = [
        #     "survey_abc12300000000000/2026-01-20T17:00:00.csv",
        #     "survey_xyz78900000000000/2026-01-20T17:00:00.csv",
        #     "survey_abc12300000000000/2026-01-20T18:00:00.csv",
        #     "survey_xyz78900000000000/2026-01-20T18:00:00.csv",
        #     "survey_xyz78900000000000/2026-01-20T19:00:00.csv",
        # ]
        # for i in range(len(correct)):
        #     self.assertIn(correct[i], exists[i])
        
        # Verify survey-specific chunk registries
        # Both surveys should have chunks created
        survey1_chunks = p.chunk_registries.filter(survey=survey1)
        survey2_chunks = p.chunk_registries.filter(survey=survey2)
        self.assertEqual(survey1_chunks.count(), 2)
        self.assertEqual(survey2_chunks.count(), 3)
        
        # Verify each chunk registry has correct data and survey_id in path
        for chunk in survey1_chunks:
            self.assertEqual(chunk.data_type, SURVEY_TIMINGS)
            self.assertEqual(chunk.participant, p)
            self.assertEqual(chunk.survey.pk, survey1.pk)
            self.assertIsNotNone(chunk.chunk_path)
            self.assertIn(SURVEY_TIMINGS, chunk.chunk_path)
            self.assertIn(survey1_obj_id, chunk.chunk_path)
            self.assertNotIn(survey2_obj_id, chunk.chunk_path)
            self.assertIsNotNone(chunk.chunk_hash)
            self.assertGreater(chunk.file_size, 0)
            self.assertTrue(chunk.is_chunkable)
        
        for chunk in survey2_chunks:
            self.assertEqual(chunk.data_type, SURVEY_TIMINGS)
            self.assertEqual(chunk.participant, p)
            self.assertEqual(chunk.survey.pk, survey2.pk)
            self.assertIsNotNone(chunk.chunk_path)
            self.assertIn(SURVEY_TIMINGS, chunk.chunk_path)
            self.assertIn(survey2_obj_id, chunk.chunk_path)
            self.assertNotIn(survey1_obj_id, chunk.chunk_path)
            self.assertIsNotNone(chunk.chunk_hash)
            self.assertGreater(chunk.file_size, 0)
            self.assertTrue(chunk.is_chunkable)
        
        
        # get the uploaded files out of put_object calles to veryfy content is correct:
        uploaded_files = []
        for call in conn.put_object.call_args_list:
            args, kwargs = call
            key = kwargs["Key"]
            body = kwargs["Body"]
            uploaded_files.append((key, body))
        
        raw_paths = [path + ".zst" for path in all_survey_chunks.vlist("chunk_path")]
        uploaded_files_dict = {key[:-4]: body for key, body in uploaded_files}  # need to strip .zst
        
        for chunk in survey1_chunks:
            self.assertIn(chunk.chunk_path, uploaded_files_dict)
            body = uploaded_files_dict[chunk.chunk_path]
            body = decrypt_server(body, self.default_study.encryption_key.encode())
            body = decompress(body)
            self.assertIn(b"survey_abc12300000000000", body)
            self.assertNotIn(b"survey_xyz78900000000000", body)
        
        for chunk in survey2_chunks:
            self.assertIn(chunk.chunk_path, uploaded_files_dict)
            body = uploaded_files_dict[chunk.chunk_path]
            body = decrypt_server(body, self.default_study.encryption_key.encode())
            body = decompress(body)
            self.assertIn(b"survey_xyz78900000000000", body)
            self.assertNotIn(b"survey_abc12300000000000", body)

# static data points for those last tests

# File 1: Early hour timestamps
FILE_DATA1 = b"""timestamp,event,level
1768928568332,Locked,0.7
1768928682951,Unlocked,0.7
1768928688711,Locked,0.7"""

# File 2: Later timestamps in same hour as file 1
FILE_DATA2 = b"""timestamp,event,level
1768929245717,Unlocked,0.7
1768929245724,Unlocked,0.7
1768929248038,Locked,0.7"""

# File 3: Timestamps in a different hour (next time bin)
FILE_DATA3 = b"""timestamp,event,level
1768932200000,Locked,0.7
1768932350000,Unlocked,0.7"""


#####################

# survey1_id = "survey_abc12300000000000"
# survey2_id = "survey_xyz78900000000000"
# Time bin 491369 - shared by both surveys

# Survey1 file 1 - in shared time bin
# actual bins:  (491369, 491369)
SURVEY1_FILE1_DATA = b"""timestamp,question id,survey id,question type,question text,question answer options,answer
1768928568332,q1,survey_abc12300000000000,radio_button,How are you feeling?,Good;Bad;Neutral,Good
1768928682951,q2,survey_abc12300000000000,free_response,Any comments?,N/A,Feeling great"""

# Survey1 file 2 - also in shared time bin 491369
# actual bins:  (491369, 491369)
SURVEY1_FILE2_DATA = b"""timestamp,question id,survey id,question type,question text,question answer options,answer
1768929245717,q1,survey_abc12300000000000,radio_button,How are you feeling?,Good;Bad;Neutral,Neutral
1768929248038,q2,survey_abc12300000000000,free_response,Any comments?,N/A,No comment"""

# Survey1 file 3 - in different time bin 491370
# actual bins:  (491370, 491370)
SURVEY1_FILE3_DATA = b"""timestamp,question id,survey id,question type,question text,question answer options,answer
1768932200000,q1,survey_abc12300000000000,radio_button,How are you feeling?,Good;Bad;Neutral,Bad
1768932350000,q2,survey_abc12300000000000,free_response,Any comments?,N/A,Not great"""

# Survey2 file 1 - in shared time bin 491369
# actual bins:  (491369, 491369)
SURVEY2_FILE1_DATA = b"""timestamp,question id,survey id,question type,question text,question answer options,answer
1768928700000,q1,survey_xyz78900000000000,slider,Rate your mood,0-10,7
1768928800000,q2,survey_xyz78900000000000,checkbox,Activities today,Sleep;Exercise;Work,Sleep;Work"""

# Survey2 file 2 - in time bin 491370 (shared with survey1 file3)
# actual bins:  (491370, 491370)

SURVEY2_FILE2_DATA = b"""timestamp,question id,survey id,question type,question text,question answer options,answer
1768932100000,q1,survey_xyz78900000000000,slider,Rate your mood,0-10,5
1768932400000,q2,survey_xyz78900000000000,checkbox,Activities today,Sleep;Exercise;Work,Exercise"""

# Survey2 file 3 - in separate time bin 491371 (alone)
# actual bins:  (491371, 491371)
SURVEY2_FILE3_DATA = b"""timestamp,question id,survey id,question type,question text,question answer options,answer
1768935800000,q1,survey_xyz78900000000000,slider,Rate your mood,0-10,8
1768935900000,q2,survey_xyz78900000000000,checkbox,Activities today,Sleep;Exercise;Work,Sleep;Exercise;Work"""

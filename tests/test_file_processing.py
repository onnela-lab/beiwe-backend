from collections import defaultdict
from datetime import datetime
from unittest.mock import Mock, patch

from cronutils import ErrorHandler, null_error_handler
from cronutils.error_handler import NullErrorHandler
from pyzstd import decompress

from constants.common_constants import CHUNKS_FOLDER, UTC
from constants.data_processing_constants import (AllBinifiedData, BinifyKey,
    CHUNK_TIMESLICE_QUANTUM, REFERENCE_CHUNKREGISTRY_HEADERS)
from constants.data_stream_constants import POWER_STATE
from constants.user_constants import ANDROID_API, IOS_API
from database.data_access_models import ChunkRegistry
from libs.file_processing.csv_merger import construct_s3_chunk_path, CsvMerger
from libs.file_processing.file_for_processing import FileForProcessing
from libs.file_processing.utility_functions_csvs import construct_csv_as_bytes
from libs.file_processing.utility_functions_simple import (binify_from_timecode,
    convert_unix_to_human_readable_timestamps)
from tests.common import CommonTestCase


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


class TestFileProcessing(CommonTestCase):
    raw_fp_good = "1234567890ABCDEFGHIJKMNO/patient1/powerState/1768928568332.csv"
    raw_fp_bad = "1234567890ABCDEFGHIJKMNO/patient1/power_state/1768928568332.csv"
    
    type_unknown_error = "data type unknown: 1234567890ABCDEFGHIJKMNO/patient1/power_state/1768928568332.csv"
    
    
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
    
    
    def test_binify_from_timecode(self):
        """Test that binify_from_timecode correctly calculates time bins"""
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
    
    
    @patch("libs.file_processing.file_for_processing.s3_retrieve")
    def test_binification_of_power_state_data(self, s3_retrieve: Mock):
        """Test that binification correctly groups data points by time bins"""
        s3_retrieve.return_value = input_power_state_content
        ftp = self.generate_file_to_process(path=self.raw_fp_good, os_type=ANDROID_API)
        
        ffp = FileForProcessing(ftp)
        ffp.prepare_data()
        
        # Now binify the data manually to test the binification logic
        binified: dict[int, list] = defaultdict(list)
        
        # Group rows by time bin
        for row in ffp.file_lines:
            if row and row[0]:
                time_bin = binify_from_timecode(row[0])
                binified[time_bin].append(row)
        
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
        # BinifyKey = tuple[str, str, str, int, bytes]  # study_object_id, patient_id, data_type, timecode int, header bytes
        study_id = ftp.study.object_id
        patient_id = ftp.participant.patient_id
        data_type = POWER_STATE
        header = ffp.header
        
        for time_bin, rows in binified.items():
            # This is the structure that would be used in actual processing
            binify_key: BinifyKey = (study_id, patient_id, data_type, time_bin, header)
            
            # Verify the key components are valid
            self.assertIsInstance(binify_key[0], str)  # study_object_id
            self.assertIsInstance(binify_key[1], str)  # patient_id
            self.assertIsInstance(binify_key[2], str)  # data_type
            self.assertIsInstance(binify_key[3], int)  # time_bin
            self.assertIsInstance(binify_key[4], bytes)  # header
    
    @patch("libs.file_processing.file_for_processing.s3_retrieve")
    def test_file_for_processing_instantiation_raises_data_type_unknowwn(self, s3_retrieve: Mock):
        s3_retrieve.return_value = input_power_state_content
        
        p = self.default_participant
        ftp = self.generate_file_to_process(
            path=self.raw_fp_bad,
            os_type=ANDROID_API,
        )
        with self.assertRaisesMessage(Exception, self.type_unknown_error):
            FileForProcessing(ftp)
    
    @patch("libs.file_processing.file_for_processing.s3_retrieve")
    def test_basic_file_for_processing_instantiation(self, s3_retrieve: Mock):
        s3_retrieve.return_value = input_power_state_content
        ftp = self.generate_file_to_process(path=self.raw_fp_good, os_type=ANDROID_API)
        
        ffp = FileForProcessing(ftp)
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
    @patch("libs.file_processing.file_for_processing.s3_retrieve")
    def test_clear_file_content(self, s3_retrieve: Mock):
        """Test that clear_file_content properly sets file_contents to None"""
        s3_retrieve.return_value = input_power_state_content
        ftp = self.generate_file_to_process(path=self.raw_fp_good, os_type=ANDROID_API)
        ffp = FileForProcessing(ftp)
        
        # Verify file_contents is populated
        self.assertIsNotNone(ffp.file_contents)
        
        # Clear it
        ffp.clear_file_content()
        
        # Verify it's now None
        self.assertIsNone(ffp.file_contents)
    
    @patch("libs.file_processing.file_for_processing.s3_retrieve")
    def test_clear_file_content_raises_on_already_cleared(self, s3_retrieve: Mock):
        """Test that clear_file_content raises AssertionError if called when already cleared"""
        s3_retrieve.return_value = input_power_state_content
        ftp = self.generate_file_to_process(path=self.raw_fp_good, os_type=ANDROID_API)
        ffp = FileForProcessing(ftp)
        
        ffp.clear_file_content()
        
        # Calling again should raise AssertionError
        with self.assertRaises(AssertionError):
            ffp.clear_file_content()
    
    @patch("libs.file_processing.file_for_processing.s3_retrieve")
    def test_raw_csv_to_line_list_multiline(self, s3_retrieve: Mock):
        """Test raw_csv_to_line_list with multi-line CSV content"""
        
        header = b'timestamp,event,level'
        s3_retrieve.return_value = input_power_state_content
        ftp = self.generate_file_to_process(path=self.raw_fp_good, os_type=ANDROID_API)
        ffp = FileForProcessing(ftp)
        
        ffp.raw_csv_to_line_list()        # Process the CSV
        self.assertIsNotNone(ffp.header)  # header was extracted
        self.assertEqual(ffp.header, header)
        
        self.assertIsNotNone(ffp.file_lines)  # file_lines were populated
        self.assertGreater(len(ffp.file_lines), 0)
        
        # each line was split into columns
        first_line = ffp.file_lines[0]
        self.assertEqual(len(first_line), 3)  # "timestamp, event, level"
        self.assertEqual(first_line[0], b'1768928568332')
        self.assertEqual(first_line[1], b'Locked')
        self.assertEqual(first_line[2], b'0.7')
        self.assertEqual(len(ffp.file_lines), 155)
        self.assertIsNone(ffp.file_contents)  # file_contents was cleared
    
    @patch("libs.file_processing.file_for_processing.s3_retrieve")
    def test_raw_csv_to_line_list_single_line(self, s3_retrieve: Mock):
        """Test raw_csv_to_line_list with single line (header only)"""
        
        s3_retrieve.return_value = single_line_content = b'timestamp,event,level'
        ftp = self.generate_file_to_process(path=self.raw_fp_good, os_type=ANDROID_API)
        ffp = FileForProcessing(ftp)
        ffp.raw_csv_to_line_list()
        self.assertEqual(ffp.header, single_line_content)  # header was set to the entire content
        self.assertEqual(ffp.file_lines, [])  # file_lines is an empty list
        self.assertIsNone(ffp.file_contents)  # file_contents was cleared
    
    @patch("libs.file_processing.file_for_processing.s3_retrieve")
    def test_raw_csv_to_line_list_requires_file_contents(self, s3_retrieve: Mock):
        """Test that raw_csv_to_line_list raises AssertionError if file_contents is None"""
        s3_retrieve.return_value = input_power_state_content
        ftp = self.generate_file_to_process(path=self.raw_fp_good, os_type=ANDROID_API)
        ffp = FileForProcessing(ftp)
        ffp.clear_file_content()
        
        # Calling raw_csv_to_line_list should raise AssertionError
        with self.assertRaises(AssertionError):
            ffp.raw_csv_to_line_list()
    
    @patch("libs.file_processing.file_for_processing.s3_retrieve")
    def test_prepare_data(self, s3_retrieve: Mock):
        """Test that prepare_data properly processes CSV data and cleans headers"""
        s3_retrieve.return_value = input_power_state_content
        ftp = self.generate_file_to_process(path=self.raw_fp_good, os_type=ANDROID_API)
        ffp = FileForProcessing(ftp)
        
        # Prepare the data
        ffp.prepare_data()
        
        # Verify file_lines were populated
        self.assertIsNotNone(ffp.file_lines)
        self.assertGreater(len(ffp.file_lines), 0)
        
        # Verify header was populated and cleaned (quotes removed, spaces stripped)
        self.assertIsNotNone(ffp.header)
        # The header should have whitespace stripped from each column
        self.assertNotIn(b' ', ffp.header)
        
        # Verify file_contents was cleared during processing
        self.assertIsNone(ffp.file_contents)
    
    @patch("libs.file_processing.file_for_processing.s3_retrieve")
    def test_clear_file_lines(self, s3_retrieve: Mock):
        """Test that clear_file_lines clears file_lines and header"""
        s3_retrieve.return_value = input_power_state_content
        ftp = self.generate_file_to_process(path=self.raw_fp_good, os_type=ANDROID_API)
        ffp = FileForProcessing(ftp)
        
        # Prepare data first to populate file_lines and header
        ffp.prepare_data()
        
        # Verify they're populated
        self.assertIsNotNone(ffp.file_lines)
        self.assertIsNotNone(ffp.header)
        
        # Clear them
        ffp.clear_file_lines()
        
        # Verify both are None
        self.assertIsNone(ffp.file_lines)
        self.assertIsNone(ffp.header)
    
    @patch("libs.file_processing.file_for_processing.s3_retrieve")
    def test_clear_file_lines_requires_file_contents_cleared(self, s3_retrieve: Mock):
        """Test that clear_file_lines requires file_contents to be cleared first"""
        s3_retrieve.return_value = input_power_state_content
        ftp = self.generate_file_to_process(path=self.raw_fp_good, os_type=ANDROID_API)
        ffp = FileForProcessing(ftp)
        
        ffp.prepare_data()
        # Don't clear file_contents - it should already be None after prepare_data
        
        # This should work fine since prepare_data clears file_contents
        ffp.clear_file_lines()
        self.assertIsNone(ffp.file_lines)
        self.assertIsNone(ffp.header)
    
    @patch("libs.file_processing.file_for_processing.s3_retrieve")
    def test_clear_file_lines_raises_on_already_cleared(self, s3_retrieve: Mock):
        """Test that clear_file_lines raises AssertionError if called when already cleared"""
        s3_retrieve.return_value = input_power_state_content
        ftp = self.generate_file_to_process(path=self.raw_fp_good, os_type=ANDROID_API)
        ffp = FileForProcessing(ftp)
        
        ffp.prepare_data()
        ffp.clear_file_lines()
        
        # Calling again should raise AssertionError
        with self.assertRaises(AssertionError):
            ffp.clear_file_lines()
    
    @patch("libs.file_processing.file_for_processing.s3_retrieve")
    def test_download_file_contents_success(self, s3_retrieve: Mock):
        """Test that download_file_contents properly retrieves and stores file contents"""
        s3_retrieve.return_value = input_power_state_content
        ftp = self.generate_file_to_process(path=self.raw_fp_good, os_type=ANDROID_API)
        ffp = FileForProcessing(ftp)
        
        # download_file_contents is called in __init__, so just verify the result
        self.assertEqual(ffp.file_contents, input_power_state_content)
        self.assertIsNone(ffp.exception)
        self.assertIsNone(ffp.traceback)
        
        # Verify s3_retrieve was called with correct parameters
        s3_retrieve.assert_called_once_with(
            ftp.s3_file_path,
            ftp.study.object_id,
            raw_path=True
        )
    
    # this prints junk and I don't care about testing this side effect, it is weird
    # @patch("libs.file_processing.file_for_processing.s3_retrieve")
    # def test_download_file_contents_failure(self, s3_retrieve: Mock):
    #     """Test that download_file_contents properly handles exceptions"""
    #     test_error = ValueError("S3 connection failed")
    #     s3_retrieve.side_effect = test_error
        
    #     ftp = self.generate_file_to_process(path=self.raw_fp_good, os_type=ANDROID_API)
        
    #     # Creating FileForProcessing should raise SomeException
    #     with self.assertRaises(SomeException):
    #         FileForProcessing(ftp)
    
    # also prints jung, and already tested I think
    # @patch("libs.file_processing.file_for_processing.s3_retrieve")
    # def test_raise_data_processing_error(self, s3_retrieve: Mock):
    #     """Test that raise_data_processing_error properly re-raises stored exceptions"""
    #     test_error = ValueError("Test error for processing")
    #     s3_retrieve.side_effect = test_error
        
    #     ftp = self.generate_file_to_process(path=self.raw_fp_good, os_type=ANDROID_API)
        
    #     # Catch the initial exception during instantiation
    #     try:
    #         FileForProcessing(ftp)
    #     except SomeException as e:
    #         pass
        
    #     # Now we can't test this directly without creating a modified version,
    #     # but we can verify the exception handling path works in download_file_contents


class FakeException(Exception): pass


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
    
    def basic_config(self) -> tuple[AllBinifiedData, ErrorHandler, dict]:
        binified_data: AllBinifiedData = defaultdict(lambda: ([], []))
        survey_id_dict = {}
        # return a null error handler so that errors actually get raised when they run for testing.
        return binified_data, null_error_handler(), survey_id_dict  # type: ignore
    
    ## Tests!
    
    # trivial
    
    def test_construct_s3_chunk_path(self):
        study_id = "test_study_id"
        patient_id = "test_patient"
        path = construct_s3_chunk_path(study_id, patient_id, POWER_STATE, BIN_1)
        
        # Verify the path structure
        self.assertTrue(path.startswith(CHUNKS_FOLDER))
        self.assertIn(study_id, path)
        self.assertIn(patient_id, path)
        self.assertIn(POWER_STATE, path)
        self.assertTrue(path.endswith(".csv"))
    
    
    @patch("libs.file_processing.csv_merger.s3_retrieve")
    def test_csv_merger_initialization_with_empty_data(self, s3_retrieve: Mock):
        binified_data, null_handler, survey_id_dict = self.basic_config()
        merger = CsvMerger(binified_data, null_handler, survey_id_dict, self.default_participant)
        
        # Verify empty state
        self.assertEqual(merger.failed_ftps, set())
        self.assertEqual(merger.ftps_to_retire, set())
        self.assertEqual(merger.upload_these, [])
        self.assertIsNone(merger.earliest_time_bin)
        self.assertIsNone(merger.latest_time_bin)
    
    
    @patch("libs.file_processing.csv_merger.s3_retrieve")
    def test_csv_merger_get_retirees(self, s3_retrieve: Mock):
        data_bin: BinifyKey = (
            self.default_study.object_id,
            self.default_participant.patient_id,
            POWER_STATE,
            BIN_1,
            POWER_STATE_HEADER_ANDROID,
        )
        binified_data, null_handler, survey_id_dict = self.basic_config()
        binified_data[data_bin] = (POWER_STATE_ROWS_1, [1, 2])
        
        merger = CsvMerger(binified_data, null_handler, survey_id_dict, self.default_participant)
        
        # Get retirees
        succeeded, failed, earliest, latest = merger.get_retirees()
        
        # All FTPs should be in succeeded set since no errors occurred
        
        self.assertEqual(succeeded, {1, 2})
        self.assertEqual(failed, set())
        self.assertEqual(earliest, BIN_1)
        self.assertEqual(latest, BIN_1)
        # sanity check
        self.assertEqual(len(merger.upload_these), 1)
    
    
    @patch("libs.file_processing.csv_merger.s3_retrieve")
    def test_csv_merger_get_retirees_with_network_failures(self, s3_retrieve: Mock):
        binified_data, _, survey_id_dict = self.basic_config()
        
        # First data bin will succeed
        data_bin_1: BinifyKey = (
            self.default_study.object_id,
            self.default_participant.patient_id,
            POWER_STATE,
            BIN_1,
            POWER_STATE_HEADER_ANDROID,
        )
        
        # Second data bin will fail (we'll set up S3 to fail for this one)
        data_bin_2: BinifyKey = (
            self.default_study.object_id,
            self.default_participant.patient_id,
            POWER_STATE,
            BIN_2,
            POWER_STATE_HEADER_ANDROID,
        )
        
        # Create a chunk registry for the second bin so S3 retrieval will be attempted
        chunk_path_2 = construct_s3_chunk_path(
            self.default_study.object_id,
            self.default_participant.patient_id,
            POWER_STATE,
            BIN_2,
        )
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
        merger = CsvMerger(binified_data, error_handler, survey_id_dict, self.default_participant)
        
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
        binified_data, _, survey_id_dict = self.basic_config()
        
        # Create a scenario that will cause an error: missing chunk in S3
        chunk_path = construct_s3_chunk_path(
            self.default_study.object_id,
            self.default_participant.patient_id,
            POWER_STATE,
            BIN_1,
        )
        
        # Create chunk registry but S3 will fail to retrieve it
        # Convert time_bin to datetime as required by the model
        ChunkRegistry.objects.create(
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
        data_bin: BinifyKey = (
            self.default_study.object_id,
            self.default_participant.patient_id,
            POWER_STATE,
            BIN_1,
            reference_header,
        )
        binified_data[data_bin] = (POWER_STATE_ROWS_1, [1, 2])
        
        # This should complete without raising, but mark FTPs as failed
        error_handler = ErrorHandler()  # this one needs a real error handler to record the error
        merger = CsvMerger(binified_data, error_handler, survey_id_dict, self.default_participant)
        
        # Verify FTPs were marked as failed
        succeeded, failed, _, _ = merger.get_retirees()
        self.assertEqual(failed, {1, 2})
        self.assertEqual(succeeded, set())
    
    # header validation logic
    
    @patch("libs.file_processing.csv_merger.s3_retrieve")
    def test_csv_merger_validate_one_header_matching(self, s3_retrieve: Mock):
        binified_data, null_handler, survey_id_dict = self.basic_config()
        
        merger = CsvMerger(binified_data, null_handler, survey_id_dict, self.default_participant)
        
        # Get the reference header, validate
        reference_header = REFERENCE_CHUNKREGISTRY_HEADERS[POWER_STATE][self.default_participant.os_type]
        result = merger.validate_one_header(reference_header, POWER_STATE)
        
        # Should return the reference header unchanged
        self.assertEqual(result, reference_header)
    
    
    @patch("libs.file_processing.csv_merger.s3_retrieve")
    def test_csv_merger_validate_one_header_mismatch(self, s3_retrieve: Mock):
        binified_data, null_handler, survey_id_dict = self.basic_config()
        
        merger = CsvMerger(binified_data, null_handler, survey_id_dict, self.default_participant)
        
        # Create a bad header, validate
        bad_header = b'timestamp,bad,header,columns'
        result = merger.validate_one_header(bad_header, POWER_STATE)
        
        # Should return the reference header, not the bad one
        reference_header = REFERENCE_CHUNKREGISTRY_HEADERS[POWER_STATE][self.default_participant.os_type]
        self.assertEqual(result, reference_header)
        self.assertNotEqual(result, bad_header)
    
    
    @patch("libs.file_processing.csv_merger.s3_retrieve")
    def test_csv_merger_validate_two_headers_identical(self, s3_retrieve: Mock):
        binified_data, null_handler, survey_id_dict = self.basic_config()
        
        merger = CsvMerger(binified_data, null_handler, survey_id_dict, self.default_participant)
        reference_header = REFERENCE_CHUNKREGISTRY_HEADERS[POWER_STATE][self.default_participant.os_type]
        
        # Both headers are the same
        result = merger.validate_two_headers(reference_header, reference_header, POWER_STATE)
        
        self.assertEqual(result, reference_header)  # Should return the reference header
    
    
    @patch("libs.file_processing.csv_merger.s3_retrieve")
    def test_csv_merger_validate_two_headers_both_match_reference(self, s3_retrieve: Mock):
        binified_data, null_handler, survey_id_dict = self.basic_config()
        
        merger = CsvMerger(binified_data, null_handler, survey_id_dict, self.default_participant)
        reference_header = REFERENCE_CHUNKREGISTRY_HEADERS[POWER_STATE][self.default_participant.os_type]
        
        # Both headers match reference
        result = merger.validate_two_headers(reference_header, reference_header, POWER_STATE)
        self.assertEqual(result, reference_header)  # Should return the reference header
    
    
    @patch("libs.file_processing.csv_merger.s3_retrieve")
    def test_csv_merger_validate_two_headers_one_matches_reference(self, s3_retrieve: Mock):
        binified_data, null_handler, survey_id_dict = self.basic_config()
        
        merger = CsvMerger(binified_data, null_handler, survey_id_dict, self.default_participant)
        reference_header = REFERENCE_CHUNKREGISTRY_HEADERS[POWER_STATE][self.default_participant.os_type]
        bad_header = b'timestamp,bad,header'
        
        # One header matches, one doesn't
        result = merger.validate_two_headers(reference_header, bad_header, POWER_STATE)
        self.assertEqual(result, reference_header)  # Should return the reference header
    
    
    @patch("libs.file_processing.csv_merger.s3_retrieve")
    def test_csv_merger_validate_two_headers_neither_matches(self, s3_retrieve: Mock):
        binified_data, null_handler, survey_id_dict = self.basic_config()
        
        merger = CsvMerger(binified_data, null_handler, survey_id_dict, self.default_participant)
        bad_header_1 = b'timestamp,bad,header1'
        bad_header_2 = b'timestamp,bad,header2'
        
        # Neither matches reference
        result = merger.validate_two_headers(bad_header_1, bad_header_2, POWER_STATE)
        
        reference_header = REFERENCE_CHUNKREGISTRY_HEADERS[POWER_STATE][self.default_participant.os_type]
        self.assertEqual(result, reference_header)  # Should still return the reference header
    
    
    # merging logic
    
    @patch("libs.file_processing.csv_merger.s3_retrieve")
    def test_csv_merger_chunk_not_exists_case(self, s3_retrieve: Mock):
        # Create test data
        data_bin: BinifyKey = (
            self.default_study.object_id,
            self.default_participant.patient_id,
            POWER_STATE,
            BIN_1,
            POWER_STATE_HEADER_ANDROID,
        )
        binified_data, null_handler, survey_id_dict = self.basic_config()
        binified_data[data_bin] = (POWER_STATE_ROWS_1, [1, 2])  # file_to_process PKs
        merger = CsvMerger(binified_data, null_handler, survey_id_dict, self.default_participant)
        
        # Verify that the chunk was processed
        self.assertEqual(len(merger.upload_these), 1)
        self.assertIn(1, merger.ftps_to_retire)  # two successes
        self.assertIn(2, merger.ftps_to_retire)
        self.assertEqual(merger.earliest_time_bin, BIN_1)  # time bins for summarystatistics
        self.assertEqual(merger.latest_time_bin, BIN_1)  # time bins for summarystatistics
        
        # Verify upload data structure
        chunk_params, chunk_path, new_contents, content_length, is_new = merger.upload_these[0]
        decompressed = decompress(new_contents)
        
        self.assertTrue(is_new)
        self.assertIn(POWER_STATE, chunk_path)
        self.assertIsNotNone(new_contents)
        self.assertEqual(content_length, len(decompressed))
        
        lines = decompressed.splitlines()
        self.assertEqual(lines[0], POWER_STATE_HEADER_ANDROID)
        self.assertEqual(lines[1], POWERSTATE_OUT_LINE_1)
        self.assertEqual(lines[2], POWERSTATE_OUT_LINE_2)
        self.assertIsNotNone(chunk_params)
        self.assertEqual(chunk_params['data_type'], POWER_STATE)
    
    
    @patch("libs.file_processing.csv_merger.s3_retrieve")
    def test_csv_merger_two_identical_lines_are_merged(self, s3_retrieve: Mock):
        duplicated_row = [T1_BYTESTR, T1_UTC, b'Locked']
        duplicate_rows = [duplicated_row, duplicated_row]  # Same row twice
        
        data_bin: BinifyKey = (
            self.default_study.object_id,
            self.default_participant.patient_id,
            POWER_STATE,
            BIN_1,
            POWER_STATE_HEADER_ANDROID,
        )
        binified_data, null_handler, survey_id_dict = self.basic_config()
        binified_data[data_bin] = (duplicate_rows, [1, 2])
        
        merger = CsvMerger(binified_data, null_handler, survey_id_dict, self.default_participant)
        
        # Verify that the chunk was processed
        self.assertEqual(len(merger.upload_these), 1)
        self.assertIn(1, merger.ftps_to_retire)
        self.assertIn(2, merger.ftps_to_retire)
        
        # Verify upload data structure
        chunk_params, chunk_path, new_contents, content_length, is_new = merger.upload_these[0]
        decompressed = decompress(new_contents)
        
        # Should have header + 2 identical data lines
        lines = decompressed.splitlines()
        self.assertEqual(len(lines), 2)  # header + 1 uniqueified data line
        
        self.assertEqual(lines[0], POWER_STATE_HEADER_ANDROID)
        self.assertEqual(lines[1], b','.join(duplicated_row))
    
    
    @patch("libs.file_processing.csv_merger.s3_retrieve")
    def test_csv_merger_three_identical_lines_are_merged(self, s3_retrieve: Mock):
        duplicated_row = [T1_BYTESTR, b'Locked']
        duplicate_rows = [duplicated_row, duplicated_row, duplicated_row]  # Same row three times
        
        data_bin: BinifyKey = (
            self.default_study.object_id,
            self.default_participant.patient_id,
            POWER_STATE,
            BIN_1,
            POWER_STATE_HEADER_ANDROID,
        )
        
        binified_data, null_handler, survey_id_dict = self.basic_config()
        binified_data[data_bin] = (duplicate_rows, [1, 2, 3])
        
        merger = CsvMerger(binified_data, null_handler, survey_id_dict, self.default_participant)
        
        # Verify that the chunk was processed
        self.assertEqual(len(merger.upload_these), 1)
        self.assertIn(1, merger.ftps_to_retire)
        self.assertIn(2, merger.ftps_to_retire)
        self.assertIn(3, merger.ftps_to_retire)
        
        # Verify upload data structure
        chunk_params, chunk_path, new_contents, content_length, is_new = merger.upload_these[0]
        decompressed = decompress(new_contents)
        
        # Should have header + 3 identical data lines
        lines = decompressed.splitlines()
        self.assertEqual(len(lines), 2)  # header + 1 uniqueified data lines
        self.assertEqual(lines[0], POWER_STATE_HEADER_ANDROID)
        self.assertEqual(lines[1], b','.join(duplicated_row))
    
    
    @patch("libs.file_processing.csv_merger.s3_retrieve")
    def test_csv_merger_two_lines_identical_timestamp_different_values(self, s3_retrieve: Mock):
        # Create test data with same timestamp but different events/values
        row_1 = [T1_BYTESTR, b'Locked']
        row_2 = [T1_BYTESTR, b'Unlocked']  # Different event and level
        rows = [row_1, row_2]
        row_out_1 = b','.join([T1_BYTESTR, T1_UTC, b'Locked'])
        row_out_2 = b','.join([T1_BYTESTR, T1_UTC, b'Unlocked'])
        
        data_bin: BinifyKey = (
            self.default_study.object_id,
            self.default_participant.patient_id,
            POWER_STATE,
            BIN_1,
            POWER_STATE_HEADER_ANDROID,
        )
        binified_data, null_handler, survey_id_dict = self.basic_config()
        binified_data[data_bin] = (rows, [1, 2])
        
        merger = CsvMerger(binified_data, null_handler, survey_id_dict, self.default_participant)
        
        # Verify that the chunk was processed
        self.assertEqual(len(merger.upload_these), 1)
        self.assertIn(1, merger.ftps_to_retire)
        self.assertIn(2, merger.ftps_to_retire)
        
        # Verify upload data structure
        chunk_params, chunk_path, new_contents, content_length, is_new = merger.upload_these[0]
        decompressed = decompress(new_contents)
        
        # Should have header + 2 data lines
        lines = decompressed.splitlines()
        self.assertEqual(len(lines), 3)  # header + 2 data lines
        
        self.assertEqual(lines[0], POWER_STATE_HEADER_ANDROID)
        self.assertEqual(lines[1], row_out_1)
        self.assertEqual(lines[2], row_out_2)
    
    
    @patch("libs.file_processing.csv_merger.s3_retrieve")
    def test_csv_merger_two_lines_exactly_one_time_bin_separate(self, s3_retrieve: Mock):
        binified_data, null_handler, survey_id_dict = self.basic_config()
        
        # Row in first time bin, second is in time bin (one hour later)
        row_1 = [T1_BYTESTR, b'Locked']
        row_2 = [b'1770361745197', b'Unlocked']
        
        data_bin_1: BinifyKey = (
            self.default_study.object_id,
            self.default_participant.patient_id,
            POWER_STATE,
            BIN_1,
            POWER_STATE_HEADER_ANDROID,
        )
        data_bin_2: BinifyKey = (
            self.default_study.object_id,
            self.default_participant.patient_id,
            POWER_STATE,
            BIN_2,
            POWER_STATE_HEADER_ANDROID,
        )
        
        binified_data[data_bin_1] = ([row_1], [1])
        binified_data[data_bin_2] = ([row_2], [2])
        merger = CsvMerger(binified_data, null_handler, survey_id_dict, self.default_participant)
        
        # Verify both chunks were processed as separate chunks
        self.assertEqual(len(merger.upload_these), 2)
        self.assertIn(1, merger.ftps_to_retire)
        self.assertIn(2, merger.ftps_to_retire)
        
        # Verify time bins are exactly one apart
        self.assertEqual(merger.earliest_time_bin, BIN_1)
        self.assertEqual(merger.latest_time_bin, BIN_2)
        self.assertEqual(BIN_2 - BIN_1, 1)
        
        # Each chunk should be a new chunk
        for chunk_params, chunk_path, new_contents, content_length, is_new in merger.upload_these:
            self.assertTrue(is_new)
            self.assertEqual(content_length, len(decompress(new_contents)))
    
    
    @patch("libs.file_processing.csv_merger.s3_retrieve")
    def test_csv_merger_timestamp_exactly_at_time_bin_boundary_are_in_the_bin_that_matches(self, s3_retrieve: Mock):
        binified_data, null_handler, survey_id_dict = self.basic_config()
        
        # Timestamp exactly at BIN_1 and bin2 boundaries: BIN_1
        timestamp_at_bin1_boundary = str(BIN_1 * CHUNK_TIMESLICE_QUANTUM * 1000).encode()
        timestamp_at_bin2_boundary = str(BIN_2 * CHUNK_TIMESLICE_QUANTUM * 1000).encode()
        row_bin1 = [timestamp_at_bin1_boundary, b'Locked']
        row_bin2 = [timestamp_at_bin2_boundary, b'Unlocked']
        
        data_bin_1: BinifyKey = (
            self.default_study.object_id,
            self.default_participant.patient_id,
            POWER_STATE,
            BIN_1,
            POWER_STATE_HEADER_ANDROID,
        )
        data_bin_2: BinifyKey = (
            self.default_study.object_id,
            self.default_participant.patient_id,
            POWER_STATE,
            BIN_2,
            POWER_STATE_HEADER_ANDROID,
        )
        
        binified_data[data_bin_1] = ([row_bin1], [1])
        binified_data[data_bin_2] = ([row_bin2], [2])
        
        merger = CsvMerger(binified_data, null_handler, survey_id_dict, self.default_participant)
        
        # Verify both chunks were processed as separate chunks
        self.assertEqual(len(merger.upload_these), 2)
        self.assertIn(1, merger.ftps_to_retire)
        self.assertIn(2, merger.ftps_to_retire)
        
        # Verify time bins are exactly one apart
        self.assertEqual(merger.earliest_time_bin, BIN_1)
        self.assertEqual(merger.latest_time_bin, BIN_2)
        
        # Verify each chunk is new
        for chunk_params, chunk_path, new_contents, content_length, is_new in merger.upload_these:
            self.assertTrue(is_new)
            self.assertEqual(content_length, len(decompress(new_contents)))
            
            # Verify the timestamps are in the decompressed output
            decompressed = decompress(new_contents)
            lines = decompressed.splitlines()
            self.assertEqual(lines[0], POWER_STATE_HEADER_ANDROID)
            self.assertGreaterEqual(len(lines), 2)  # Header + at least one data line
    
    
    @patch("libs.file_processing.csv_merger.s3_retrieve")
    def test_csv_merger_timestamps_out_of_chronological_order(self, s3_retrieve: Mock):
        
        binified_data, null_handler, survey_id_dict = self.basic_config()
        
        # Create rows with timestamps out of chronological order (all within same bin)
        # Most recent first, then older
        row_newest = [b"1770358450000", b"Unlocked"]  # Most recent
        row_middle = [b"1770358250000", b"Locked"]    # Middle
        row_oldest = [b"1770358145197", b"Locked"]    # Oldest
        rows = [row_newest, row_middle, row_oldest]  # Out of order (reverse chronological)
        
        OUTOFORDER_LINE_NEWEST_OUT = b"1770358450000,2026-02-06T06:14:10.000,Unlocked"
        OUTOFORDER_LINE_MIDDLE_OUT = b"1770358250000,2026-02-06T06:10:50.000,Locked"
        OUTOFORDER_LINE_OLDEST_OUT = b"1770358145197,2026-02-06T06:09:05.197,Locked"
        
        data_bin: BinifyKey = (
            self.default_study.object_id,
            self.default_participant.patient_id,
            POWER_STATE,
            BIN_1,
            POWER_STATE_HEADER_ANDROID,
        )
        binified_data[data_bin] = (rows, [1, 2, 3])
        
        merger = CsvMerger(binified_data, null_handler, survey_id_dict, self.default_participant)
        
        # Verify chunk was processed
        self.assertEqual(len(merger.upload_these), 1)
        self.assertIn(1, merger.ftps_to_retire)
        self.assertIn(2, merger.ftps_to_retire)
        self.assertIn(3, merger.ftps_to_retire)
        
        # Verify upload data
        chunk_params, chunk_path, new_contents, content_length, is_new = merger.upload_these[0]
        decompressed = decompress(new_contents)
        
        self.assertTrue(is_new)
        self.assertEqual(content_length, len(decompressed))
        
        lines = decompressed.splitlines()
        # Should have header + 3 data lines
        self.assertEqual(len(lines), 4)
        self.assertEqual(lines[0], POWER_STATE_HEADER_ANDROID)
        self.assertEqual(lines[1], OUTOFORDER_LINE_OLDEST_OUT)
        self.assertEqual(lines[2], OUTOFORDER_LINE_MIDDLE_OUT)
        self.assertEqual(lines[3], OUTOFORDER_LINE_NEWEST_OUT)
    
    
    @patch("libs.file_processing.csv_merger.s3_retrieve")
    def test_csv_merger_timestamps_out_of_chronological_order_within_different_hours(self, s3_retrieve: Mock):
        """Test that CsvMerger handles out-of-order rows spanning multiple time bins"""
        # Expected output lines for this test
        OUTOFORDER_BIN2_LINE = b"1770361745197,2026-02-06T07:09:05.197,Unlocked"
        OUTOFORDER_BIN1_LINE = b"1770358145197,2026-02-06T06:09:05.197,Locked"
        
        binified_data, null_handler, survey_id_dict = self.basic_config()
        
        # Create rows from BIN_2 first, then BIN_1 (reverse chronological across bins)
        row_bin2_newest = [b'1770361745197', b'Unlocked']  # From BIN_2, most recent
        row_bin1_oldest = [b'1770358145197', b'Locked']    # From BIN_1, oldest
        
        data_bin_1: BinifyKey = (
            self.default_study.object_id,
            self.default_participant.patient_id,
            POWER_STATE,
            BIN_1,
            POWER_STATE_HEADER_ANDROID,
        )
        data_bin_2: BinifyKey = (
            self.default_study.object_id,
            self.default_participant.patient_id,
            POWER_STATE,
            BIN_2,
            POWER_STATE_HEADER_ANDROID,
        )
        
        # Present in reverse order (BIN_2 before BIN_1)
        binified_data[data_bin_2] = ([row_bin2_newest], [2])
        binified_data[data_bin_1] = ([row_bin1_oldest], [1])
        
        merger = CsvMerger(binified_data, null_handler, survey_id_dict, self.default_participant)
        
        # Verify both chunks were processed
        self.assertEqual(len(merger.upload_these), 2)
        self.assertIn(1, merger.ftps_to_retire)
        self.assertIn(2, merger.ftps_to_retire)
        
        # Verify time bins are tracked correctly
        self.assertEqual(merger.earliest_time_bin, BIN_1)
        self.assertEqual(merger.latest_time_bin, BIN_2)
        chunk_params1, chunk_path1, new_contents1, content_length1, is_new1 = merger.upload_these[0]
        chunk_params2, chunk_path2, new_contents2, content_length2, is_new2 = merger.upload_these[1]
        
        # Verify BIN_1 chunk exists and is correct
        bin1_lines = decompress(new_contents1).splitlines()
        self.assertEqual(len(bin1_lines), 2)  # Header + 1 data line
        self.assertEqual(bin1_lines[0], POWER_STATE_HEADER_ANDROID)
        self.assertEqual(bin1_lines[1], OUTOFORDER_BIN1_LINE)
        
        # Verify BIN_2 chunk exists and is correct
        bin2_lines = decompress(new_contents2).splitlines()
        self.assertEqual(len(bin2_lines), 2)  # Header + 1 data line
        self.assertEqual(bin2_lines[0], POWER_STATE_HEADER_ANDROID)
        self.assertEqual(bin2_lines[1], OUTOFORDER_BIN2_LINE)
    
    
    @patch("libs.file_processing.csv_merger.s3_retrieve")
    def test_csv_merger_one_millisecond_before_next_bin(self, s3_retrieve: Mock):
        # Timestamp: just before BIN_2 boundary - should stay in BIN_1
        timestamp_before_boundary = str(BIN_1 * CHUNK_TIMESLICE_QUANTUM * 1000 + 3599999).encode()
        expected_output = b"1767603599999,2026-01-05T08:59:59.999,Locked"
        
        binified_data, null_handler, survey_id_dict = self.basic_config()
        row = [timestamp_before_boundary, b'Locked']
        
        data_bin: BinifyKey = (
            self.default_study.object_id,
            self.default_participant.patient_id,
            POWER_STATE,
            BIN_1,
            POWER_STATE_HEADER_ANDROID,
        )
        binified_data[data_bin] = ([row], [1])
        
        merger = CsvMerger(binified_data, null_handler, survey_id_dict, self.default_participant)
        
        # Verify single chunk was processed
        self.assertEqual(len(merger.upload_these), 1)
        self.assertIn(1, merger.ftps_to_retire)
        self.assertEqual(merger.earliest_time_bin, BIN_1)
        self.assertEqual(merger.latest_time_bin, BIN_1)
        
        # Verify upload data
        chunk_params, chunk_path, new_contents, content_length, is_new = merger.upload_these[0]
        decompressed = decompress(new_contents)
        
        self.assertTrue(is_new)
        self.assertEqual(content_length, len(decompressed))
        
        lines = decompressed.splitlines()
        self.assertEqual(len(lines), 2)  # Header + 1 data line
        self.assertEqual(lines[0], POWER_STATE_HEADER_ANDROID)
        self.assertEqual(lines[1], expected_output)
    
    
    @patch("libs.file_processing.csv_merger.s3_retrieve")
    def test_csv_merger_one_millisecond_after_bin_start(self, s3_retrieve: Mock):
        # Timestamp: just after BIN_1 boundary - should go into BIN_2
        timestamp_after_boundary = str(BIN_1 * CHUNK_TIMESLICE_QUANTUM * 1000 + 3600001).encode()
        expected_line = b"1767603600001,2026-01-05T09:00:00.001,Unlocked"
        
        binified_data, null_handler, survey_id_dict = self.basic_config()
        row = [timestamp_after_boundary, b'Unlocked']
        
        data_bin: BinifyKey = (
            self.default_study.object_id,
            self.default_participant.patient_id,
            POWER_STATE,
            BIN_2,
            POWER_STATE_HEADER_ANDROID,
        )
        binified_data[data_bin] = ([row], [1])
        
        merger = CsvMerger(binified_data, null_handler, survey_id_dict, self.default_participant)
        
        # Verify single chunk was processed
        self.assertEqual(len(merger.upload_these), 1)
        self.assertIn(1, merger.ftps_to_retire)
        self.assertEqual(merger.earliest_time_bin, BIN_2)
        self.assertEqual(merger.latest_time_bin, BIN_2)
        
        # Verify upload data
        chunk_params, chunk_path, new_contents, content_length, is_new = merger.upload_these[0]
        decompressed = decompress(new_contents)
        
        self.assertTrue(is_new)
        self.assertEqual(content_length, len(decompressed))
        
        lines = decompressed.splitlines()
        self.assertEqual(len(lines), 2)  # Header + 1 data line
        self.assertEqual(lines[0], POWER_STATE_HEADER_ANDROID)
        self.assertEqual(lines[1], expected_line)
    
    
    @patch("libs.file_processing.csv_merger.s3_retrieve")
    def test_csv_merger_separate_time_bins(self, s3_retrieve: Mock):
        binified_data, null_handler, survey_id_dict = self.basic_config()
        data_bin_1: BinifyKey = (
            self.default_study.object_id,
            self.default_participant.patient_id,
            POWER_STATE,
            BIN_1,
            POWER_STATE_HEADER_ANDROID,
        )
        data_bin_2: BinifyKey = (
            self.default_study.object_id,
            self.default_participant.patient_id,
            POWER_STATE,
            BIN_2,
            POWER_STATE_HEADER_ANDROID,
        )
        binified_data[data_bin_1] = (POWER_STATE_ROWS_1, [1])
        binified_data[data_bin_2] = (POWER_STATE_ROWS_2, [2])
        merger = CsvMerger(binified_data, null_handler, survey_id_dict, self.default_participant)
        
        # Verify both chunks were processed
        self.assertEqual(len(merger.upload_these), 2)
        self.assertEqual(merger.earliest_time_bin, BIN_1)
        self.assertEqual(merger.latest_time_bin, BIN_2)
        
        # Verify all FTPs were processed
        self.assertIn(1, merger.ftps_to_retire)
        self.assertIn(2, merger.ftps_to_retire)
    
    
    @patch("libs.file_processing.csv_merger.s3_retrieve")
    def test_csv_merger_chunk_exists_case(self, s3_retrieve: Mock):
        binified_data, null_handler, survey_id_dict = self.basic_config()
        
        # Create an existing chunk in the database
        chunk_path = construct_s3_chunk_path(
            self.default_study.object_id,
            self.default_participant.patient_id,
            POWER_STATE,
            BIN_1,
        )
        
        # Create the chunk registry entry
        # Convert time_bin to datetime as required by the model
        time_bin_datetime = datetime.fromtimestamp(BIN_1 * CHUNK_TIMESLICE_QUANTUM, UTC)
        chunk_registry = ChunkRegistry.objects.create(
            study_id=self.default_participant.study_id,
            participant_id=self.default_participant.id,
            data_type=POWER_STATE,
            chunk_path=chunk_path,
            chunk_hash="old_hash",
            time_bin=time_bin_datetime,
            file_size=1000,
            is_chunkable=True,
        )
        
        # Mock S3 to return existing data
        existing_data = construct_csv_as_bytes(POWER_STATE_HEADER_ANDROID, POWER_STATE_ROWS_1)
        s3_retrieve.return_value = existing_data
        
        # Set up binified data - use header with UTC time column since that's what convert_unix_to_human_readable_timestamps returns
        data_bin: BinifyKey = (
            self.default_study.object_id,
            self.default_participant.patient_id,
            POWER_STATE,
            BIN_1,
            POWER_STATE_HEADER_ANDROID,
        )
        binified_data[data_bin] = (POWER_STATE_ROWS_2, [1, 2])
        
        merger = CsvMerger(binified_data, null_handler, survey_id_dict, self.default_participant)
        
        # Verify that the chunk was merged (not created new)
        self.assertEqual(len(merger.upload_these), 1)
        chunk_params, returned_chunk_path, new_contents, content_length, is_new = merger.upload_these[0]
        
        # is_new should be False because we're updating an existing chunk
        self.assertFalse(is_new)
        self.assertEqual(returned_chunk_path, chunk_path)
        
        # Verify FTPs were processed
        succeeded, failed, _, _ = merger.get_retirees()
        self.assertEqual(succeeded, {1, 2})
        self.assertEqual(failed, set())

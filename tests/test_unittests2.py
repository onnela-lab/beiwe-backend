import hashlib
from datetime import datetime
from io import BytesIO
from unittest.mock import _Call, MagicMock, Mock, patch

from django.utils import timezone

from constants.common_constants import API_TIME_FORMAT, CHUNKS_FOLDER, UTC
from constants.data_stream_constants import AUDIO_RECORDING, SURVEY_ANSWERS, SURVEY_TIMINGS
from constants.message_strings import (ERR_ANDROID_REFERENCE_VERSION_CODE_DIGITS,
    ERR_ANDROID_TARGET_VERSION_DIGITS, ERR_IOS_REFERENCE_VERSION_NAME_FORMAT,
    ERR_IOS_TARGET_VERSION_FORMAT, ERR_IOS_VERSION_COMPONENTS_DIGITS,
    ERR_TARGET_VERSION_CANNOT_BE_MISSING, ERR_TARGET_VERSION_MUST_BE_STRING, ERR_UNKNOWN_OS_TYPE)
from constants.s3_constants import (COMPRESSED_DATA_MISSING_AT_UPLOAD,
    COMPRESSED_DATA_MISSING_ON_POP, COMPRESSED_DATA_PRESENT_AT_COMPRESSION,
    COMPRESSED_DATA_PRESENT_ON_ASSIGNMENT, COMPRESSED_DATA_PRESENT_ON_DOWNLOAD,
    UNCOMPRESSED_DATA_MISSING_AT_COMPRESSION, UNCOMPRESSED_DATA_MISSING_ON_POP,
    UNCOMPRESSED_DATA_PRESENT_ON_ASSIGNMENT, UNCOMPRESSED_DATA_PRESENT_ON_DOWNLOAD,
    UNCOMPRESSED_DATA_PRESENT_WRONG_AT_UPLOAD)
from constants.user_constants import ANDROID_API, IOS_API
from database.models import ForestVersion, S3File
from libs.aes import encrypt_for_server
from libs.celery_control import DebugCeleryApp
from libs.s3 import BadS3PathException, decrypt_server, NoSuchKeyException, S3Storage
from libs.utils.file_name_utils import determine_base_file_name
from libs.utils.compression import compress
from libs.utils.forest_utils import get_forest_git_hash
from libs.utils.participant_app_version_comparison import (is_this_version_gt_participants,
    is_this_version_gte_participants, is_this_version_lt_participants,
    is_this_version_lte_participants)
from scripts.update_forest_version import main as update_forest_version_main
from tests.common import CommonTestCase


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
    
    UNCOMPRESSED_SLUG = b"content"
    COMPRESSED_SLUG = compress(UNCOMPRESSED_SLUG)
    SHA1_SLUG = hashlib.sha1(UNCOMPRESSED_SLUG).digest()
    ENCRYPTED_SLUG = encrypt_for_server(UNCOMPRESSED_SLUG, CommonTestCase.DEFAULT_ENCRYPTION_KEY_BYTES)
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
        
        # with self.assertRaises(IOSDataRecoveryDisabledException):  # old error, make sure it doesn't raise
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
        s.set_file_content_precompressed(self.COMPRESSED_SLUG, len(self.UNCOMPRESSED_SLUG), self.SHA1_SLUG)
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
        s.compress_and_push_to_storage_then_clear_memory()
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
        s.compress_and_push_to_storage_then_clear_memory()
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
            s.compress_and_push_to_storage_then_clear_memory,
        )
        s.set_file_content_precompressed(self.COMPRESSED_SLUG, len(self.UNCOMPRESSED_SLUG), self.SHA1_SLUG)
        self.assertRaisesRegex(
            AssertionError,
            UNCOMPRESSED_DATA_MISSING_AT_COMPRESSION,
            s.compress_and_push_to_storage_then_clear_memory,
        )
    
    def test_precompressed_upload__compress_and_push_to_storage_and_clear_memory(self):
        s = self.default_s3storage_with_prefix
        s.set_file_content_precompressed(self.COMPRESSED_SLUG, len(self.UNCOMPRESSED_SLUG), self.SHA1_SLUG)
        self.assertFalse(S3File.objects.exists())
        self.assertRaisesRegex(
            AssertionError,
            UNCOMPRESSED_DATA_MISSING_AT_COMPRESSION,
            s.compress_and_push_to_storage_then_clear_memory,
        )
        self.assertRaisesRegex(
            AssertionError,
            COMPRESSED_DATA_PRESENT_ON_ASSIGNMENT,
            s.set_file_content_uncompressed,
            self.COMPRESSED_SLUG
        )
        s.pop_compressed_file_content()
        s.set_file_content_uncompressed(self.COMPRESSED_SLUG)
        s.compress_and_push_to_storage_then_clear_memory()
    
    def test_COMPRESSED_DATA_PRESENT_AT_COMPRESSION(self):
        s = self.default_s3storage_with_prefix
        s.set_file_content_precompressed(self.COMPRESSED_SLUG, len(self.UNCOMPRESSED_SLUG), self.SHA1_SLUG)
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
            s.push_to_storage_precompressed_and_clear_memory,
        )
    
    def test_UNCOMPRESSED_DATA_PRESENT_WRONG_AT_UPLOAD(self):
        s = self.default_s3storage_with_prefix
        s.set_file_content_precompressed(self.COMPRESSED_SLUG, len(self.UNCOMPRESSED_SLUG), self.SHA1_SLUG)
        s.uncompressed_data = b"fake, illegal"
        self.assertRaisesRegex(
            AssertionError,
            UNCOMPRESSED_DATA_PRESENT_WRONG_AT_UPLOAD.replace("(", r"\(").replace(")", r"\)"),  # has
            s.push_to_storage_precompressed_and_clear_memory,
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
        s.set_file_content_precompressed(self.COMPRESSED_SLUG, len(self.UNCOMPRESSED_SLUG), self.SHA1_SLUG)
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
            s.set_file_content_precompressed,
            self.COMPRESSED_SLUG,
            len(self.UNCOMPRESSED_SLUG),
            self.SHA1_SLUG
        )
    
    @patch("libs.s3.conn")
    def test_push_to_storage_precompressed(self, conn=MagicMock()):
        s = self.default_s3storage_with_prefix
        s.set_file_content_precompressed(self.COMPRESSED_SLUG, len(self.UNCOMPRESSED_SLUG), self.SHA1_SLUG)
        self.assertFalse(S3File.objects.exists())
        s.push_to_storage_precompressed_and_clear_memory()
        
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
        
        self.assertFalse(S3File.objects.exists())
    
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
        
        self.assertFalse(S3File.objects.exists())
    
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
        self.assertIsNotNone(s3_file.sha1)
        self.assertEqual(len(s3_file.sha1), 20)
    
    def assert_correct_uploaded_s3file(self, s3_file: S3File):
        self.assertIsNotNone(s3_file.path)
        self.assertEqual(s3_file.size_uncompressed, len(b"content"))
        self.assertEqual(s3_file.size_compressed, len(self.COMPRESSED_SLUG))
        self.assertEqual(s3_file.study, self.default_study)
        self.assertEqual(s3_file.participant, self.default_participant)
        self.assertEqual(s3_file.sha1, hashlib.sha1(b"content").digest())
    
    def assert_correct_uploaded_s3file_already_compressed(self, s3_file: S3File):
        # participant_id, study_id, size_compressed, encryption_time_ns, upload_time_ns, last_updated
        self.assertIsNotNone(s3_file.path)
        self.assertIsNotNone(s3_file.participant_id)
        self.assertIsNotNone(s3_file.study_id)
        self.assertIsNotNone(s3_file.size_compressed)
        self.assertIsNotNone(s3_file.last_updated)
        self.assertEqual(s3_file.size_compressed, len(self.COMPRESSED_SLUG))
        self.assertEqual(s3_file.study, self.default_study)
        self.assertIsNotNone(s3_file.sha1)
        self.assertEqual(len(s3_file.sha1), 20)
        self.assertEqual(s3_file.size_uncompressed, len(b"content"))


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
            data_type=AUDIO_RECORDING,
        )
        self.assertEqual(
            determine_base_file_name(d),
            "steve/audio_recordings/thesurveyobjectidvalue/2018-04-27 19_39_48.384000+00_00.wav"
        )
    
    def test_survey_id_present_in_audio_survey_with_param_as_mp4(self):
        d = self.updated_dict(
            chunk_path="5873fe38644ad7557b168e43/steve/voiceRecording/123456789012345678901234/1524857988384.mp4",
            survey__object_id="thesurveyobjectidvalue",
            data_type=AUDIO_RECORDING,
        )
        self.assertEqual(
            determine_base_file_name(d),
            "steve/audio_recordings/thesurveyobjectidvalue/2018-04-27 19_39_48.384000+00_00.mp4"
        )
    
    def test_survey_id_present_in_audio_survey_without_param(self):
        d = self.updated_dict(
            chunk_path="5873fe38644ad7557b168e43/steve/voiceRecording/123456789012345678901234/1524857988384.wav",
            data_type=AUDIO_RECORDING,
        )
        self.assertEqual(
            determine_base_file_name(d),
            "steve/audio_recordings/123456789012345678901234/2018-04-27 19_39_48.384000+00_00.wav"
        )
    
    def test_survey_id_present_in_without_param_or_file_path(self):
        d = self.updated_dict(
            chunk_path="5873fe38644ad7557b168e43/steve/voiceRecording/1524857988384.wav",
            data_type=AUDIO_RECORDING,
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


class TestUpdateForestVersion(CommonTestCase):
    
    def test_update_forest_version(self):
        a = ForestVersion.singleton()
        self.assertEqual(a.package_version, "")
        self.assertEqual(a.git_commit, "")
        del a
        update_forest_version_main()
        
        b = ForestVersion.singleton()
        self.assertNotEqual(b.package_version, "", "Package version should be updated!")
        self.assertNotEqual(b.git_commit, "", "Git commit should be updated!")
    
    def test_get_forest_git_hash_gets_anything_at_all(self):
        hash = get_forest_git_hash()
        self.assertNotEqual(hash, "")

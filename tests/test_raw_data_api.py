import json
from unittest.mock import MagicMock, patch

from django.http.response import FileResponse

from constants.data_stream_constants import ALL_DATA_STREAMS, SURVEY_TIMINGS
from constants.testing_constants import EMPTY_ZIP, SIMPLE_FILE_CONTENTS
from constants.user_constants import ResearcherRole
from database.data_access_models import ChunkRegistry
from database.system_models import DataAccessRecord
from tests.common import CommonTestCase, DataApiTest
from tests.helpers import DummyThreadPool


#
## data_access_api
#

class TestGetData(DataApiTest):
    """ WARNING: there are heisenbugs in debugging the download data api endpoint.

    There is a generator that is conditionally present (`handle_database_query`), it can swallow
    errors. As a generater iterating over it consumes it, so printing it breaks the code.
    
    You Must Patch libs.streaming_zip.ThreadPool
        The database connection breaks throwing errors on queries that should succeed.
        The iterator inside the zip file generator generally fails, and the zip file is empty.

    You Must Patch libs.streaming_zip.s3_retrieve
        Otherwise s3_retrieve will fail due to the patch is tests.common.
    """
    
    ENDPOINT_NAME = "raw_data_api.get_data"
    REGISTRY_HASH = "registry_hash"
    
    # retain and usethis structure in order to force a test addition on a new file type.
    # "particip" is the DEFAULT_PARTICIPANT_NAME
    # 'u1Z3SH7l2xNsw72hN3LnYi96' is the  DEFAULT_SURVEY_OBJECT_ID
    PATIENT_NAME = CommonTestCase.DEFAULT_PARTICIPANT_NAME
    FILE_NAMES = {                                        # ↓ that Z makes it a timzone'd datetime
        "accelerometer": ("something.csv", "2020-10-05 02:00Z",
                         f"{PATIENT_NAME}/accelerometer/2020-10-05 02_00_00+00_00.csv"),
        "ambient_audio": ("something.mp4", "2020-10-05 02:00Z",
                         f"{PATIENT_NAME}/ambient_audio/2020-10-05 02_00_00+00_00.mp4"),
        "app_log": ("app_log.csv", "2020-10-05 02:00Z",
                         f"{PATIENT_NAME}/app_log/2020-10-05 02_00_00+00_00.csv"),
        "bluetooth": ("bluetooth.csv", "2020-10-05 02:00Z",
                         f"{PATIENT_NAME}/bluetooth/2020-10-05 02_00_00+00_00.csv"),
        "calls": ("calls.csv", "2020-10-05 02:00Z",
                         f"{PATIENT_NAME}/calls/2020-10-05 02_00_00+00_00.csv"),
        "devicemotion": ("devicemotion.csv", "2020-10-05 02:00Z",
                         f"{PATIENT_NAME}/devicemotion/2020-10-05 02_00_00+00_00.csv"),
        "gps": ("gps.csv", "2020-10-05 02:00Z",
                         f"{PATIENT_NAME}/gps/2020-10-05 02_00_00+00_00.csv"),
        "gyro": ("gyro.csv", "2020-10-05 02:00Z",
                         f"{PATIENT_NAME}/gyro/2020-10-05 02_00_00+00_00.csv"),
        "identifiers": ("identifiers.csv", "2020-10-05 02:00Z",
                         f"{PATIENT_NAME}/identifiers/2020-10-05 02_00_00+00_00.csv"),
        "ios_log": ("ios_log.csv", "2020-10-05 02:00Z",
                         f"{PATIENT_NAME}/ios_log/2020-10-05 02_00_00+00_00.csv"),
        "magnetometer": ("magnetometer.csv", "2020-10-05 02:00Z",
                         f"{PATIENT_NAME}/magnetometer/2020-10-05 02_00_00+00_00.csv"),
        "power_state": ("power_state.csv", "2020-10-05 02:00Z",
                         f"{PATIENT_NAME}/power_state/2020-10-05 02_00_00+00_00.csv"),
        "proximity": ("proximity.csv", "2020-10-05 02:00Z",
                         f"{PATIENT_NAME}/proximity/2020-10-05 02_00_00+00_00.csv"),
        "reachability": ("reachability.csv", "2020-10-05 02:00Z",
                         f"{PATIENT_NAME}/reachability/2020-10-05 02_00_00+00_00.csv"),
        "survey_answers": ("survey_obj_id/something2/something3.csv", "2020-10-05 02:00Z",
                          # expecting: patient_id/data_type/survey_id/time.csv
                         f"{PATIENT_NAME}/survey_answers/something2/2020-10-05 02_00_00+00_00.csv"),
        "survey_timings": ("something1/something2/something3/something4/something5.csv", "2020-10-05 02:00Z",
                          # expecting: patient_id/data_type/survey_id/time.csv
                          f"{PATIENT_NAME}/survey_timings/u1Z3SH7l2xNsw72hN3LnYi96/2020-10-05 02_00_00+00_00.csv"),
        "texts": ("texts.csv", "2020-10-05 02:00Z",
                         f"{PATIENT_NAME}/texts/2020-10-05 02_00_00+00_00.csv"),
        "audio_recordings": ("audio_recordings.wav", "2020-10-05 02:00Z",
                         f"{PATIENT_NAME}/audio_recordings/2020-10-05 02_00_00+00_00.wav"),
        "wifi": ("wifi.csv", "2020-10-05 02:00Z",
                         f"{PATIENT_NAME}/wifi/2020-10-05 02_00_00+00_00.csv"),
        }
    
    # setting the threadpool needs to apply to each test, following this pattern because its easy.
    @patch("libs.streaming_zip.ThreadPool")
    def test_basics(self, threadpool: MagicMock):
        threadpool.return_value = DummyThreadPool()
        self._test_basics(as_site_admin=False)
    
    @patch("libs.streaming_zip.ThreadPool")
    def test_basics_as_site_admin(self, threadpool: MagicMock):
        threadpool.return_value = DummyThreadPool()
        self._test_basics(as_site_admin=True)
    
    @patch("libs.streaming_zip.ThreadPool")
    def test_downloads_and_file_naming(self, threadpool: MagicMock):
        threadpool.return_value = DummyThreadPool()
        self._test_downloads_and_file_naming()
    
    @patch("libs.streaming_zip.ThreadPool")
    def test_registry_doesnt_download(self, threadpool: MagicMock):
        threadpool.return_value = DummyThreadPool()
        self._test_registry_doesnt_download()
    
    @patch("libs.streaming_zip.ThreadPool")
    def test_time_bin(self, threadpool: MagicMock):
        threadpool.return_value = DummyThreadPool()
        self._test_time_bin()
    
    @patch("libs.streaming_zip.ThreadPool")
    def test_user_query(self, threadpool: MagicMock):
        threadpool.return_value = DummyThreadPool()
        self._test_user_query()
    
    @patch("libs.streaming_zip.ThreadPool")
    def test_data_streams(self, threadpool: MagicMock):
        threadpool.return_value = DummyThreadPool()
        self._test_data_streams()
    
    # but don't patch ThreadPool for this one
    def test_downloads_and_file_naming_heisenbug(self):
        # As far as I can tell the ThreadPool seems to screw up the connection to the test
        # database, and queries on the non-main thread either find no data or connect to the wrong
        # database (presumably your normal database?).
        # Please retain this behavior and consult me (Eli, Biblicabeebli) during review.  This means a
        # change has occurred to the multithreading, and is probably related to an obscure but known
        # memory leak in the data access api download enpoint that is relevant on large downloads. """
        try:
            self._test_downloads_and_file_naming()
        except AssertionError as e:
            # this will happen on the first file it tests, accelerometer.
            literal_string_of_error_message = f"b'{self.PATIENT_NAME}/accelerometer/2020-10-05 " \
                "02_00_00+00_00.csv' not found in b'PK\\x05\\x06\\x00\\x00\\x00\\x00\\x00" \
                "\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00'"
            
            if str(e) != literal_string_of_error_message:
                raise Exception(
                    f"\n'{literal_string_of_error_message}'\nwas not equal to\n'{str(e)}'\n"
                    "\n  You have changed something that is possibly related to "
                    "threading via a ThreadPool or DummyThreadPool"
                )
    
    def _test_basics(self, as_site_admin: bool):
        if as_site_admin:
            self.session_researcher.update(site_admin=True)
        else:
            self.set_session_study_relation(ResearcherRole.researcher)
        resp: FileResponse = self.smart_post(study_pk=self.session_study.id, web_form="anything")
        self.assertEqual(resp.status_code, 200)
        for i, file_bytes in enumerate(resp.streaming_content, start=1):
            pass
        self.assertEqual(i, 1)
        # this is an empty zip file as output by the api.  PK\x05\x06 is zip-speak for an empty
        # container.  Behavior can vary on how zip decompressors handle an empty zip, some fail.
        self.assertEqual(file_bytes, EMPTY_ZIP)
        
        # test without web_form, which will create the registry file (which is empty)
        resp2: FileResponse = self.smart_post(study_pk=self.session_study.id)
        self.assertEqual(resp2.status_code, 200)
        file_content = b""
        for i2, file_bytes2 in enumerate(resp2.streaming_content, start=1):
            file_content = file_content + file_bytes2
        self.assertEqual(i2, 2)
        self.assert_present(b"registry{}", file_content)
    
    @patch("libs.streaming_zip.s3_retrieve")
    def _test_downloads_and_file_naming(self, s3_retrieve: MagicMock):
        # basics
        s3_retrieve.return_value = SIMPLE_FILE_CONTENTS
        self.set_session_study_relation(ResearcherRole.researcher)
        
        # need to test all data types
        for data_type in ALL_DATA_STREAMS:
            path, time_bin, output_name = self.FILE_NAMES[data_type]
            file_contents = self.generate_chunkregistry_and_download(data_type, path, time_bin)
            # this is an 'in' test because the file name is part of the zip file, as cleartext
            self.assertIn(output_name.encode(), file_contents)
            self.assertIn(s3_retrieve.return_value, file_contents)
    
    @patch("libs.streaming_zip.s3_retrieve")
    def _test_data_streams(self, s3_retrieve: MagicMock):
        # basics
        s3_retrieve.return_value = SIMPLE_FILE_CONTENTS
        self.set_session_study_relation(ResearcherRole.researcher)
        file_path = "some_file_path.csv"
        basic_args = ("accelerometer", file_path, "2020-10-05 02:00Z")
        
        # assert normal args actually work
        file_contents = self.generate_chunkregistry_and_download(*basic_args)
        self.assertNotEqual(file_contents, EMPTY_ZIP)
        
        # test matching data type downloads
        file_contents = self.generate_chunkregistry_and_download(
            *basic_args, query_data_streams='["accelerometer"]'
        )
        self.assertNotEqual(file_contents, EMPTY_ZIP)
        # same with only the string (no brackets, client.post handles serialization)
        file_contents = self.generate_chunkregistry_and_download(
            *basic_args, query_data_streams="accelerometer"
        )
        self.assertNotEqual(file_contents, EMPTY_ZIP)
        
        # test invalid data stream
        file_contents = self.generate_chunkregistry_and_download(
            *basic_args, query_data_streams='"[accelerometer,gyro]', status_code=404
        )
        
        # test valid, non-matching data type does not download
        file_contents = self.generate_chunkregistry_and_download(
            *basic_args, query_data_streams='["gyro"]'
        )
        self.assertEqual(file_contents, EMPTY_ZIP)
    
    @patch("libs.streaming_zip.s3_retrieve")
    def _test_registry_doesnt_download(self, s3_retrieve: MagicMock):
        # basics
        s3_retrieve.return_value = SIMPLE_FILE_CONTENTS
        self.set_session_study_relation(ResearcherRole.researcher)
        file_path = "some_file_path.csv"
        basic_args = ("accelerometer", file_path, "2020-10-05 02:00Z")
        
        # assert normal args actually work
        file_contents = self.generate_chunkregistry_and_download(*basic_args)
        self.assertNotEqual(file_contents, EMPTY_ZIP)
        
        # test that file is not downloaded when a valid json registry is present
        # (the test for the empty zip is much, easiest, even if this combination of parameters
        # is technically not kosher.)
        file_contents = self.generate_chunkregistry_and_download(
            *basic_args, registry=json.dumps({file_path: self.REGISTRY_HASH}), force_web_form=True
        )
        self.assertEqual(file_contents, EMPTY_ZIP)
        
        # test that a non-matching hash does not block download.
        file_contents = self.generate_chunkregistry_and_download(
            *basic_args, registry=json.dumps({file_path: "bad hash value"})
        )
        self.assertNotEqual(file_contents, EMPTY_ZIP)
        
        # test bad json objects
        self.generate_chunkregistry_and_download(
            *basic_args, registry=json.dumps([self.REGISTRY_HASH]), status_code=400
        )
        self.generate_chunkregistry_and_download(
            *basic_args, registry=json.dumps([file_path]), status_code=400
        )
        # empty string is probably worth testing
        file_contents = self.generate_chunkregistry_and_download(
            *basic_args, registry="", status_code=400
        )
    
    @patch("libs.streaming_zip.s3_retrieve")
    def _test_time_bin(self, s3_retrieve: MagicMock):
        # basics
        s3_retrieve.return_value = SIMPLE_FILE_CONTENTS
        self.set_session_study_relation(ResearcherRole.researcher)
        basic_args = ("accelerometer", "some_file_path.csv", "2020-10-05 02:00Z")
        
        # generic request should succeed
        file_contents = self.generate_chunkregistry_and_download(*basic_args)
        self.assertNotEqual(file_contents, EMPTY_ZIP)
        self.assertIn(SIMPLE_FILE_CONTENTS, file_contents)
        
        # the api time parameter format is "%Y-%m-%dT%H:%M:%S"
        # from a time before time_bin of chunkregistry
        file_contents = self.generate_chunkregistry_and_download(
            *basic_args, query_time_bin_start="2020-10-05T01:00:00",
        )
        self.assertNotEqual(file_contents, EMPTY_ZIP)
        self.assertIn(SIMPLE_FILE_CONTENTS, file_contents)
        
        # inner check should be equal to or after the given date
        file_contents = self.generate_chunkregistry_and_download(
            *basic_args, query_time_bin_start="2020-10-05T02:00:00",
        )
        self.assertNotEqual(file_contents, EMPTY_ZIP)
        self.assertIn(SIMPLE_FILE_CONTENTS, file_contents)
        
        # inner check should be equal to or before the given date
        file_contents = self.generate_chunkregistry_and_download(
            *basic_args, query_time_bin_end="2020-10-05T02:00:00",
        )
        self.assertNotEqual(file_contents, EMPTY_ZIP)
        self.assertIn(SIMPLE_FILE_CONTENTS, file_contents)
        
        # this should fail, start date is late
        file_contents = self.generate_chunkregistry_and_download(
            *basic_args, query_time_bin_start="2020-10-05T03:00:00",
        )
        self.assertEqual(file_contents, EMPTY_ZIP)
        
        # this should succeed, end date is after start date
        file_contents = self.generate_chunkregistry_and_download(
            *basic_args, query_time_bin_end="2020-10-05T03:00:00",
        )
        self.assertNotEqual(file_contents, EMPTY_ZIP)
        self.assertIn(SIMPLE_FILE_CONTENTS, file_contents)
        
        # should succeed, within time range
        file_contents = self.generate_chunkregistry_and_download(
            *basic_args,
            query_time_bin_start="2020-10-05T02:00:00",
            query_time_bin_end="2020-10-05T03:00:00",
        )
        self.assertNotEqual(file_contents, EMPTY_ZIP)
        self.assertIn(SIMPLE_FILE_CONTENTS, file_contents)
        
        # test with bad time bins, returns no data, user error, no special case handling
        file_contents = self.generate_chunkregistry_and_download(
            *basic_args,
            query_time_bin_start="2020-10-05T03:00:00",
            query_time_bin_end="2020-10-05T02:00:00",
        )
        self.assertEqual(file_contents, EMPTY_ZIP)
        
        # test inclusive
        file_contents = self.generate_chunkregistry_and_download(
            *basic_args,
            query_time_bin_start="2020-10-05T02:00:00",
            query_time_bin_end="2020-10-05T02:00:00",
        )
        self.assertNotEqual(file_contents, EMPTY_ZIP)
        self.assertIn(SIMPLE_FILE_CONTENTS, file_contents)
        
        # test bad time format
        self.generate_chunkregistry_and_download(
            *basic_args, query_time_bin_start="2020-10-05 01:00:00", status_code=400
        )
    
    @patch("libs.streaming_zip.s3_retrieve")
    def _test_user_query(self, s3_retrieve: MagicMock):
        # basics
        s3_retrieve.return_value = SIMPLE_FILE_CONTENTS
        self.set_session_study_relation(ResearcherRole.researcher)
        basic_args = ("accelerometer", "some_file_path.csv", "2020-10-05 02:00Z")
        
        # generic request should succeed
        file_contents = self.generate_chunkregistry_and_download(*basic_args)
        self.assertNotEqual(file_contents, EMPTY_ZIP)
        self.assertIn(SIMPLE_FILE_CONTENTS, file_contents)
        
        # Test bad username
        output_status_code = self.generate_chunkregistry_and_download(
            *basic_args, query_patient_ids='["jeff"]', status_code=404
        )
        self.assertEqual(output_status_code, 404)  # redundant, whatever
        
        # test working participant filter
        file_contents = self.generate_chunkregistry_and_download(
            *basic_args, query_patient_ids=[self.default_participant.patient_id],
        )
        self.assertNotEqual(file_contents, EMPTY_ZIP)
        self.assertIn(SIMPLE_FILE_CONTENTS, file_contents)
        # same but just the string
        file_contents = self.generate_chunkregistry_and_download(
            *basic_args, query_patient_ids=self.default_participant.patient_id,
        )
        self.assertNotEqual(file_contents, EMPTY_ZIP)
        self.assertIn(SIMPLE_FILE_CONTENTS, file_contents)
        
        # test empty patients doesn't do anything
        file_contents = self.generate_chunkregistry_and_download(
            *basic_args, query_patient_ids='[]',
        )
        self.assertNotEqual(file_contents, EMPTY_ZIP)
        self.assertIn(SIMPLE_FILE_CONTENTS, file_contents)
        
        # test no matching data. create user, query for that user
        self.generate_participant(self.session_study, "jeff")
        file_contents = self.generate_chunkregistry_and_download(
            *basic_args, query_patient_ids='["jeff"]',
        )
        self.assertEqual(file_contents, EMPTY_ZIP)
    
    def generate_chunkregistry_and_download(
        self,
        data_type: str,
        file_path: str,
        time_bin: str,
        status_code: int = 200,
        registry: bool = None,
        query_time_bin_start: str = None,
        query_time_bin_end: str = None,
        query_patient_ids: str = None,
        query_data_streams: str = None,
        force_web_form: bool = False,
    ):
        post_kwargs = {"study_pk": self.session_study.id}
        generate_kwargs = {"time_bin": time_bin, "path": file_path}
        tracking = {"researcher": self.session_researcher, "query_params": {}}
        
        if data_type == SURVEY_TIMINGS:
            generate_kwargs["survey"] = self.default_survey
        
        if registry is not None:
            post_kwargs["registry"] = registry
            generate_kwargs["hash_value"] = self.REGISTRY_HASH  # strings must match
            tracking["registry_dict_size"] = True
        else:
            post_kwargs["web_form"] = ""
        
        if force_web_form:
            post_kwargs["web_form"] = ""
        
        if query_data_streams is not None:
            post_kwargs["data_streams"] = query_data_streams
            tracking["query_params"]["data_streams"] = query_data_streams
        
        if query_patient_ids is not None:
            post_kwargs["user_ids"] = query_patient_ids
            tracking["user_ids"] = query_patient_ids
        
        if query_time_bin_start:
            post_kwargs['time_start'] = query_time_bin_start
            tracking['time_start'] = query_time_bin_start
        if query_time_bin_end:
            post_kwargs['time_end'] = query_time_bin_end
            tracking['time_end'] = query_time_bin_end
        
        # clear records, create chunkregistry and post
        DataAccessRecord.objects.all().delete()  # we automate tihs testing, easiest to clear it
        self.generate_chunkregistry(
            self.session_study, self.default_participant, data_type, **generate_kwargs
        )
        resp: FileResponse = self.smart_post(**post_kwargs)
        
        # some basics for testing that DataAccessRecords are created
        assert DataAccessRecord.objects.count() == 1, (post_kwargs, resp.status_code, DataAccessRecord.objects.count())
        record = DataAccessRecord.objects.order_by("-created_on").first()
        self.assertEqual(record.researcher.id, self.session_researcher.id)
        
        # Test for a status code, default 200
        self.assertEqual(resp.status_code, status_code)
        if resp.status_code != 200:
            # no iteration, clear db
            ChunkRegistry.objects.all().delete()
            return resp.status_code
        
        # directly comparing these dictionaries is quite non-trivial, not really worth testing tbh?
        # post_kwargs.pop("web_form")
        # self.assertEqual(json.loads(record.query_params), post_kwargs)
        
        # then iterate over the streaming output and concatenate it.
        bytes_list = []
        for i, file_bytes in enumerate(resp.streaming_content, start=1):
            bytes_list.append(file_bytes)
            # print(data_type, i, file_bytes)
        
        # database cleanup has to be after the iteration over the file contents
        ChunkRegistry.objects.all().delete()
        return b"".join(bytes_list)

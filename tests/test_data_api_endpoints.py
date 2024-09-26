# trunk-ignore-all(ruff/B018)
# trunk-ignore-all(bandit/B105)
from datetime import date, datetime, timedelta

import orjson
import zstd
from dateutil.tz import UTC
from django.core.exceptions import ValidationError
from django.http import StreamingHttpResponse
from django.utils import timezone

from authentication.tableau_authentication import (check_tableau_permissions,
    TableauAuthenticationFailed, TableauPermissionDenied, X_ACCESS_KEY_ID, X_ACCESS_KEY_SECRET)
from constants.forest_constants import DATA_QUANTITY_FIELD_NAMES, SERIALIZABLE_FIELD_NAMES
from constants.message_strings import MISSING_JSON_CSV_MESSAGE
from constants.user_constants import ResearcherRole
from database.profiling_models import UploadTracking
from database.security_models import ApiKey
from database.study_models import Study
from database.survey_models import Survey, SurveyArchive
from database.user_models_participant import AppHeartbeats, AppVersionHistory
from database.user_models_researcher import StudyRelation
from tests.common import DataApiTest, SmartRequestsTestCase, TableauAPITest
from tests.helpers import compare_dictionaries, ParticipantTableHelperMixin


#
## Data Apis
#

class TestApiCredentialCheck(DataApiTest):
    ENDPOINT_NAME = "data_api_endpoints.get_studies"
    
    def test_missing_all_parameters(self):
        # use _smart_post
        resp = self.less_smart_post()
        # 400, missing parameter
        self.assertEqual(400, resp.status_code)
    
    def test_only_secret_key(self):
        resp = self.less_smart_post(secret_key=self.session_secret_key)
        # 400, missing parameter
        self.assertEqual(400, resp.status_code)
    
    def test_only_access_key(self):
        resp = self.less_smart_post(access_key=self.session_access_key)
        # 400, missing parameter
        self.assertEqual(400, resp.status_code)
    
    def test_regex_validation(self):
        # asserts that the regex validation is working on the secret key
        self.API_KEY.access_key_secret = "apples"
        self.assertRaises(ValidationError, self.API_KEY.save)
    
    def test_wrong_secret_key_db(self):
        # Weird, but keep it, useful when debugging this test.
        # the_id = self.session_researcher.id  # instantiate the researcher, get their id
        # have to bypass validation
        ApiKey.objects.filter(id=self.API_KEY.id).update(access_key_secret="apples")
        resp = self.smart_post()
        # key doesn't match, forbidden
        self.assertEqual(403, resp.status_code)
    
    def test_wrong_secret_key_post(self):
        resp = self.less_smart_post(access_key="apples", secret_key=self.session_secret_key)
        # key doesn't match, forbidden
        self.assertEqual(403, resp.status_code)
    
    def test_wrong_access_key_db(self):
        # Weird, but keep it, useful when debugging this test.
        self.API_KEY.access_key_id = "apples"
        self.API_KEY.save()
        resp = self.smart_post()
        # no such user, forbidden
        self.assertEqual(403, resp.status_code)
    
    def test_wrong_access_key_post(self):
        resp = self.less_smart_post(access_key=self.session_access_key, secret_key="apples")
        # no such user, forbidden
        self.assertEqual(403, resp.status_code)
    
    def test_access_key_special_characters(self):
        self.session_access_key = "\x00" * 64
        self.smart_post_status_code(400)
    
    def test_secret_key_special_characters(self):
        self.session_secret_key = "\x00" * 64
        self.smart_post_status_code(400)
    
    def test_site_admin(self):
        self.assign_role(self.session_researcher, ResearcherRole.site_admin)
        self.smart_post_status_code(200)
    
    def test_researcher(self):
        self.assign_role(self.session_researcher, ResearcherRole.study_admin)
        self.smart_post_status_code(200)
    
    def test_study_admin(self):
        self.assign_role(self.session_researcher, ResearcherRole.researcher)
        self.smart_post_status_code(200)


class TestAPIGetStudies(DataApiTest):
    ENDPOINT_NAME = "data_api_endpoints.get_studies"
    
    def test_inactive_credentials(self):
        """ this test serves as a test of authentication database details. """
        self.API_KEY.is_active = False
        self.API_KEY.save()
        self.smart_post_status_code(403)
        self.API_KEY.refresh_from_db()
        self.assertFalse(self.API_KEY.is_active)  # don't change it yet
        self.assertIsNone(self.API_KEY.last_used)
        
        self.API_KEY.update_only(is_active=True) # ok now change it
        self.smart_post_status_code(200)
        self.API_KEY.refresh_from_db()
        self.assertIsInstance(self.API_KEY.last_used, datetime)
    
    def test_no_study(self):
        resp = self.smart_post_status_code(200)
        self.assertEqual(Study.objects.count(), 0)
        self.assertEqual(orjson.loads(resp.content), {})
    
    def test_no_study_relation(self):
        self.session_study
        resp = self.smart_post_status_code(200)
        self.assertEqual(Study.objects.count(), 1)
        self.assertEqual(orjson.loads(resp.content), {})
    
    def test_multiple_studies_one_relation(self):
        self.set_session_study_relation(ResearcherRole.researcher)
        self.generate_study("study2")
        resp = self.smart_post_status_code(200)
        self.assertEqual(
            orjson.loads(resp.content), {self.session_study.object_id: self.DEFAULT_STUDY_NAME}
        )
    
    def test_study_relation(self):
        self.set_session_study_relation(ResearcherRole.researcher)
        resp = self.smart_post_status_code(200)
        self.assertEqual(
            orjson.loads(resp.content), {self.session_study.object_id: self.DEFAULT_STUDY_NAME}
        )
    
    def test_study_relation_deleted(self):
        self.set_session_study_relation(ResearcherRole.researcher)
        self.session_study.update_only(deleted=True)
        resp = self.smart_post_status_code(200)
        self.assertEqual(orjson.loads(resp.content), {})
    
    def test_multiple_studies(self):
        self.set_session_study_relation(ResearcherRole.researcher)
        study2 = self.generate_study("study2")
        self.generate_study_relation(self.session_researcher, study2, ResearcherRole.researcher)
        resp = self.smart_post_status_code(200)
        self.assertEqual(
            orjson.loads(resp.content), {
                self.session_study.object_id: self.DEFAULT_STUDY_NAME,
                study2.object_id: study2.name
            }
        )
    
    def test_site_admin(self):
        self.set_session_study_relation(ResearcherRole.site_admin)
        resp = self.smart_post_status_code(200)
        self.assertEqual(
            orjson.loads(resp.content), {self.session_study.object_id: self.DEFAULT_STUDY_NAME}
        )
    
    def test_site_admin_deleted(self):
        self.set_session_study_relation(ResearcherRole.site_admin)
        self.session_study.update_only(deleted=True)
        resp = self.smart_post_status_code(200)
        self.assertEqual(orjson.loads(resp.content), {})


class TestAPIStudyUserAccess(DataApiTest):
    ENDPOINT_NAME = "data_api_endpoints.get_participant_ids_in_study"
    
    def test_missing_all_parameters(self):
        # self.set_session_study_relation(ResearcherRole)
        # use _smart_post
        resp = self.less_smart_post()
        # 400, missing parameter
        self.assertEqual(400, resp.status_code)
    
    def test_only_secret_key(self):
        resp = self.less_smart_post(secret_key=self.session_secret_key)
        # 400, missing parameter
        self.assertEqual(400, resp.status_code)
    
    def test_only_access_key(self):
        resp = self.less_smart_post(access_key=self.session_access_key)
        # 400, missing parameter
        self.assertEqual(400, resp.status_code)
    
    def test_only_study_obj_id(self):
        resp = self.less_smart_post(study_id=self.session_study.object_id)
        # 400, missing parameter
        self.assertEqual(400, resp.status_code)
    
    def test_only_study_pk(self):
        resp = self.less_smart_post(study_pk=self.session_study.pk)
        # 400, missing parameter
        self.assertEqual(400, resp.status_code)
    
    def test_wrong_secret_key_post(self):
        resp = self.less_smart_post(
            access_key="apples", secret_key=self.session_secret_key, study_pk=self.session_study.pk
        )
        # key doesn't match, forbidden
        self.assertEqual(403, resp.status_code)
    
    def test_wrong_access_key_post(self):
        resp = self.less_smart_post(
            access_key=self.session_access_key, secret_key="apples", study_pk=self.session_study.pk
        )
        # no such user, forbidden
        self.assertEqual(403, resp.status_code)
    
    def test_no_such_study_pk(self):
        # 0 is an invalid study id
        self.smart_post_status_code(404, study_pk=0)
    
    def test_no_such_study_obj(self):
        # 0 is an invalid study id
        self.smart_post_status_code(404, study_id='a' * 24)
    
    def test_bad_object_id(self):
        # 0 is an invalid study id
        self.smart_post_status_code(400, study_id='[' * 24)
        self.smart_post_status_code(400, study_id='a' * 5)
    
    def test_access_key_special_characters(self):
        self.session_access_key = "\x00" * 64
        self.smart_post_status_code(400, study_pk=self.session_study.pk)
    
    def test_secret_key_special_characters(self):
        self.session_secret_key = "\x00" * 64
        self.smart_post_status_code(400, study_pk=self.session_study.pk)
    
    def test_site_admin(self):
        self.assign_role(self.session_researcher, ResearcherRole.site_admin)
        self.smart_post_status_code(200, study_pk=self.session_study.pk)
    
    def test_researcher(self):
        self.assign_role(self.session_researcher, ResearcherRole.study_admin)
        self.smart_post_status_code(200, study_pk=self.session_study.pk)
    
    def test_study_admin(self):
        self.assign_role(self.session_researcher, ResearcherRole.researcher)
        self.smart_post_status_code(200, study_pk=self.session_study.pk)
    
    def test_no_relation(self):
        self.assign_role(self.session_researcher, None)
        self.smart_post_status_code(403, study_pk=self.session_study.pk)


class TestGetUsersInStudy(DataApiTest):
    ENDPOINT_NAME = "data_api_endpoints.get_participant_ids_in_study"
    
    def test_no_participants(self):
        self.set_session_study_relation(ResearcherRole.researcher)
        resp = self.smart_post_status_code(200, study_id=self.session_study.object_id)
        self.assertEqual(resp.content, b"[]")
    
    def test_one_participant(self):
        self.set_session_study_relation(ResearcherRole.researcher)
        self.using_default_participant()
        resp = self.smart_post_status_code(200, study_id=self.session_study.object_id)
        self.assertEqual(resp.content, f'["{self.default_participant.patient_id}"]'.encode())
    
    def test_two_participants(self):
        self.set_session_study_relation(ResearcherRole.researcher)
        self.using_default_participant()
        p2 = self.generate_participant(self.session_study)
        resp = self.smart_post_status_code(200, study_id=self.session_study.object_id)
        # ordering here is random because because generate_participant is random, need to handle it.
        match = [self.default_participant.patient_id, p2.patient_id]
        match.sort()
        from_json = orjson.loads(resp.content)
        from_json.sort()
        self.assertEqual(from_json, match)


class TestGetParticipantDataInfo(DataApiTest):
    ENDPOINT_NAME = "data_api_endpoints.get_participant_data_quantities"
    
    @property
    def ref_zero_row_output(self):
        # this is manual so that if you change the fields in the future we will get a failure
        return {
            'accelerometer_bytes': 0,
            'ambient_audio_bytes': 0,
            'app_log_bytes': 0,
            'bluetooth_bytes': 0,
            'calls_bytes': 0,
            'devicemotion_bytes': 0,
            'gps_bytes': 0,
            'gyro_bytes': 0,
            'identifiers_bytes': 0,
            'ios_log_bytes': 0,
            'magnetometer_bytes': 0,
            'power_state_bytes': 0,
            'proximity_bytes': 0,
            'reachability_bytes': 0,
            'survey_answers_bytes': 0,
            'survey_timings_bytes': 0,
            'texts_bytes': 0,
            'audio_recordings_bytes': 0,
            'wifi_bytes': 0,
        }
    
    def test_no_participants(self):
        self.set_session_study_relation(ResearcherRole.researcher)
        resp = self.smart_post_status_code(200, study_id=self.session_study.object_id)
        self.assertEqual(resp.content, b"{}")
    
    def test_one_empty_participant(self):
        self.set_session_study_relation(ResearcherRole.researcher)
        self.using_default_participant()
        resp = self.smart_post_status_code(200, study_id=self.session_study.object_id)
        self.assertEqual(orjson.loads(resp.content), {self.default_participant.patient_id: self.ref_zero_row_output})
    
    def test_two_empty_participants(self):
        self.set_session_study_relation(ResearcherRole.researcher)
        self.using_default_participant()
        p2 = self.generate_participant(self.session_study)
        resp = self.smart_post_status_code(200, study_id=self.session_study.object_id)
        self.assertEqual(
            orjson.loads(resp.content),
            {
                self.default_participant.patient_id: self.ref_zero_row_output,
                p2.patient_id: self.ref_zero_row_output,
            }
        )
    
    def test_one_participant_with_data_1(self):
        self.set_session_study_relation(ResearcherRole.researcher)
        self.default_summary_statistic_daily.update(**{k: 1 for k in DATA_QUANTITY_FIELD_NAMES})
        ref_out = self.ref_zero_row_output
        for k in ref_out:
            ref_out[k] = 1
        resp = self.smart_post_status_code(200, study_id=self.session_study.object_id)
        self.assertEqual(orjson.loads(resp.content), {self.default_participant.patient_id: ref_out})
    
    def test_one_participant_with_each_field_incrementing(self):
        self.set_session_study_relation(ResearcherRole.researcher)
        # depends on row order in DATA_QUANTITY_FIELD_NAMES
        self.default_summary_statistic_daily.update(**{k: i for i, k in enumerate(DATA_QUANTITY_FIELD_NAMES)})
        ref_out = self.ref_zero_row_output
        for i, k in enumerate(ref_out):
            ref_out[k] = i
        resp = self.smart_post_status_code(200, study_id=self.session_study.object_id)
        self.assertEqual(orjson.loads(resp.content), {self.default_participant.patient_id: ref_out})
    
    def test_three_participants_with_data(self):
        self.set_session_study_relation(ResearcherRole.researcher)
        
        p1 = self.default_participant
        self.default_summary_statistic_daily.update(**{k: 10 for k in DATA_QUANTITY_FIELD_NAMES})
        # patient0 would be an invalid patient id because it has a 0 in it, we just need something
        # that sorts before patient1
        p0 = self.generate_participant(self.session_study, "atient11")
        self.generate_summary_statistic_daily(date.today(), p0).update(**{k: 100 for k in DATA_QUANTITY_FIELD_NAMES})
        p2 = self.generate_participant(self.session_study, "patient2")
        self.generate_summary_statistic_daily(date.today(), p2).update(**{k: 1000 for k in DATA_QUANTITY_FIELD_NAMES})
        
        # setup unique rows
        ref_row_out_p1 = self.ref_zero_row_output
        for k in ref_row_out_p1:
            ref_row_out_p1[k] = 10
        ref_row_out_p0 = self.ref_zero_row_output
        for k in ref_row_out_p0:
            ref_row_out_p0[k] = 100
        ref_row_out_p2 = self.ref_zero_row_output
        for k in ref_row_out_p2:
            ref_row_out_p2[k] = 1000
        
        resp = self.smart_post_status_code(200, study_id=self.session_study.object_id)
        self.assertEqual(
            orjson.loads(resp.content),
            {
                p0.patient_id: ref_row_out_p0,
                p1.patient_id: ref_row_out_p1,
                p2.patient_id: ref_row_out_p2,
            }
        )


class TestDownloadStudyInterventions(DataApiTest):
    ENDPOINT_NAME = "data_api_endpoints.download_study_interventions"
    
    def test_no_interventions(self):
        self.set_session_study_relation(ResearcherRole.researcher)
        resp = self.smart_post_status_code(200, study_id=self.session_study.object_id)
        self.assertEqual(resp.content, b"{}")
    
    def test_survey_with_one_intervention(self):
        self.set_session_study_relation(ResearcherRole.researcher)
        self.default_populated_intervention_date
        self.default_relative_schedule
        resp = self.smart_post_status_code(200, study_id=self.session_study.object_id)
        json_unpacked = orjson.loads(resp.content)
        correct_output = {
            self.DEFAULT_PARTICIPANT_NAME:
                {
                    self.DEFAULT_SURVEY_OBJECT_ID:
                        {
                            self.DEFAULT_INTERVENTION_NAME: self.CURRENT_DATE.isoformat()
                        }
                }
        }
        self.assertDictEqual(json_unpacked, correct_output)


class TestStudySurveyHistory(DataApiTest):
    ENDPOINT_NAME = "data_api_endpoints.download_study_survey_history"
    
    def test_no_surveys(self):
        self.set_session_study_relation(ResearcherRole.researcher)
        resp = self.smart_post_status_code(200, study_id=self.session_study.object_id)
        ret = b"".join(resp.streaming_content)
        # the output is a dictionary with survey ids as keys
        self.assertEqual(ret, b"{}")
    
    def test_one_survey_two_archives(self):
        self.set_session_study_relation(ResearcherRole.researcher)
        self.default_survey
        
        self.assertEqual(Survey.objects.count(), 1)
        self.assertEqual(SurveyArchive.objects.count(), 1)
        self.default_survey.content = '["a_string"]'
        self.default_survey.archive()
        self.assertEqual(SurveyArchive.objects.count(), 2)
        
        # for archive in SurveyArchive.objects.all():
        
        resp = self.smart_post_status_code(200, study_id=self.session_study.object_id)
        should_be = '{"u1Z3SH7l2xNsw72hN3LnYi96":[{"archive_start":'\
                    '"replace1","survey_json":[]},{"archive_start":'\
                    '"replace2","survey_json":["a_string"]}]}'
        archive1, archive2 = SurveyArchive.objects.all()
        should_be = should_be.replace("replace1", archive1.archive_start.isoformat())
        should_be = should_be.replace("replace2", archive2.archive_start.isoformat())
        ret = b"".join(resp.streaming_content)
        self.assertEqual(ret, should_be.encode())
    
    def test_one_survey_one_archive(self):
        self.set_session_study_relation(ResearcherRole.researcher)
        self.default_survey
        self.assertEqual(Survey.objects.count(), 1)
        self.assertEqual(SurveyArchive.objects.count(), 1)
        archive = self.default_survey.most_recent_archive()
        resp = self.smart_post_status_code(200, study_id=self.session_study.object_id)
        should_be = b'{"u1Z3SH7l2xNsw72hN3LnYi96":[{"archive_start":"replace","survey_json":[]}]}'
        should_be = should_be.replace(b"replace", archive.archive_start.isoformat().encode())
        ret = b"".join(resp.streaming_content)
        self.assertEqual(ret, should_be)


class TestDownloadParticipantTableData(DataApiTest, ParticipantTableHelperMixin):
    ENDPOINT_NAME = "data_api_endpoints.get_participant_table_data"
    
    def test_no_study_param(self):
        self.set_session_study_relation(ResearcherRole.researcher)
        self.smart_post_status_code(400)
    
    def test_missing_data_param(self):
        self.set_session_study_relation(ResearcherRole.researcher)
        resp = self.smart_post_status_code(400, study_id=self.session_study.object_id)
        self.assertEqual(resp.content, MISSING_JSON_CSV_MESSAGE)
    
    def test_data_format_param_wrong(self):
        self.set_session_study_relation(ResearcherRole.researcher)
        resp = self.smart_post_status_code(400, study_id=self.session_study.object_id, data_format="apples")
        self.assertEqual(resp.content, MISSING_JSON_CSV_MESSAGE)
    
    def test_no_data_csv(self):
        self.set_session_study_relation(ResearcherRole.researcher)
        resp = self.smart_post_status_code(200, study_id=self.session_study.object_id, data_format="csv")
        # its just the header row and a \r\n
        self.assertEqual(resp.content, self.header().encode())
    
    def test_no_data_json(self):
        self.set_session_study_relation(ResearcherRole.researcher)
        resp = self.smart_post_status_code(200, study_id=self.session_study.object_id, data_format="json")
        # there are no rows for columns to be in
        self.assertEqual(resp.content, b"[]")
    
    def test_no_data_json_table(self):
        self.set_session_study_relation(ResearcherRole.researcher)
        resp = self.smart_post_status_code(
            200, study_id=self.session_study.object_id, data_format="json_table"
        )
        # results in a table with a first row of column names
        # this looks like a string containing: [["Created On","Patient ID","Status"," .....
        # strip() strips \r\n
        row = ('[["' + '","'.join(self.header().strip().split(",")) + '"]]').encode()
        self.assertEqual(resp.content, row)
    
    def test_one_participant_csv(self):
        resp = self._do_one_participant("csv")
        # the header row and a row of data
        # b'Created On,Patient ID,Status,OS Type,Last Upload,Last Survey Download,Last Registration,Last Set Password,Last Push Token Update,Last Device Settings Update,Last OS Version,App Version Code,App Version Name,Last Heartbeat
        #2024-06-06,patient1,Inactive,ANDROID,None,None,None,None,None,None,None,None,None,None
        row1 = self.header()
        row2 = "2020-01-01,patient1,Inactive,ANDROID,"
        row2 += "None,None,None,None,None,None,None,None,None,None,None\r\n"
        self.assertEqual(resp.content, (row1 + row2).encode())
        self.modify_participant()
        
        # this is a correct dictionary representation of the row
        reference_output_populated = {
            'Created On': '2020-01-01',
            'Patient ID': 'patient1',
            'Status': 'Inactive',
            'OS Type': 'ANDROID',
            'First Registration Date': 'None',
            'Last Registration': '2020-01-04 12:00:00 (UTC)',
            'Last Upload': '2020-01-02 12:00:00 (UTC)',
            'Last Survey Download': '2020-01-03 12:00:00 (UTC)',
            'Last Set Password': '2020-01-05 12:00:00 (UTC)',
            'Last Push Token Update': '2020-01-06 12:00:00 (UTC)',
            'Last Device Settings Update': '2020-01-07 12:00:00 (UTC)',
            'Last OS Version': '1.0',
            'App Version Code': '6',
            'App Version Name': 'six',
            'Last Heartbeat': '2020-01-08 12:00:00 (UTC)',
        }
        # sanity check that the keys here are the same as the header row...
        self.assertEqual(row1, ",".join(reference_output_populated.keys()) + "\r\n" )
        reference_content = (row1 + ",".join(reference_output_populated.values())).encode() + b"\r\n"
        
        resp = self._do_one_participant("csv")
        self.assertEqual(resp.content, reference_content)
    
    def test_one_participant_json(self):
        resp = self._do_one_participant("json")
        keys = self.header().strip().split(",")  # strip() strips \r\n
        
        row = {
            keys[0]: "2020-01-01",   # Created On
            keys[1]: "patient1",     # Patient ID
            keys[2]: "Inactive",     # Status
            keys[3]: "ANDROID",      # OS Type
            keys[4]: "None",         # First Registration Date
            keys[5]: "None",         # Last Registration
            keys[6]: "None",         # Last Upload
            keys[7]: "None",         # Last Survey Download
            keys[8]: "None",         # Last Set Password
            keys[9]: "None",         # Last Push Token Update
            keys[10]: "None",        # Last Device Settings Update
            keys[11]: "None",        # Last OS Version
            keys[12]: "None",        # App Version Code
            keys[13]: "None",        # App Version Name
            keys[14]: "None",        # Last Heartbeat
        }
        self.assertEqual(orjson.loads(resp.content), [row])
        
        self.modify_participant()
        resp = self._do_one_participant("json")
        row = {
            keys[0]: "2020-01-01",                  # Created On
            keys[1]: "patient1",                    # Patient ID
            keys[2]: "Inactive",                    # Status
            keys[3]: "ANDROID",                     # OS Type
            keys[4]: "None",                        # First Registration Date
            keys[5]: "2020-01-04 12:00:00 (UTC)",   # Last Registration
            keys[6]: "2020-01-02 12:00:00 (UTC)",   # Last Upload
            keys[7]: "2020-01-03 12:00:00 (UTC)",   # Last Survey Download
            keys[8]: "2020-01-05 12:00:00 (UTC)",   # Last Set Password
            keys[9]: "2020-01-06 12:00:00 (UTC)",   # Last Push Token Update
            keys[10]: "2020-01-07 12:00:00 (UTC)",  # Last Device Settings Update
            keys[11]: "1.0",                        # Last OS Version
            keys[12]: "6",                          # App Version Code
            keys[13]: "six",                        # App Version Name
            keys[14]: "2020-01-08 12:00:00 (UTC)",  # Last Heartbeat
        }
        self.assertEqual(orjson.loads(resp.content), [row])
    
    def test_one_participant_json_table(self):
        resp = self._do_one_participant("json_table")
        keys = self.header().strip().split(",")  # strip() strips \r\n
        
        row = [
            "2020-01-01",  # Created On
            "patient1",    # Patient ID
            "Inactive",    # Status
            "ANDROID",     # OS Type
            "None",        # First Registration Date
            "None",        # Last Registration
            "None",        # Last Upload
            "None",        # Last Survey Download
            "None",        # Last Set Password
            "None",        # Last Push Token Update
            "None",        # Last Device Settings Update
            "None",        # Last OS Version
            "None",        # App Version Code
            "None",        # App Version Name
            "None",        # Last Heartbeat
        ]
        self.assertEqual(orjson.loads(resp.content), [keys, row])
        
        self.modify_participant()
        resp = self._do_one_participant("json_table")
        row = [
            "2020-01-01",                  # Created On
            "patient1",                    # Patient ID
            "Inactive",                    # Status
            "ANDROID",                     # OS Type
            "None",                        # First Registration Date
            "2020-01-04 12:00:00 (UTC)",   # Last Registration
            "2020-01-02 12:00:00 (UTC)",   # Last Upload
            "2020-01-03 12:00:00 (UTC)",   # Last Survey Download
            "2020-01-05 12:00:00 (UTC)",   # Last Set Password
            "2020-01-06 12:00:00 (UTC)",   # Last Push Token Update
            "2020-01-07 12:00:00 (UTC)",   # Last Device Settings Update
            "1.0",                         # Last OS Version
            "6",                           # App Version Code
            "six",                         # App Version Name
            "2020-01-08 12:00:00 (UTC)",   # Last Heartbeat
        ]
        self.assertEqual(orjson.loads(resp.content), [keys, row])
    
    def _do_one_participant(self, data_format: str):
        if not hasattr(self, "_default_study_relation"):
            self.set_session_study_relation(ResearcherRole.researcher)
        self.using_default_participant()
        self.default_participant.update_only(created_on=datetime(2020, 1, 1, 12, tzinfo=UTC))
        return self.smart_post_status_code(200, study_id=self.session_study.object_id, data_format=data_format)
    
    def modify_participant(self):
        # you have to update this list if you add fields to EXTRA_TABLE_FIELDS
        some_column_names_and_values = [
            ("created_on", datetime(2020, 1, 1, 12, tzinfo=UTC)),  # not an extra row
            ("last_upload", datetime(2020, 1, 2, 12, tzinfo=UTC)),
            ("last_get_latest_surveys", datetime(2020, 1, 3, 12, tzinfo=UTC)),
            ("last_register_user", datetime(2020, 1, 4, 12, tzinfo=UTC)),
            ("last_set_password", datetime(2020, 1, 5, 12, tzinfo=UTC)),
            ("last_set_fcm_token", datetime(2020, 1, 6, 12, tzinfo=UTC)),
            ("last_get_latest_device_settings", datetime(2020, 1, 7, 12, tzinfo=UTC)),
            ("last_os_version", "1.0"),
            ("last_version_code", "6"),
            ("last_version_name", "six"),
            ("last_heartbeat_checkin", datetime(2020, 1, 8, 12, tzinfo=UTC)),
        ]
        for name, value in some_column_names_and_values:
            setattr(self.default_participant, name, value)
        self.default_participant.save()


class TestGetParticipantUploadHistory(DataApiTest):
    ENDPOINT_NAME = "data_api_endpoints.get_participant_upload_history"
    
    def create_an_upload(self):
        # file name has a transformation applied to it, the patient id is stripped
        UploadTracking.objects.create(
            participant=self.default_participant,
            file_path=f"{self.default_participant.patient_id}/some_file_name",
            file_size="10",
            timestamp=datetime(2020,1,1,12, tzinfo=UTC),
        )
    
    def test_no_participant_parameter(self):
        # it should 400
        self.set_session_study_relation(ResearcherRole.researcher)
        self.using_default_participant()
        resp = self.smart_post_status_code(400)
        self.assertEqual(resp.content, b"")
    
    def test_bad_participant_parameter(self):
        # it should 404 and not render the 404 page
        self.set_session_study_relation(ResearcherRole.researcher)
        self.using_default_participant()
        resp = self.smart_post_status_code(404, participant_id="a" * 8)
        self.assertEqual(resp.content, b"")
    
    def test_researcher_one_participant_no_uploads(self):
        self.set_session_study_relation(ResearcherRole.researcher)
        self._test_one_participant_no_uploads()
    
    def test_study_admin_one_participant_no_uploads(self):
        self.set_session_study_relation(ResearcherRole.study_admin)
        self._test_one_participant_no_uploads()
    
    def test_site_admin_one_participant_no_uploads(self):
        self.set_session_study_relation(ResearcherRole.site_admin)
        self._test_one_participant_no_uploads()
    
    def _test_one_participant_no_uploads(self):
        resp = self.smart_post_status_code(200, participant_id=self.default_participant.patient_id)
        content = b"".join(resp.streaming_content)
        self.assertEqual(content, b'[]')
    
    def test_no_relation_one_participant_no_uploads(self):
        resp = self.smart_post_status_code(403, participant_id=self.default_participant.patient_id)
    
    def test_one_participant_one_upload_values(self):
        self.set_session_study_relation(ResearcherRole.researcher)
        self.using_default_participant()
        self.create_an_upload()
        resp = self.smart_post_status_code(200, participant_id=self.default_participant.patient_id)
        content = b"".join(resp.streaming_content)
        self.assertEqual(
            content,
            b'[{"file_size":10,"timestamp":"2020-01-01T12:00:00Z","file_name":"some_file_name"}]'
        )
    
    def test_one_participant_one_upload_values_list(self):
        # as values but formatted data lacks keys.
        self.set_session_study_relation(ResearcherRole.researcher)
        self.using_default_participant()
        self.create_an_upload()
        resp = self.smart_post_status_code(
            200, participant_id=self.default_participant.patient_id, omit_keys="true"
        )
        content = b"".join(resp.streaming_content)
        self.assertEqual(content, b'[[10,"2020-01-01T12:00:00Z","some_file_name"]]')
    
    def test_ten_uploads_values(self):
        self.set_session_study_relation(ResearcherRole.researcher)
        self.using_default_participant()
        for i in range(10):
            self.create_an_upload()
        resp = self.smart_post_status_code(200, participant_id=self.default_participant.patient_id)
        content = b"".join(resp.streaming_content)
        text = b'{"file_size":10,"timestamp":"2020-01-01T12:00:00Z","file_name":"some_file_name"}'
        for i in range(9):
            text += b',{"file_size":10,"timestamp":"2020-01-01T12:00:00Z","file_name":"some_file_name"}'
        text = b"[" + text + b"]"
        self.assertEqual(content, text)
    
    def test_ten_uploads_values_list(self):
        self.set_session_study_relation(ResearcherRole.researcher)
        self.using_default_participant()
        for i in range(10):
            self.create_an_upload()
        resp = self.smart_post_status_code(
            200, participant_id=self.default_participant.patient_id, omit_keys="true"
        )
        content = b"".join(resp.streaming_content)
        text = b'[10,"2020-01-01T12:00:00Z","some_file_name"]'
        for i in range(9):
            text += b',[10,"2020-01-01T12:00:00Z","some_file_name"]'
        text = b"[" + text + b"]"
        self.assertEqual(content, text)


class TestParticipantHeartbeatHistory(DataApiTest):
    ENDPOINT_NAME = "data_api_endpoints.get_participant_heartbeat_history"
    
    def create_a_heartbeat(self):
        AppHeartbeats.objects.create(
            timestamp=datetime(2020,1,1,12, tzinfo=UTC), participant=self.default_participant)
    
    def test_no_participant_parameter(self):
        # it should 400
        self.set_session_study_relation(ResearcherRole.researcher)
        self.using_default_participant()
        resp = self.smart_post_status_code(400)
        self.assertEqual(resp.content, b"")
    
    def test_bad_participant_parameter(self):
        # it should 404 and not render the 404 page
        self.set_session_study_relation(ResearcherRole.researcher)
        self.using_default_participant()
        resp = self.smart_post_status_code(404, participant_id="a" * 8)
        self.assertEqual(resp.content, b"")
    
    def test_researcher_one_participant_no_heartbeats(self):
        self.set_session_study_relation(ResearcherRole.researcher)
        self._test_one_participant_no_heartbeats()
    
    def test_study_admin_one_participant_no_heartbeats(self):
        self.set_session_study_relation(ResearcherRole.study_admin)
        self._test_one_participant_no_heartbeats()
    
    def test_site_admin_one_participant_no_heartbeats(self):
        self.set_session_study_relation(ResearcherRole.site_admin)
        self._test_one_participant_no_heartbeats()
    
    def _test_one_participant_no_heartbeats(self):
        resp = self.smart_post_status_code(200, participant_id=self.default_participant.patient_id)
        content = b"".join(resp.streaming_content)
        self.assertEqual(content, b'[]')
    
    def test_no_relation_one_participant_no_heartbeats(self):
        resp = self.smart_post_status_code(403, participant_id=self.default_participant.patient_id)
    
    def test_one_participant_one_heartbeat_values(self):
        self.set_session_study_relation(ResearcherRole.researcher)
        self.using_default_participant()
        self.create_a_heartbeat()
        resp = self.smart_post_status_code(200, participant_id=self.default_participant.patient_id)
        content = b"".join(resp.streaming_content)
        self.assertEqual(content, b'[{"timestamp":"2020-01-01T12:00:00Z"}]')
    
    def test_one_participant_one_heartbeat_values_list(self):
        # as values but formatted data lacks keys.
        self.set_session_study_relation(ResearcherRole.researcher)
        self.using_default_participant()
        self.create_a_heartbeat()
        resp = self.smart_post_status_code(
            200, participant_id=self.default_participant.patient_id, omit_keys="true"
        )
        content = b"".join(resp.streaming_content)
        self.assertEqual(content, b'["2020-01-01T12:00:00Z"]')
    
    def test_ten_heartbeats_values(self):
        self.set_session_study_relation(ResearcherRole.researcher)
        self.using_default_participant()
        for i in range(10):
            self.create_a_heartbeat()
        resp = self.smart_post_status_code(200, participant_id=self.default_participant.patient_id)
        content = b"".join(resp.streaming_content)
        text = b'{"timestamp":"2020-01-01T12:00:00Z"}'
        for i in range(9):
            text += b',{"timestamp":"2020-01-01T12:00:00Z"}'
        text = b"[" + text + b"]"
        self.assertEqual(content, text)
    
    def test_ten_heartbeats_values_list(self):
        self.set_session_study_relation(ResearcherRole.researcher)
        self.using_default_participant()
        for i in range(10):
            self.create_a_heartbeat()
        resp = self.smart_post_status_code(
            200, participant_id=self.default_participant.patient_id, omit_keys="true"
        )
        content = b"".join(resp.streaming_content)
        text = b'"2020-01-01T12:00:00Z"'
        for i in range(9):
            text += b',"2020-01-01T12:00:00Z"'
        text = b"[" + text + b"]"
        self.assertEqual(content, text)


class TestParticipantVersionHistory(DataApiTest):
    ENDPOINT_NAME = "data_api_endpoints.get_participant_version_history"
    
    def setUp(self) -> None:
        # we need a timestamp to sanity check against, generated at the beginning of each test
        self.test_start = timezone.now()
        return super().setUp()
    
    @property
    def format_the_test_start_correctly(self):
        options = orjson.OPT_OMIT_MICROSECONDS | orjson.OPT_UTC_Z
        # need to strip the brackets and off the ends
        return orjson.dumps([self.test_start], option=options).decode()[1:-1]
    
    @property
    def a_values_string(self) -> bytes:
        return '{"app_version_code":"1","app_version_name":"1.0","os_version":"1.0","created_on"' \
            f':{self.format_the_test_start_correctly}'.encode() + b'}'
    
    @property
    def a_values_list_string(self) -> bytes:
        return f'["1","1.0","1.0",{self.format_the_test_start_correctly}]'.encode()
    
    def create_a_version(self) -> AppVersionHistory:
        history = self.default_participant.app_version_history.create(
            # this breaks the rules for ios app versions, assumes android
            app_version_code="1", app_version_name="1.0", os_version="1.0", os_is_ios=False
        )
        history.update_only(created_on=self.test_start)  # annoyingly this can't be specified above
        return history
    
    def test_no_participant_parameter(self):
        # it should 400
        self.set_session_study_relation(ResearcherRole.researcher)
        self.using_default_participant()
        resp = self.smart_post_status_code(400)
        self.assertEqual(resp.content, b"")
    
    def test_bad_participant_parameter(self):
        # it should 404 and not render the 404 page
        self.set_session_study_relation(ResearcherRole.researcher)
        self.using_default_participant()
        resp = self.smart_post_status_code(404, participant_id="a" * 8)
        self.assertEqual(resp.content, b"")
    
    def test_researcher_one_participant_no_versions(self):
        self.set_session_study_relation(ResearcherRole.researcher)
        self._test_one_participant_no_versions()
    
    def test_study_admin_one_participant_no_versions(self):
        self.set_session_study_relation(ResearcherRole.study_admin)
        self._test_one_participant_no_versions()
    
    def test_site_admin_one_participant_no_versions(self):
        self.set_session_study_relation(ResearcherRole.site_admin)
        self._test_one_participant_no_versions()
    
    def _test_one_participant_no_versions(self):
        resp = self.smart_post_status_code(200, participant_id=self.default_participant.patient_id)
        content = b"".join(resp.streaming_content)
        self.assertEqual(content, b'[]')
    
    def test_no_relation_one_participant_no_versions(self):
        resp = self.smart_post_status_code(403, participant_id=self.default_participant.patient_id)
    
    def test_one_participant_one_version_values(self):
        self.set_session_study_relation(ResearcherRole.researcher)
        self.using_default_participant()
        self.create_a_version()
        resp = self.smart_post_status_code(200, participant_id=self.default_participant.patient_id)
        content = b"".join(resp.streaming_content)
        self.assertEqual(content, b"[" + self.a_values_string + b"]")
    
    def test_one_participant_one_version_values_list(self):
        self.set_session_study_relation(ResearcherRole.researcher)
        self.using_default_participant()
        self.create_a_version()
        resp = self.smart_post_status_code(
            200, participant_id=self.default_participant.patient_id, omit_keys="true"
        )
        content = b"".join(resp.streaming_content)
        self.assertEqual(content, b"[" + self.a_values_list_string + b"]")
    
    def test_ten_versions_values(self):
        self.set_session_study_relation(ResearcherRole.researcher)
        self.using_default_participant()
        for _ in range(10):
            self.create_a_version()
        resp = self.smart_post_status_code(200, participant_id=self.default_participant.patient_id)
        content = b"".join(resp.streaming_content)
        text = self.a_values_string
        repeat = b',' + text
        for _ in range(9):
            text += repeat
        text = b"[" + text + b"]"
        self.assertEqual(content, text)
    
    def test_ten_versions_values_list(self):
        self.set_session_study_relation(ResearcherRole.researcher)
        self.using_default_participant()
        for _ in range(10):
            self.create_a_version()
        resp = self.smart_post_status_code(
            200, participant_id=self.default_participant.patient_id, omit_keys="true"
        )
        content = b"".join(resp.streaming_content)
        text = self.a_values_list_string
        for _ in range(9):
            text += b"," + self.a_values_list_string
        text = b"[" + text + b"]"
        self.assertEqual(content, text)


# data_api_endpoints.get_summary_statistics is identical to the tableau_api.get_tableau_daily, which is
# tested extensively in test_tableau_api.py. The difference is that this endpoint uses the data
# access api decorator for authentication and the other is explicitly for tableau integration.
# All we need to test is that this works at all.
class TestGetSummaryStatistics(DataApiTest):
    ENDPOINT_NAME = "data_api_endpoints.get_summary_statistics"
    
    def test_no_study_param(self):
        self.set_session_study_relation(ResearcherRole.researcher)
        self.smart_post_status_code(400)
    
    def test_no_data(self):
        self.set_session_study_relation(ResearcherRole.researcher)
        resp = self.smart_post_status_code(200, study_id=self.session_study.object_id)
        content = b"".join(resp.streaming_content)
        self.assertEqual(content, b"[]")
    
    def test_single_summary_statistic(self):
        self.set_session_study_relation(ResearcherRole.researcher)
        self.default_summary_statistic_daily
        resp = self.smart_post_status_code(200, study_id=self.session_study.object_id)
        content = b"".join(resp.streaming_content)
        
        # get the data
        list_of_dict = orjson.loads(content)
        self.assertEqual(len(list_of_dict), 1)
        exported_summary_statistic = list_of_dict[0]
        
        # assemble the correct data directly out of the database, do some formatting, confirm match.
        correct = {
            k: v for k,v in
            self.default_summary_statistic_daily.as_dict().items()
            if k in SERIALIZABLE_FIELD_NAMES
        }
        correct["date"] = correct["date"].isoformat()
        correct["study_id"] = self.session_study.object_id
        correct["participant_id"] = self.default_participant.patient_id
        
        self.assertDictEqual(exported_summary_statistic, correct)


class TestGetParticipantDeviceStatusHistory(DataApiTest):
    ENDPOINT_NAME = "data_api_endpoints.get_participant_device_status_report_history"
    COLUMNS = ["created_on", "endpoint", "app_os", "os_version", "app_version", "device_status"]
    
    def test_no_participant_parameter(self):
        # it should 400
        self.set_session_study_relation(ResearcherRole.researcher)
        self.using_default_participant()
        resp = self.smart_post_status_code(400)
        self.assertEqual(resp.content, b"")
    
    def test_bad_participant_parameter(self):
        # it should 404 and not render the 404 page
        self.set_session_study_relation(ResearcherRole.researcher)
        self.using_default_participant()
        resp = self.smart_post_status_code(404, participant_id="a" * 8)
        self.assertEqual(resp.content, b"")
    
    def test_participant_on_unauthenticated_study(self):
        wrong_study = self.generate_study("study 2")
        self.generate_study_relation(self.session_researcher, wrong_study, ResearcherRole.researcher)
        self.using_default_participant()
        resp = self.smart_post_status_code(403, participant_id=self.default_participant.patient_id)
    
    def test_fields_are_correct_empty_report(self):
        self.set_session_study_relation(ResearcherRole.researcher)
        self.using_default_participant()
        status_history = self.generate_device_status_report_history()
        resp = self.smart_post_status_code(200, participant_id=self.default_participant.patient_id)
        content = b"".join(resp.streaming_content)
        out_list_of_dicts = orjson.loads(content)
        self.assertEqual(len(out_list_of_dicts), 1)
        out_dict = out_list_of_dicts[0]
        reference_out_dict = {
            'created_on': status_history.created_on.strftime("%Y-%m-%dT%H:%M:%SZ"),
            'endpoint': 'test',
            'app_os': 'ANDROID',
            'os_version': '1.0',
            'app_version': '1.0',
            'device_status': {}
        }
        self.assertDictEqual(out_dict, reference_out_dict)
    
    def test_fields_are_correct_with_compression(self):
        obj = ["this is a test string inside a json list so we have something to deserialize"]
        slug = b'["this is a test string inside a json list so we have something to deserialize"]'
        zslug = zstd.compress(slug, 0, 0)  # saves 4 whole bytes!
        self.set_session_study_relation(ResearcherRole.researcher)
        self.using_default_participant()
        status_history = self.generate_device_status_report_history(compressed_report=zslug)
        resp = self.smart_post_status_code(200, participant_id=self.default_participant.patient_id)
        content = b"".join(resp.streaming_content)
        out_list_of_dicts = orjson.loads(content)
        self.assertEqual(len(out_list_of_dicts), 1)
        out_dict = out_list_of_dicts[0]
        reference_out_dict = {
            'created_on': status_history.created_on.strftime("%Y-%m-%dT%H:%M:%SZ"),
            'endpoint': 'test',
            'app_os': 'ANDROID',
            'os_version': '1.0',
            'app_version': '1.0',
            'device_status': obj,
        }
        self.assertDictEqual(out_dict, reference_out_dict)


#
## Tableau API
#

class TestGetTableauDaily(TableauAPITest):
    ENDPOINT_NAME = "data_api_endpoints.get_tableau_daily"
    today = date.today()
    yesterday = date.today() - timedelta(days=1)
    tomorrow = date.today() + timedelta(days=-1)
    # parameters are
    # end_date, start_date, limit, order_by, order_direction, participant_ids, fields
    
    # helpers
    @property
    def params_all_fields(self):
        return {"fields": ",".join(SERIALIZABLE_FIELD_NAMES)}
    
    @property
    def params_all_defaults(self):
        return {'participant_ids': self.default_participant.patient_id, **self.params_all_fields}
    
    @property
    def full_response_dict(self):
        defaults = self.default_summary_statistic_daily_cheatsheet()
        defaults["date"] = date.today().isoformat()
        defaults["participant_id"] = self.default_participant.patient_id
        defaults["study_id"] = self.session_study.object_id
        return defaults
    
    def smart_get_200_auto_headers(self, **kwargs) -> StreamingHttpResponse:
        return self.smart_get_status_code(
            200, self.session_study.object_id, data=kwargs, **self.raw_headers
        )
    
    def test_tableau_api_credential_upgrade(self, **kwargs) -> StreamingHttpResponse:
        self.assertEqual(ApiKey.DESIRED_ALGORITHM, "sha256")
        self.assertEqual(ApiKey.DESIRED_ITERATIONS, 2)
        ApiKey.objects.all().delete()  # clear the autogenerated test key
        # generate a new key with the sha1 (copying TableauAPITest)
        ApiKey.DESIRED_ALGORITHM = "sha1"
        self.api_key = ApiKey.generate(self.session_researcher)
        ApiKey.DESIRED_ALGORITHM = "sha256"
        self.api_key_public = self.api_key.access_key_id
        self.api_key_private = self.api_key.access_key_secret_plaintext
        original_secret = self.api_key.access_key_secret
        # run the test_summary_statistics_daily_no_params_empty_db test to make sure it works at all
        self.test_summary_statistics_daily_no_params_empty_db()
        self.api_key.refresh_from_db()
        self.assertNotEqual(original_secret, self.api_key.access_key_secret)
        self.assertIn("sha256", self.api_key.access_key_secret)
        self.assertIn("sha1", original_secret)
        # and run the test again to make sure the new db entry continues to work.
        self.test_summary_statistics_daily_no_params_empty_db()
    
    def test_bad_field_name(self):
        self.generate_summary_statistic_daily()
        params = self.params_all_defaults
        params["fields"] = params["fields"].replace("accelerometer", "accellerometer")
        resp = self.smart_get_status_code(
            400, self.session_study.object_id, data=params, **self.raw_headers
        )
        self.assertEqual(
            resp.content, b'{"errors": ["beiwe_accellerometer_bytes is not a valid field"]}'
        )
    
    def test_summary_statistics_daily_no_params_empty_db(self):
        # unpack the raw headers like this, they magically just work because http language is weird
        resp = self.smart_get_200_auto_headers()
        response_content = b"".join(resp.streaming_content)
        self.assertEqual(response_content, b'[]')
    
    def test_summary_statistics_daily_all_params_empty_db(self):
        resp = self.smart_get_200_auto_headers(**self.params_all_fields)
        response_content = b"".join(resp.streaming_content)
        self.assertEqual(response_content, b'[]')
    
    def test_summary_statistics_daily_all_params_all_populated(self):
        self.generate_summary_statistic_daily()
        resp = self.smart_get_200_auto_headers(**self.params_all_defaults)
        response_object = orjson.loads(b"".join(resp.streaming_content))
        self.assertEqual(len(response_object), 1)
        assert compare_dictionaries(response_object[0], self.full_response_dict)
    
    def test_summary_statistics_daily_all_params_dates_all_populated(self):
        self.generate_summary_statistic_daily()
        params = {"end_date": date.today(), "start_date": date.today(), **self.params_all_defaults}
        resp = self.smart_get_200_auto_headers(**params)
        response_object = orjson.loads(b"".join(resp.streaming_content))
        self.assertEqual(len(response_object), 1)
        assert compare_dictionaries(response_object[0], self.full_response_dict)
    
    def test_summary_statistics_daily_all_fields_one_at_a_time(self):
        today = date.today()
        self.generate_summary_statistic_daily()
        cheat_sheet = self.default_summary_statistic_daily_cheatsheet()
        cheat_sheet["date"] = today.isoformat()
        cheat_sheet["participant_id"] = self.default_participant.patient_id
        cheat_sheet["study_id"] = self.session_study.object_id
        normal_params = self.params_all_defaults
        normal_params.pop("fields")
        for field in SERIALIZABLE_FIELD_NAMES:
            params = {"end_date": today, "start_date": today, "fields": field, **normal_params}
            resp = self.smart_get_200_auto_headers(**params)
            response_object = orjson.loads(b"".join(resp.streaming_content))
            self.assertEqual(len(response_object), 1)
            assert compare_dictionaries(response_object[0], {field: cheat_sheet[field]})
    
    def test_summary_statistics_daily_all_params_2_results_all_populated(self):
        self.generate_summary_statistic_daily()
        self.generate_summary_statistic_daily(a_date=self.yesterday)
        resp = self.smart_get_200_auto_headers(**self.params_all_defaults)
        response_object = orjson.loads(b"".join(resp.streaming_content))
        self.assertEqual(len(response_object), 2)
        compare_me = self.full_response_dict
        assert compare_dictionaries(response_object[0], compare_me)
        compare_me['date'] = self.yesterday.isoformat()
        assert compare_dictionaries(response_object[1], compare_me)
    
    def test_summary_statistics_daily_limit_param(self):
        self.generate_summary_statistic_daily()
        self.generate_summary_statistic_daily(a_date=self.yesterday)
        params = {"limit": 1, **self.params_all_defaults}
        resp = self.smart_get_200_auto_headers(**params)
        response_object = orjson.loads(b"".join(resp.streaming_content))
        self.assertEqual(len(response_object), 1)
        assert compare_dictionaries(response_object[0], self.full_response_dict)
    
    def test_summary_statistics_daily_date_ordering(self):
        self.generate_summary_statistic_daily()
        self.generate_summary_statistic_daily(a_date=self.yesterday)
        # the default ordering is ascending
        params = {"order_direction": "descending", **self.params_all_defaults}
        resp = self.smart_get_200_auto_headers(**params)
        response_object = orjson.loads(b"".join(resp.streaming_content))
        self.assertEqual(len(response_object), 2)
        compare_me = self.full_response_dict
        assert compare_dictionaries(response_object[0], compare_me)
        compare_me['date'] = self.yesterday.isoformat()  # set to yesterday
        assert compare_dictionaries(response_object[1], compare_me)
        
        # assert that ascending is correct
        params = {"order_direction": "ascending", **self.params_all_defaults}
        resp = self.smart_get_200_auto_headers(**params)
        response_object = orjson.loads(b"".join(resp.streaming_content))
        self.assertEqual(len(response_object), 2)
        assert compare_dictionaries(response_object[0], compare_me)
        compare_me['date'] = self.today.isoformat()  # revert to today
        assert compare_dictionaries(response_object[1], compare_me)
        
        # assert that empty ordering is the default
        params = {"order_direction": "", **self.params_all_defaults}
        resp = self.smart_get_200_auto_headers(**params)
        response_object = orjson.loads(b"".join(resp.streaming_content))
        self.assertEqual(len(response_object), 2)
        assert compare_dictionaries(response_object[0], compare_me)
        compare_me['date'] = self.yesterday.isoformat()  # set to yesterday
        assert compare_dictionaries(response_object[1], compare_me)
    
    def test_summary_statistics_daily_participant_ordering(self):
        self.generate_summary_statistic_daily()
        self.generate_summary_statistic_daily(participant=self.generate_participant(
            study=self.session_study, patient_id="22222222",
        ))
        # the default ordering is ascending
        params = {
            **self.params_all_defaults,
            # "order_direction": "ascending",
            "ordered_by": "participant_id",
            "participant_ids": self.default_participant.patient_id + ",22222222",
        }
        resp = self.smart_get_200_auto_headers(**params)
        response_object = orjson.loads(b"".join(resp.streaming_content))
        self.assertEqual(len(response_object), 2)
        compare_me = self.full_response_dict
        assert compare_dictionaries(response_object[1], compare_me)
        compare_me['participant_id'] = "22222222"  # set to participant 2
        assert compare_dictionaries(response_object[0], compare_me)
        
        params["order_direction"] = "descending"
        resp = self.smart_get_200_auto_headers(**params)
        response_object = orjson.loads(b"".join(resp.streaming_content))
        self.assertEqual(len(response_object), 2)
        assert compare_dictionaries(response_object[1], compare_me)
        compare_me['participant_id'] = self.default_participant.patient_id  # revert to participant 1
        assert compare_dictionaries(response_object[0], compare_me)
    
    def test_summary_statistics_daily_wrong_date(self):
        self.generate_summary_statistic_daily()
        params = self.params_all_defaults
        params["end_date"] = self.tomorrow
        params["start_date"] = self.tomorrow
        resp = self.smart_get_200_auto_headers(**params)
        response_object = orjson.loads(b"".join(resp.streaming_content))
        self.assertEqual(response_object, [])
    
    def test_summary_statistics_daily_wrong_future_date(self):
        self.generate_summary_statistic_daily()
        params = self.params_all_defaults
        params["end_date"] = self.tomorrow
        params["start_date"] = self.tomorrow
        resp = self.smart_get_200_auto_headers(**params)
        response_object = orjson.loads(b"".join(resp.streaming_content))
        self.assertEqual(response_object, [])
    
    def test_summary_statistics_daily_wrong_past_date(self):
        self.generate_summary_statistic_daily()
        params = self.params_all_defaults
        params["end_date"] = self.yesterday
        params["start_date"] = self.yesterday
        resp = self.smart_get_200_auto_headers(**params)
        response_object = orjson.loads(b"".join(resp.streaming_content))
        self.assertEqual(response_object, [])
    
    def test_summary_statistics_daily_bad_participant(self):
        self.generate_summary_statistic_daily()
        params = self.params_all_defaults
        params["participant_ids"] = "bad_id"
        resp = self.smart_get_200_auto_headers(**params)
        response_object = orjson.loads(b"".join(resp.streaming_content))
        self.assertEqual(response_object, [])
    
    def test_summary_statistics_daily_no_participant(self):
        self.generate_summary_statistic_daily()
        params = self.params_all_defaults
        params.pop("participant_ids")
        resp = self.smart_get_200_auto_headers(**params)
        response_object = orjson.loads(b"".join(resp.streaming_content))
        # self.assertEqual(response_object, [])
        assert compare_dictionaries(response_object[0], self.full_response_dict)


class TableauApiAuthTests(TableauAPITest):
    """ Test methods of the api authentication system """
    ENDPOINT_NAME = TableauAPITest.IGNORE_THIS_ENDPOINT
    
    def test_check_permissions_working(self):
        # if this doesn't raise an error it has succeeded
        check_tableau_permissions(self.default_header, study_object_id=self.session_study.object_id)
    
    def test_check_permissions_none(self):
        ApiKey.objects.all().delete()
        with self.assertRaises(TableauAuthenticationFailed) as cm:
            check_tableau_permissions(
                self.default_header, study_object_id=self.session_study.object_id
            )
    
    def test_deleted_study(self):
        self.session_study.update(deleted=True)
        with self.assertRaises(TableauPermissionDenied) as cm:
            check_tableau_permissions(
                self.default_header, study_object_id=self.session_study.object_id
            )
    
    def test_check_permissions_inactive(self):
        self.api_key.update(is_active=False)
        with self.assertRaises(TableauAuthenticationFailed) as cm:
            check_tableau_permissions(
                self.default_header, study_object_id=self.session_study.object_id
            )
    
    def test_check_permissions_bad_secret(self):
        # note that ':' does not appear in base64 encoding, preventing any collision errors based on
        # the current implementation.
        class NotRequest:
            headers = {
                X_ACCESS_KEY_ID: self.api_key_public,
                X_ACCESS_KEY_SECRET: ":::" + self.api_key_private[3:],
            }
        with self.assertRaises(TableauAuthenticationFailed) as cm:
            check_tableau_permissions(
                NotRequest, study_object_id=self.session_study.object_id
            )
    
    def test_check_permissions_forest_disabled(self):
        # forest_enabled should have no effect on the permissions check
        self.session_study.update(forest_enabled=False)
        check_tableau_permissions(self.default_header, study_object_id=self.session_study.object_id)
        self.session_study.update(forest_enabled=True)
        check_tableau_permissions(self.default_header, study_object_id=self.session_study.object_id)
    
    def test_check_permissions_bad_study(self):
        self.assertFalse(ApiKey.objects.filter(access_key_id=" bad study id ").exists())
        with self.assertRaises(TableauPermissionDenied) as cm:
            check_tableau_permissions(
                self.default_header, study_object_id=" bad study id "
            )
    
    def test_check_permissions_no_study_permission(self):
        StudyRelation.objects.filter(
            study=self.session_study, researcher=self.session_researcher).delete()
        with self.assertRaises(TableauPermissionDenied) as cm:
            check_tableau_permissions(
                self.default_header, study_object_id=self.session_study.object_id
            )


class TestWebDataConnector(SmartRequestsTestCase):
    ENDPOINT_NAME = "data_api_endpoints.web_data_connector"
    
    LOCAL_COPY_SERIALIZABLE_FIELD_NAMES = [
        # Metadata
        "date",
        "participant_id",
        "study_id",
        "timezone",
        
        # Data quantities
        "beiwe_accelerometer_bytes",
        "beiwe_ambient_audio_bytes",
        "beiwe_app_log_bytes",
        "beiwe_bluetooth_bytes",
        "beiwe_calls_bytes",
        "beiwe_devicemotion_bytes",
        "beiwe_gps_bytes",
        "beiwe_gyro_bytes",
        "beiwe_identifiers_bytes",
        "beiwe_ios_log_bytes",
        "beiwe_magnetometer_bytes",
        "beiwe_power_state_bytes",
        "beiwe_proximity_bytes",
        "beiwe_reachability_bytes",
        "beiwe_survey_answers_bytes",
        "beiwe_survey_timings_bytes",
        "beiwe_texts_bytes",
        "beiwe_audio_recordings_bytes",
        "beiwe_wifi_bytes",
        
        # GPS
        "jasmine_distance_diameter",
        "jasmine_distance_from_home",
        "jasmine_distance_traveled",
        "jasmine_flight_distance_average",
        "jasmine_flight_distance_stddev",
        "jasmine_flight_duration_average",
        "jasmine_flight_duration_stddev",
        "jasmine_gps_data_missing_duration",
        "jasmine_home_duration",
        "jasmine_gyration_radius",
        "jasmine_significant_location_count",
        "jasmine_significant_location_entropy",
        "jasmine_pause_time",
        "jasmine_obs_duration",
        "jasmine_obs_day",
        "jasmine_obs_night",
        "jasmine_total_flight_time",
        "jasmine_av_pause_duration",
        "jasmine_sd_pause_duration",
        
        # Willow, Texts
        "willow_incoming_text_count",
        "willow_incoming_text_degree",
        "willow_incoming_text_length",
        "willow_outgoing_text_count",
        "willow_outgoing_text_degree",
        "willow_outgoing_text_length",
        "willow_incoming_text_reciprocity",
        "willow_outgoing_text_reciprocity",
        "willow_outgoing_MMS_count",
        "willow_incoming_MMS_count",
        
        # Willow, Calls
        "willow_incoming_call_count",
        "willow_incoming_call_degree",
        "willow_incoming_call_duration",
        "willow_outgoing_call_count",
        "willow_outgoing_call_degree",
        "willow_outgoing_call_duration",
        "willow_missed_call_count",
        "willow_missed_callers",
        "willow_uniq_individual_call_or_text_count",
        
        # Sycamore, Survey Frequency
        "sycamore_total_surveys",
        "sycamore_total_completed_surveys",
        "sycamore_total_opened_surveys",
        "sycamore_average_time_to_submit",
        "sycamore_average_time_to_open",
        "sycamore_average_duration",
        
        # Oak, walking statistics
        "oak_walking_time",
        "oak_steps",
        "oak_cadence",
    ]
    
    # This is a very bad test. `content` is actually an html page (because tableau is strange)
    def test_page_content(self):
        resp = self.smart_get(self.session_study.object_id)
        content = resp.content.decode()
        
        # test that someone has updated this test if the fields ever change
        for field in self.LOCAL_COPY_SERIALIZABLE_FIELD_NAMES:
            self.assert_present(field, content)
    
    def test_all_fields_present_in_test(self):
        # sanity check that the fields are present in both copies of this list - yes you have to
        # update the copy of the list whenever you change the list.
        for field in self.LOCAL_COPY_SERIALIZABLE_FIELD_NAMES:
            self.assertIn(field, SERIALIZABLE_FIELD_NAMES)

# trunk-ignore-all(ruff/B018,bandit/B105)
from datetime import date, datetime, timedelta

import orjson
import time_machine
from dateutil.tz import UTC
from django.core.exceptions import ValidationError
from django.utils import timezone

from authentication.tableau_authentication import (check_tableau_permissions,
    TableauAuthenticationFailed, TableauPermissionDenied, X_ACCESS_KEY_ID, X_ACCESS_KEY_SECRET)
from constants.common_constants import EST
from constants.forest_constants import DATA_QUANTITY_FIELD_NAMES, SERIALIZABLE_FIELD_NAMES
from constants.message_strings import MESSAGE_SEND_SUCCESS, MISSING_JSON_CSV_MESSAGE
from constants.schedule_constants import ScheduleTypes
from constants.testing_constants import MONDAY_JAN_10_NOON_2022_EST
from constants.user_constants import ANDROID_API, ResearcherRole, TABLEAU_TABLE_FIELD_TYPES
from database.forest_models import SummaryStatisticDaily
from database.models import ArchivedEvent
from database.profiling_models import UploadTracking
from database.security_models import ApiKey
from database.study_models import Study
from database.survey_models import Survey, SurveyArchive
from database.user_models_participant import AppHeartbeats, AppVersionHistory
from database.user_models_researcher import StudyRelation
from libs.utils.compression import compress
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
    
    def data(self) -> tuple[list[str], list[str|None], list[str|None]]:
        keys_row = self.header().strip().split(",")
        unfilled_row = [  # wow ok these suck but can't really be factored the test being meaningless...
            "2020-01-01",   # Created On
            "patient1",     # Patient ID
            "Inactive",     # Status
            "ANDROID",      # OS Type
            None,           # First Registration Date
            None,           # Last Registration
            None,           # Last Timezone
            None,           # Last Upload
            None,           # Last Survey Download
            None,           # Last Set Password
            None,           # Last Push Token Update
            None,           # Last Device Settings Update
            None,           # Last OS Version
            None,           # App Version Code
            None,           # App Version Name
            None,           # Last Heartbeat
        ]
        filled_row = [  # cannot be factored out, different date representations
            "2020-01-01",                    # Created On
            "patient1",                      # Patient ID
            "Inactive",                      # Status
            "ANDROID",                       # OS Type
            None,                            # First Registration Date
            "2020-01-04T12:00:00-05:00",     # Last Registration
            "America/New_York",              # Last Timezone
            "2020-01-02T12:00:00-05:00",     # Last Upload
            "2020-01-03T12:00:00-05:00",     # Last Survey Download
            "2020-01-05T12:00:00-05:00",     # Last Set Password
            "2020-01-06T12:00:00-05:00",     # Last Push Token Update
            "2020-01-07T12:00:00-05:00",     # Last Device Settings Update
            "1.0",                           # Last OS Version
            "6",                             # App Version Code
            "six",                           # App Version Name
            "2020-01-08T12:00:00-05:00",     # Last Heartbeat
        ]
        self.assertEqual(len(filled_row), len(keys_row), "yo these need to be the same size")
        self.assertEqual(len(unfilled_row), len(keys_row), "yo these need to be the same size")
        self.assertEqual(len(filled_row), len(unfilled_row), "yo these need to be the same size")
        return keys_row, unfilled_row, filled_row
    
    def test_one_participant_csv(self):
        resp = self._do_one_participant("csv")
        self.modify_participant()
        header_row, _, _ = self.data()
        
        csv_values = [  # has a different DT format
            '2020-01-01',                     # Created On
            'patient1',                       # Patient ID
            'Inactive',                       # Status
            'ANDROID',                        # OS Type
            'None',                           # First Registration
            '2020-01-04 12:00:00 (EST)',      # Last Registration
            'America/New_York',               # Last Timezone
            '2020-01-02 12:00:00 (EST)',      # Last Upload
            '2020-01-03 12:00:00 (EST)',      # Last Survey Dowload
            '2020-01-05 12:00:00 (EST)',      # Last Set password
            '2020-01-06 12:00:00 (EST)',      # Last Push Token
            '2020-01-07 12:00:00 (EST)',      # Last Device Settings
            '1.0',                            # Last OS Version
            '6',                              # App Version Cod
            'six',                            # App Version Nam
            '2020-01-08 12:00:00 (EST)',      # Last Heartbeat
        ]
        self.assertEqual(len(csv_values), len(header_row), "needs to be the same size too")
        # convert to bytes
        header_bytes = ",".join(header_row).encode() + b"\r\n"
        row_bytes = ",".join(csv_values).encode() + b"\r\n"
        correct = header_bytes + row_bytes        
        
        resp = self._do_one_participant("csv")
        self.assertEqual(resp.content, correct)    
    
    def test_one_participant_json(self):
        self.default_participant.update_only(unknown_timezone=True)
        resp = self._do_one_participant("json")
        # keys = self.header().strip().split(",")  # strip() strips \r\n
        keys_row, unfilled_row, filled_row = self.data()
        row = dict(zip(keys_row, unfilled_row))
        self.assertEqual(orjson.loads(resp.content), [row])
        
        self.modify_participant()
        resp = self._do_one_participant("json")
        row = dict(zip(keys_row, filled_row))
        self.assertEqual(orjson.loads(resp.content), [row])
    
    def test_one_participant_json_table(self):
        self.default_participant.update_only(unknown_timezone=True)
        resp = self._do_one_participant("json_table")
        keys_row, unfilled_row, filled_row = self.data()
        self.assertEqual(orjson.loads(resp.content), [keys_row, unfilled_row])
        
        self.modify_participant()
        resp = self._do_one_participant("json_table")
        self.assertEqual(orjson.loads(resp.content), [keys_row, filled_row])
    
    def _do_one_participant(self, data_format: str):
        if not hasattr(self, "_default_study_relation"):
            self.set_session_study_relation(ResearcherRole.researcher)
        self.using_default_participant()
        self.default_participant.update_only(created_on=datetime(2020, 1, 1, 12, tzinfo=UTC))
        return self.smart_post_status_code(200, study_id=self.session_study.object_id, data_format=data_format)
    
    def modify_participant(self):
        # you may have to update this list if you add fields to EXTRA_TABLE_FIELDS...
        some_column_names_and_values = [
            ("created_on", datetime(2020, 1, 1, 12, tzinfo=EST)),  # not an extra row
            ("last_upload", datetime(2020, 1, 2, 12, tzinfo=EST)),
            ("last_get_latest_surveys", datetime(2020, 1, 3, 12, tzinfo=EST)),
            ("last_register_user", datetime(2020, 1, 4, 12, tzinfo=EST)),
            ("last_set_password", datetime(2020, 1, 5, 12, tzinfo=EST)),
            ("last_set_fcm_token", datetime(2020, 1, 6, 12, tzinfo=EST)),
            ("last_get_latest_device_settings", datetime(2020, 1, 7, 12, tzinfo=EST)),
            ("last_os_version", "1.0"),
            ("last_version_code", "6"),
            ("last_version_name", "six"),
            ("last_heartbeat_checkin", datetime(2020, 1, 8, 12, tzinfo=EST)),
            ("unknown_timezone", False),
        ]
        for name, value in some_column_names_and_values:
            setattr(self.default_participant, name, value)
        self.default_participant.save()


class TestDownloadParticipantTableDataTableau(TableauAPITest, ParticipantTableHelperMixin):
    ENDPOINT_NAME = "data_api_endpoints.get_tableau_participant_table_data"
    
    # We real tests are in the participant api test, this just checks that it works and has
    # normal json output.
    
    def test_no_access(self):
        self.smart_get_status_code(400, self.session_study.object_id)
    
    def test_access(self):
        resp = self.smart_get_status_code(200, self.session_study.object_id, **self.raw_headers)
        self.assertEqual(resp.content, b"[]")  # no participants literally an empty list
    
    @time_machine.travel("2020-01-01 12:00:00 UTC")
    def test_one_participant(self):
        self.default_study_field
        self.default_intervention
        self.default_participant_field_value
        self.default_populated_intervention_date
        now = timezone.now()
        now_str = "2020-01-01T07:00:00"  # no timezone info, but in study timezone time
        self.default_participant.update(
            created_on=now,
            first_register_user=now,
            last_set_fcm_token=now,
            last_get_latest_surveys=now,
            last_register_user=now,
            last_set_password=now,
            last_get_latest_device_settings=now,
            last_version_code="1.0",
            last_version_name="6",
            last_os_version=ANDROID_API,
            last_heartbeat_checkin=now,
            last_upload=now,
        )
        
        resp = self.smart_get_status_code(200, self.session_study.object_id, **self.raw_headers)
        
        # this one has yet another very specific time format because tableau is bad
        thing = [{
            "created_on": "2020-01-01",
            "patient_id": self.default_participant.patient_id,
            "status": "Active (just now)",
            "os_type": "ANDROID",
            "default_intervention_name": "2020-01-01",
            "default_study_field_name": "default_study_field_value",
            "first_registration_date": now_str,
            "last_registration": now_str,
            "last_timezone": "America/New_York",
            "last_upload": now_str,
            "last_survey_download": now_str,
            "last_set_password": now_str,
            "last_push_token_update": now_str,
            "last_device_settings_update": now_str,
            "last_os_version": ANDROID_API,
            "app_version_code": "1.0",
            "app_version_name": "6",
            "last_heartbeat": now_str,
        }]
        self.assertEqual(resp.content, orjson.dumps(thing))


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
        
        zslug = compress(slug)
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


class TestGetParticipantNotificationHistory(DataApiTest):
    ENDPOINT_NAME = "data_api_endpoints.get_participant_notification_history"
    
    def test_no_participant_parameter(self):
        # it should 400
        self.set_session_study_relation(ResearcherRole.researcher)
        self.using_default_participant()
        resp = self.smart_post_status_code(400)
        self.assertEqual(resp.content, b"")
    
    def test_participant_no_notification_history(self):
        self.set_session_study_relation(ResearcherRole.researcher)
        resp = self.smart_post_status_code(200, participant_id=self.default_participant.patient_id)
        self.assertEqual(resp.content, b"{}")
    
    def build_archived_event(self, participant=None):
        p = participant or self.default_participant
        archived_event_params = dict(
            survey_archive=self.default_survey.archives.first(),
            participant=p,
            schedule_type=ScheduleTypes.weekly,
            scheduled_time=MONDAY_JAN_10_NOON_2022_EST,
            status=MESSAGE_SEND_SUCCESS,
            uuid='9ae6981c-08ac-4179-b385-be068e390054',
            was_resend=False,
            confirmed_received=True,
        )
        # test that we had all the fields specified
        self.assertEqual(
            set(archived_event_params), {
                field.name
                for field in ArchivedEvent()._meta.fields
                if field.name not in ("id", "created_on", "last_updated")
            }
        )
        
        a = ArchivedEvent(**archived_event_params)
        a.save()  # run validation
        a.force_update_only(
            created_on=MONDAY_JAN_10_NOON_2022_EST, last_updated=MONDAY_JAN_10_NOON_2022_EST,
        )
        return a
    
    @property
    def comparator_params_EST(self):
        return {
            'u1Z3SH7l2xNsw72hN3LnYi96':
                [
                    {
                        'confirmed_received': False,
                        'scheduled_time': '2022-01-10T12:00:00-05:00',
                        'timestamp': '2022-01-10T12:00:00-05:00',
                        'type': 'weekly',
                        'uuid': '9ae6981c-08ac-4179-b385-be068e390054',
                        'push_rejected': False,
                        'resend': False
                    }
                ]
        }
    
    @property
    def comparator_params_UTC(self):
        return {
            'u1Z3SH7l2xNsw72hN3LnYi96':
                [
                    {
                        'confirmed_received': False,
                        'scheduled_time': '2022-01-10T17:00:00Z',
                        'timestamp': '2022-01-10T17:00:00Z',
                        'type': 'weekly',
                        'uuid': '9ae6981c-08ac-4179-b385-be068e390054',
                        'push_rejected': False,
                        'resend': False
                    }
                ]
        }
    
    @staticmethod
    def merge_em(d1: dict, d2: dict):
        d1 = d1["u1Z3SH7l2xNsw72hN3LnYi96"][0]
        d2 = d2["u1Z3SH7l2xNsw72hN3LnYi96"][0]
        return {"u1Z3SH7l2xNsw72hN3LnYi96": [d1, d2]}
    
    def test_participant_one_notification_history_ET(self):
        self.set_session_study_relation(ResearcherRole.researcher)
        a = self.build_archived_event()
        resp = self.smart_post_status_code(200, participant_id=self.default_participant.patient_id)
        self.assertEqual(orjson.loads(resp.content), self.comparator_params_EST)
    
    def test_participant_one_notification_history_UTC(self):
        self.set_session_study_relation(ResearcherRole.researcher)
        a = self.build_archived_event()
        resp = self.smart_post_status_code(200, participant_id=self.default_participant.patient_id, utc=True)
        self.assertEqual(orjson.loads(resp.content), self.comparator_params_UTC)
    
    def test_one_participant_two_notifications(self):
        self.set_session_study_relation(ResearcherRole.researcher)
        a = self.build_archived_event()
        a2 = self.build_archived_event()
        resp = self.smart_post_status_code(200, participant_id=self.default_participant.patient_id)
        self.assertDictEqual(
            orjson.loads(resp.content),
            self.merge_em(self.comparator_params_EST, self.comparator_params_EST)
        )
    
    def test_one_participant_two_notifications_sort(self):
        self.set_session_study_relation(ResearcherRole.researcher)
        a = self.build_archived_event()
        a2 = self.build_archived_event()
        resp = self.smart_post_status_code(200, participant_id=self.default_participant.patient_id)
        a2.force_update_only(scheduled_time=a2.scheduled_time - timedelta(days=1))
        d2 = self.comparator_params_EST
        d2["scheduled_time"] = '2022-01-09T12:00:00-05:00'
        self.assertDictEqual(
            orjson.loads(resp.content),
            self.merge_em(d2, self.comparator_params_EST)
        )
    
    def test_two_participants_each_with_one_notification(self):
        a = self.build_archived_event()
        b = self.build_archived_event(participant=self.generate_participant(self.default_study))
        self.set_session_study_relation(ResearcherRole.researcher)
        resp = self.smart_post_status_code(200, participant_id=self.default_participant.patient_id)
        self.assertDictEqual(orjson.loads(resp.content), self.comparator_params_EST)

#
## Tableau API
#

class TestGetTableauDaily(TableauAPITest):
    ENDPOINT_NAME = "data_api_endpoints.get_tableau_summary_statistics"
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
        defaults = SummaryStatisticDaily.default_summary_statistic_daily_cheatsheet()
        defaults["date"] = date.today().isoformat()
        defaults["participant_id"] = self.default_participant.patient_id
        defaults["study_id"] = self.session_study.object_id
        return defaults
    
    def smart_get_200_auto_headers(self, **kwargs):
        return self.smart_get_status_code(
            200, self.session_study.object_id, data=kwargs, **self.raw_headers
        )
    
    def test_tableau_api_credential_upgrade(self, **kwargs):
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
        cheat_sheet = SummaryStatisticDaily.default_summary_statistic_daily_cheatsheet()
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


class TestWebDataConnectorSummaryStatistics(SmartRequestsTestCase):
    ENDPOINT_NAME = "data_api_endpoints.web_data_connector_summary_statistics"
    
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


class TestWebDataConnectorParticipantTable(SmartRequestsTestCase):
    ENDPOINT_NAME = "data_api_endpoints.web_data_connector_participant_table"
    
    LOCAL_COPY_FIELD_NAMES = [
        "{id: 'created_on', dataType: tableau.dataTypeEnum.date,},",
        "{id: 'patient_id', dataType: tableau.dataTypeEnum.string,},",
        "{id: 'status', dataType: tableau.dataTypeEnum.string,},",
        "{id: 'os_type', dataType: tableau.dataTypeEnum.string,},",
        "{id: 'default_intervention_name', dataType: tableau.dataTypeEnum.string,},",  # optional
        "{id: 'default_study_field_name', dataType: tableau.dataTypeEnum.string,},",  # optional
        "{id: 'first_registration_date', dataType: tableau.dataTypeEnum.datetime,},",
        "{id: 'last_registration', dataType: tableau.dataTypeEnum.datetime,},",
        "{id: 'last_timezone', dataType: tableau.dataTypeEnum.string,},",
        "{id: 'last_upload', dataType: tableau.dataTypeEnum.datetime,},",
        "{id: 'last_survey_download', dataType: tableau.dataTypeEnum.datetime,},",
        "{id: 'last_set_password', dataType: tableau.dataTypeEnum.datetime,},",
        "{id: 'last_push_token_update', dataType: tableau.dataTypeEnum.datetime,},",
        "{id: 'last_device_settings_update', dataType: tableau.dataTypeEnum.datetime,},",
        "{id: 'last_os_version', dataType: tableau.dataTypeEnum.string,},",
        "{id: 'app_version_code', dataType: tableau.dataTypeEnum.string,},",
        "{id: 'app_version_name', dataType: tableau.dataTypeEnum.string,},",
        "{id: 'last_heartbeat', dataType: tableau.dataTypeEnum.datetime,},",
    ]
    
    def test_columns_copy(self):
        # test manual
        assert "created_on" in self.LOCAL_COPY_FIELD_NAMES[0]
        assert "patient_id" in self.LOCAL_COPY_FIELD_NAMES[1]
        assert "status" in self.LOCAL_COPY_FIELD_NAMES[2]
        assert "os_type" in self.LOCAL_COPY_FIELD_NAMES[3]
        assert "default_intervention_name" in self.LOCAL_COPY_FIELD_NAMES[4]  # optional
        assert "default_study_field_name" in self.LOCAL_COPY_FIELD_NAMES[5]  # optional
        assert "first_registration_date" in self.LOCAL_COPY_FIELD_NAMES[6]  # optional
        assert "last_registration" in self.LOCAL_COPY_FIELD_NAMES[7]  # optional
        assert "last_timezone" in self.LOCAL_COPY_FIELD_NAMES[8]
        assert "last_upload" in self.LOCAL_COPY_FIELD_NAMES[1+8]
        assert "last_survey_download" in self.LOCAL_COPY_FIELD_NAMES[1+9]
        assert "last_set_password" in self.LOCAL_COPY_FIELD_NAMES[1+10]
        assert "last_push_token_update" in self.LOCAL_COPY_FIELD_NAMES[1+11]
        assert "last_device_settings_update" in self.LOCAL_COPY_FIELD_NAMES[1+12]
        assert "last_os_version" in self.LOCAL_COPY_FIELD_NAMES[1+13]
        assert "app_version_code" in self.LOCAL_COPY_FIELD_NAMES[1+14]
        assert "app_version_name" in self.LOCAL_COPY_FIELD_NAMES[1+15]
        assert "last_heartbeat" in self.LOCAL_COPY_FIELD_NAMES[1+16]
        
        # test that is fairly obvious to save debugging time, 2 custom fields
        assert len(self.LOCAL_COPY_FIELD_NAMES) - 2 == len(TABLEAU_TABLE_FIELD_TYPES)
        
        local_keys = list(TABLEAU_TABLE_FIELD_TYPES)
        local_keys.insert(4, "default_intervention_name")
        local_keys.insert(5, "default_study_field_name")
        
        # test that the things actually align
        for line_number, key in enumerate(local_keys):
            self.assertIn(key, self.LOCAL_COPY_FIELD_NAMES[line_number])
    
    def test_page_content(self):
        self.default_participant
        self.default_study_field
        self.default_intervention
        resp = self.smart_get(self.session_study.object_id)
        content = resp.content.decode()
        
        # test that someone has updated this test if the fields ever change
        for field in self.LOCAL_COPY_FIELD_NAMES:
            self.assert_present(field, content)

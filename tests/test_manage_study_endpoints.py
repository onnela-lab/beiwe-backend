# trunk-ignore-all(ruff/B018)
from datetime import datetime, timedelta
from typing import Tuple

import orjson
from django.utils import timezone

from constants.common_constants import API_DATE_FORMAT
from constants.user_constants import ResearcherRole
from database.schedule_models import Intervention
from database.study_models import StudyField
from database.user_models_participant import Participant
from tests.common import ResearcherSessionTest
from tests.helpers import ParticipantTableHelperMixin


class TestToggleStudyEasyEnrollment(ResearcherSessionTest):
    ENDPOINT_NAME = "manage_study_endpoints.toggle_easy_enrollment_study"
    REDIRECT_ENDPOINT_NAME = "study_endpoints.edit_study"
    
    def test_site_admin(self):
        self.set_session_study_relation(ResearcherRole.site_admin)
        self._test_success()
    
    def test_study_admin(self):
        self.set_session_study_relation(ResearcherRole.study_admin)
        self._test_success()
    
    def _test_success(self):
        self.assertFalse(self.default_study.easy_enrollment)
        self.smart_get_redirect(self.session_study.id)
        self.default_study.refresh_from_db()
        self.assertTrue(self.default_study.easy_enrollment)
    
    def test_researcher(self):
        self.set_session_study_relation(ResearcherRole.researcher)
        self._test_fail()
    
    def test_no_relation(self):
        self.session_researcher.study_relations.all().delete()  # should be redundant
        self._test_fail()
    
    def _test_fail(self):
        self.assertFalse(self.default_study.easy_enrollment)
        self.easy_get(self.ENDPOINT_NAME, status_code=403, study_id=self.session_study.id).content
        self.default_study.refresh_from_db()
        self.assertFalse(self.default_study.easy_enrollment)


# fixme: add user type tests
class TestRenameStudy(ResearcherSessionTest):
    ENDPOINT_NAME = "manage_study_endpoints.rename_study"
    REDIRECT_ENDPOINT_NAME = "study_endpoints.edit_study"
    
    def test(self):
        self.set_session_study_relation(ResearcherRole.site_admin)
        self.smart_post_redirect(self.session_study.id, new_study_name="hello!")
        self.session_study.refresh_from_db()
        self.assertEqual(self.session_study.name, "hello!")


# FIXME: add error cases to this test
class TestSetStudyTimezone(ResearcherSessionTest):
    ENDPOINT_NAME = "manage_study_endpoints.set_study_timezone"
    REDIRECT_ENDPOINT_NAME = "study_endpoints.edit_study"
    
    def test_study_admin(self):
        self.set_session_study_relation(ResearcherRole.study_admin)
        self._test_success()
    
    def test_site_admin(self):
        self.set_session_study_relation(ResearcherRole.site_admin)
        self._test_success()
    
    def _test_success(self):
        self.smart_post(self.session_study.id, new_timezone_name="Pacific/Noumea")
        self.session_study.refresh_from_db()
        self.assertEqual(self.session_study.timezone_name, "Pacific/Noumea")


# FIXME: implement this test beyond "it doesn't crash"
class TestStudyParticipantApi(ResearcherSessionTest):
    ENDPOINT_NAME = "manage_study_endpoints.study_participants_api"
    
    COLUMN_ORDER_KEY = "order[0][column]"
    ORDER_DIRECTION_KEY = "order[0][dir]"
    SEARCH_PARAMETER = "search[value]"
    SOME_TIMESTAMP = timezone.make_aware(datetime(2020, 10, 1))
    SOME_TIMESTAMP_STR = SOME_TIMESTAMP.strftime(API_DATE_FORMAT)
    
    THE_STATUS_FIELD_NAMES = [
        "last_get_latest_surveys",
        "last_upload",
        "last_register_user",
        "last_set_password",
        "last_set_fcm_token",
        "last_get_latest_device_settings",
    ]
    
    # This endpoint is stupidly complex, it implements pagination, sorting, search ordering.
    
    def setUp(self) -> None:
        # we need a flag for multiple calls to set_session_study_relation
        ret = super().setUp()
        self.default_study.update(timezone_name="UTC")  # wow this actually matters
        self.STUDY_RELATION_SET = False
        return ret
    
    @property
    def DEFAULT_PARAMETERS(self):
        # you need to be at least a researcher, factor out this clutter
        if not self.STUDY_RELATION_SET:
            self.set_session_study_relation(ResearcherRole.researcher)
            self.STUDY_RELATION_SET = True
        return {
            "draw":
                1,
            "start":
                0,
            "length":
                10,
            # sort, sort order, search term.  order key is index into this list, larger values
            # target first interventions then custom fields:
            # ['created_on', 'patient_id', 'registered', 'os_type']
            self.COLUMN_ORDER_KEY:
                0,
            self.ORDER_DIRECTION_KEY:
                "asc",
            self.SEARCH_PARAMETER:
                "",
        }
    
    def CONSTRUCT_RESPONSE(self, status: str):
        return {
            "draw":
                1,
            "recordsTotal":
                1,
            "recordsFiltered":
                1,
            "data":
                [[
                    self.SOME_TIMESTAMP_STR,
                    "",  # first_register_user
                    self.default_participant.patient_id,
                    status,
                    "ANDROID"
                ]]
        }
    
    def test_basics(self):
        # manually set the created on timestamp... its a pain to set and a pain to test.
        self.default_participant.update_only(created_on=self.SOME_TIMESTAMP)
        # this endpoint uses get args, for which we have to pass in the dict as the "data" kwarg
        resp = self.smart_post_status_code(200, self.session_study.id, **self.DEFAULT_PARAMETERS)
        content = orjson.loads(resp.content)
        # this participant has never contacted the server, but it does have a device id.
        self.assertEqual(content, self.CONSTRUCT_RESPONSE("Inactive"))
    
    def test_not_logged_in_1(self):
        self.default_participant.update_only(created_on=self.SOME_TIMESTAMP)
        self.session_researcher.force_global_logout()
        resp = self.smart_post_status_code(200, self.session_study.id, **self.DEFAULT_PARAMETERS)
        self.assertEqual(resp.content, b'[]')
    
    def test_not_logged_in_2(self):
        self.default_participant.update_only(created_on=self.SOME_TIMESTAMP)
        self.do_researcher_logout()
        resp = self.smart_post_status_code(200, self.session_study.id, **self.DEFAULT_PARAMETERS)
        self.assertEqual(resp.content, b'[]')
    
    def test_404_no_matching_study(self):
        self.default_participant.update_only(created_on=self.SOME_TIMESTAMP)
        resp = self.smart_post_status_code(404, 0)
        self.assertEqual(resp.content, b'')
    
    def test_400_illegal_study_id(self):
        self.default_participant.update_only(created_on=self.SOME_TIMESTAMP)
        resp = self.smart_post_status_code(400, "jeff")
        self.assertEqual(resp.content, b'')
    
    def test_no_study_relation(self):
        self.default_participant.update_only(created_on=self.SOME_TIMESTAMP)
        self.session_researcher.study_relations.all().delete()
        resp = self.smart_post_status_code(403, self.session_study.id)
        self.assertEqual(resp.content, b'')
    
    def test_various_statuses(self):
        # this test tests all the timestamp fields that are used to determine the status of a participant,
        # which is displayed on the status column of the participants table.
        self.default_participant.update_only(created_on=self.SOME_TIMESTAMP)
        
        # That triple **:
        # For every field name - set all of them to null, but override the first dict ** with a
        # second dict ** that forces the status_field_name to the current/appropriate time. (You
        # can't ** two dicts with overlapping keys as function parameters directly, you get a
        # "multiple values for keyword argument" error - which is a TypeError for some reason), but
        # you Can that into an inline dictionary, and then ** that dictionary into the function
        # parameters.
        fields_as_nones = {field: None for field in self.THE_STATUS_FIELD_NAMES}
        t = timezone.now()
        for status_field_name in self.THE_STATUS_FIELD_NAMES:
            self.default_participant.update_only(**{**fields_as_nones, **{status_field_name: t}})
            resp = self.smart_post_status_code(
                200, self.session_study.id, **self.DEFAULT_PARAMETERS
            )
            self.assertDictEqual(
                orjson.loads(resp.content), self.CONSTRUCT_RESPONSE("Active (just now)")
            )
        
        t = timezone.now() - timedelta(minutes=6)
        for status_field_name in self.THE_STATUS_FIELD_NAMES:
            self.default_participant.update_only(**{**fields_as_nones, **{status_field_name: t}})
            resp = self.smart_post_status_code(
                200, self.session_study.id, **self.DEFAULT_PARAMETERS
            )
            self.assertDictEqual(
                orjson.loads(resp.content), self.CONSTRUCT_RESPONSE("Active (last hour)")
            )
        
        t = timezone.now() - timedelta(hours=2)
        for status_field_name in self.THE_STATUS_FIELD_NAMES:
            self.default_participant.update_only(**{**fields_as_nones, **{status_field_name: t}})
            resp = self.smart_post_status_code(
                200, self.session_study.id, **self.DEFAULT_PARAMETERS
            )
            self.assertDictEqual(
                orjson.loads(resp.content), self.CONSTRUCT_RESPONSE("Active (past day)")
            )
        
        t = timezone.now() - timedelta(days=2)
        for status_field_name in self.THE_STATUS_FIELD_NAMES:
            self.default_participant.update_only(**{**fields_as_nones, **{status_field_name: t}})
            resp = self.smart_post_status_code(
                200, self.session_study.id, **self.DEFAULT_PARAMETERS
            )
            self.assertDictEqual(
                orjson.loads(resp.content), self.CONSTRUCT_RESPONSE("Active (past week)")
            )
    
    def test_with_intervention(self):
        self.default_participant.update_only(created_on=self.SOME_TIMESTAMP)
        # need to populate some database state, this database stat is expected to be populated when
        # a participant is created and/or when an intervention is created.
        self.default_intervention
        self.default_populated_intervention_date
        resp = self.smart_post_status_code(200, self.session_study.id, **self.DEFAULT_PARAMETERS)
        content = orjson.loads(resp.content)
        correct_content = self.CONSTRUCT_RESPONSE("Inactive")
        correct_content["data"][0].append(
            self.CURRENT_DATE.strftime(API_DATE_FORMAT)
        )  # the value populated in the intervention date
        self.assertEqual(content, correct_content)
    
    def test_with_custom_field(self):
        self.default_participant.update_only(created_on=self.SOME_TIMESTAMP)
        self.default_participant_field_value  # populate database state
        resp = self.smart_post_status_code(200, self.session_study.id, **self.DEFAULT_PARAMETERS)
        content = orjson.loads(resp.content)
        correct_content = self.CONSTRUCT_RESPONSE("Inactive")
        correct_content["data"][0].append(self.DEFAULT_PARTICIPANT_FIELD_VALUE)  # default value
        self.assertEqual(content, correct_content)
    
    def test_with_both(self):
        self.default_participant.update_only(created_on=self.SOME_TIMESTAMP)
        self.default_intervention  # populate database state
        self.default_populated_intervention_date
        self.default_participant_field_value
        resp = self.smart_post_status_code(200, self.session_study.id, **self.DEFAULT_PARAMETERS)
        content = orjson.loads(resp.content)
        correct_content = self.CONSTRUCT_RESPONSE("Inactive")
        correct_content["data"][0].append(self.CURRENT_DATE.strftime(API_DATE_FORMAT))
        correct_content["data"][0].append(self.DEFAULT_PARTICIPANT_FIELD_VALUE)
        self.assertEqual(content, correct_content)
    
    def test_simple_ordering(self):
        # setup default participant
        self.default_participant.update_only(created_on=self.SOME_TIMESTAMP)
        self.default_intervention
        self.default_populated_intervention_date
        self.default_participant_field_value
        # setup second participant
        p2 = self.generate_participant(self.session_study, "patient2")
        p2.update_only(created_on=self.SOME_TIMESTAMP + timedelta(days=1))  # for sorting
        self.generate_intervention_date(
            p2, self.default_intervention, None
        )  # correct db population
        # construct the correct response data (yuck)
        correct_content = self.CONSTRUCT_RESPONSE("Inactive")
        correct_content["recordsTotal"] = 2
        correct_content["recordsFiltered"] = 2
        correct_content["data"][0].append(self.CURRENT_DATE.strftime(API_DATE_FORMAT))
        correct_content["data"][0].append(self.DEFAULT_PARTICIPANT_FIELD_VALUE)
        # created on, patient id, registered, os_type, intervention date, custom field
        # (registered is based on presence of os_type)
        correct_content["data"].append(
            [p2.created_on.strftime(API_DATE_FORMAT), "", p2.patient_id, "Inactive", "ANDROID", "", ""]
        )
        # request, compare
        params = self.DEFAULT_PARAMETERS
        resp = self.smart_post_status_code(200, self.session_study.id, **params)
        content = orjson.loads(resp.content)
        self.assertEqual(content, correct_content)
        # reverse the order
        params[self.ORDER_DIRECTION_KEY] = "desc"
        correct_content["data"].append(correct_content["data"].pop(0))  # swap 2 rows
        resp = self.smart_post_status_code(200, self.session_study.id, **params)
        content = orjson.loads(resp.content)
        self.assertEqual(content, correct_content)


class TestInterventionsPage(ResearcherSessionTest):
    ENDPOINT_NAME = "manage_study_endpoints.interventions_page"
    REDIRECT_ENDPOINT_NAME = "manage_study_endpoints.interventions_page"
    
    def test_get(self):
        self.set_session_study_relation(ResearcherRole.study_admin)
        self.generate_intervention(self.session_study, "obscure_name_of_intervention")
        resp = self.smart_get(self.session_study.id)
        self.assert_present("obscure_name_of_intervention", resp.content)
    
    def test_post(self):
        self.set_session_study_relation(ResearcherRole.study_admin)
        resp = self.smart_post(self.session_study.id, new_intervention="ohello")
        self.assertEqual(resp.status_code, 302)
        intervention = Intervention.objects.get(study=self.session_study)
        self.assertEqual(intervention.name, "ohello")


class TestDeleteIntervention(ResearcherSessionTest):
    ENDPOINT_NAME = "manage_study_endpoints.delete_intervention"
    REDIRECT_ENDPOINT_NAME = "manage_study_endpoints.interventions_page"
    
    def test(self):
        self.set_session_study_relation(ResearcherRole.study_admin)
        intervention = self.generate_intervention(
            self.session_study, "obscure_name_of_intervention"
        )
        self.smart_post_redirect(self.session_study.id, intervention=intervention.id)
        self.assertFalse(Intervention.objects.filter(id=intervention.id).exists())


class TestEditIntervention(ResearcherSessionTest):
    ENDPOINT_NAME = "manage_study_endpoints.edit_intervention"
    REDIRECT_ENDPOINT_NAME = "manage_study_endpoints.interventions_page"
    
    def test(self):
        self.set_session_study_relation(ResearcherRole.study_admin)
        intervention = self.generate_intervention(
            self.session_study, "obscure_name_of_intervention"
        )
        self.smart_post_redirect(
            self.session_study.id, intervention_id=intervention.id, edit_intervention="new_name"
        )
        intervention_new = Intervention.objects.get(id=intervention.id)
        self.assertEqual(intervention.id, intervention_new.id)
        self.assertEqual(intervention_new.name, "new_name")


class TestStudyFields(ResearcherSessionTest):
    ENDPOINT_NAME = "manage_study_endpoints.study_fields"
    REDIRECT_ENDPOINT_NAME = "manage_study_endpoints.study_fields"
    
    def test_get(self):
        self.set_session_study_relation(ResearcherRole.study_admin)
        self.generate_study_field(self.session_study, "obscure_name_of_study_field")
        # This isn't a pure redirect endpoint, we need get to have a 200
        resp = self.smart_get(self.session_study.id)
        self.assertEqual(resp.status_code, 200)
        self.assert_present("obscure_name_of_study_field", resp.content)
    
    def test_post(self):
        self.set_session_study_relation(ResearcherRole.study_admin)
        resp = self.smart_post_redirect(self.session_study.id, new_field="ohello")
        self.assertEqual(resp.status_code, 302)
        study_field = StudyField.objects.get(study=self.session_study)
        self.assertEqual(study_field.field_name, "ohello")


class TestDeleteStudyField(ResearcherSessionTest):
    ENDPOINT_NAME = "manage_study_endpoints.delete_field"
    REDIRECT_ENDPOINT_NAME = "manage_study_endpoints.study_fields"
    
    def test(self):
        self.set_session_study_relation(ResearcherRole.study_admin)
        study_field = self.generate_study_field(self.session_study, "obscure_name_of_study_field")
        self.smart_post_redirect(self.session_study.id, field=study_field.id)
        self.assertFalse(StudyField.objects.filter(id=study_field.id).exists())


class TestEditStudyField(ResearcherSessionTest):
    ENDPOINT_NAME = "manage_study_endpoints.edit_custom_field"
    REDIRECT_ENDPOINT_NAME = "manage_study_endpoints.study_fields"
    
    def test(self):
        self.set_session_study_relation(ResearcherRole.study_admin)
        study_field = self.generate_study_field(self.session_study, "obscure_name_of_study_field")
        self.smart_post_redirect(
            self.session_study.id, field_id=study_field.id, edit_custom_field="new_name"
        )
        study_field_new = StudyField.objects.get(id=study_field.id)
        self.assertEqual(study_field.id, study_field_new.id)
        self.assertEqual(study_field_new.field_name, "new_name")


# test download_participants_csv
class TestDownloadParticipantsCsv(ResearcherSessionTest, ParticipantTableHelperMixin):
    ENDPOINT_NAME = "manage_study_endpoints.download_participants_csv"
    JAN_1_2020 = datetime(2020, 1, 1, 12, tzinfo=timezone.utc)
    ELEVEN_NONES_STRING = ",None" * 11  # One none for each field in EXTRA_TABLE_FIELDS
    
    @property
    def response_basic(self) -> bytes:
        return (self.header() + f"2020-01-01,patient1,Inactive,ANDROID{self.ELEVEN_NONES_STRING}\r\n" +
                                f"2020-01-01,patient2,Inactive,ANDROID{self.ELEVEN_NONES_STRING}\r\n")
    
    @property
    def response_with_intervention(self) -> bytes:
        return (self.header(intervention=True) +
                f"2020-01-01,patient1,Inactive,ANDROID,2020-01-01{self.ELEVEN_NONES_STRING}\r\n" +
                f"2020-01-01,patient2,Inactive,ANDROID,2020-01-01{self.ELEVEN_NONES_STRING}\r\n")
    
    @property
    def response_with_custom_field(self) -> bytes:
        return (self.header(custom_field=True) +
                f"2020-01-01,patient1,Inactive,ANDROID,default_study_field_value{self.ELEVEN_NONES_STRING}\r\n" +
                f"2020-01-01,patient2,Inactive,ANDROID,default_study_field_value{self.ELEVEN_NONES_STRING}\r\n")
    
    @property
    def response_with_intervention_and_custom_field(self) -> bytes:
        return (
            self.header(intervention=True, custom_field=True) +
            f"2020-01-01,patient1,Inactive,ANDROID,2020-01-01,default_study_field_value{self.ELEVEN_NONES_STRING}\r\n" +
            f"2020-01-01,patient2,Inactive,ANDROID,2020-01-01,default_study_field_value{self.ELEVEN_NONES_STRING}\r\n"
        )
    
    @property
    def setup_two_base_participants(self) -> Tuple[Participant, Participant]:
        p1 = self.generate_participant(self.session_study, "patient1")
        p2 = self.generate_participant(self.session_study, "patient2")
        p1.update_only(created_on=self.JAN_1_2020)
        p2.update_only(created_on=self.JAN_1_2020)
        return p1, p2
    
    def test_study_admin(self):
        self.set_session_study_relation(ResearcherRole.study_admin)
        self._test_two_participants_basic()
    
    def test_study_researcher(self):
        self.set_session_study_relation(ResearcherRole.researcher)
        self._test_two_participants_basic()
    
    def test_site_admin(self):
        self.set_session_study_relation(ResearcherRole.site_admin)
        self._test_two_participants_basic()
    
    def _test_two_participants_basic(self):
        self.setup_two_base_participants
        resp = self.smart_get(self.session_study.id)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp["Content-Type"], "text/csv")
        self.assertEqual(resp.content.decode(), self.response_basic)
    
    def test_two_participants_with_intervention(self):
        self.set_session_study_relation(ResearcherRole.researcher)
        p1, p2 = self.setup_two_base_participants
        self.generate_intervention_date(p1, self.default_intervention, self.JAN_1_2020)
        self.generate_intervention_date(p2, self.default_intervention, self.JAN_1_2020)
        resp = self.smart_get(self.session_study.id)
        self.assertEqual(resp.content.decode(), self.response_with_intervention)
        
    def test_two_participants_with_custom_field(self):
        self.set_session_study_relation(ResearcherRole.researcher)
        p1, p2 = self.setup_two_base_participants
        self.generate_participant_field_value(
            self.default_study_field, p1, self.DEFAULT_PARTICIPANT_FIELD_VALUE)
        self.generate_participant_field_value(
            self.default_study_field, p2, self.DEFAULT_PARTICIPANT_FIELD_VALUE)
        resp = self.smart_get(self.session_study.id)
        self.assertEqual(self.response_with_custom_field, resp.content.decode())
    
    def test_two_participants_with_intervention_and_custom_field(self):
        self.set_session_study_relation(ResearcherRole.researcher)
        p1, p2 = self.setup_two_base_participants
        self.generate_intervention_date(p1, self.default_intervention, self.JAN_1_2020)
        self.generate_intervention_date(p2, self.default_intervention, self.JAN_1_2020)
        self.generate_participant_field_value(
            self.default_study_field, p1, self.DEFAULT_PARTICIPANT_FIELD_VALUE)
        self.generate_participant_field_value(
            self.default_study_field, p2, self.DEFAULT_PARTICIPANT_FIELD_VALUE)
        resp = self.smart_get(self.session_study.id)
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp["Content-Type"], "text/csv")
        self.assertEqual(resp.content.decode(), self.response_with_intervention_and_custom_field)
    
    def test_no_participants(self):
        self.set_session_study_relation(ResearcherRole.researcher)
        content = self.smart_get(self.session_study.id).content
        self.assertEqual(content.decode(), self.header())
    
    def test_single_base_participant(self):
        self.set_session_study_relation(ResearcherRole.researcher)
        p1 = self.generate_participant(self.session_study, "patient1")
        p1.update_only(created_on=self.JAN_1_2020)
        resp = self.smart_get(self.session_study.id)
        # I don't know why there is a trailing newline, but it is there.
        ref = "\r\n".join(self.response_basic.splitlines()[:-1]) + "\r\n"
        self.assertEqual(resp.content.decode(), ref)

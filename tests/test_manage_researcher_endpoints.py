from datetime import timedelta

import time_machine
from django.http import HttpResponseRedirect
from django.utils import timezone

from constants.message_strings import (API_KEY_IS_DISABLED, MFA_CODE_6_DIGITS, MFA_CODE_DIGITS_ONLY,
    MFA_CODE_MISSING, MFA_RESET_BAD_PERMISSIONS, MFA_SELF_BAD_PASSWORD, MFA_SELF_DISABLED,
    MFA_SELF_NO_PASSWORD, MFA_SELF_SUCCESS, MFA_TEST_DISABLED, MFA_TEST_FAIL, MFA_TEST_SUCCESS,
    NEW_PASSWORD_MISMATCH, NEW_PASSWORD_N_LONG, NEW_PASSWORD_RULES_FAIL, NO_MATCHING_API_KEY,
    PASSWORD_RESET_FAIL_SITE_ADMIN, PASSWORD_RESET_SUCCESS, WRONG_CURRENT_PASSWORD)
from constants.security_constants import MFA_CREATED
from constants.testing_constants import ADMIN_ROLES
from constants.user_constants import ALL_RESEARCHER_TYPES, EXPIRY_NAME, ResearcherRole
from database.security_models import ApiKey
from database.study_models import Study
from database.system_models import DataAccessRecord
from database.user_models_researcher import Researcher, StudyRelation
from libs.utils.http_utils import easy_url
from libs.utils.security_utils import generate_easy_alphanumeric_string
from tests.common import ResearcherSessionTest, TableauAPITest


class TestAdministratorDemoteStudyAdmin(ResearcherSessionTest):
    # FIXME: this endpoint does not test for site admin cases correctly, the test passes but is
    # wrong. Behavior is fine because it has no relevant side effects except for the know bug where
    # site admins need to be manually added to a study before being able to download data.
    ENDPOINT_NAME = "manage_researcher_endpoints.administrator_demote_study_administrator_to_researcher"
    
    def test_researcher_as_researcher(self):
        r2 = self.generate_researcher(relation_to_session_study=ResearcherRole.researcher)
        self.smart_post_status_code(403, researcher_id=r2.id, study_id=self.session_study.id)
        self.assertEqual(r2.study_relations.get().relationship, ResearcherRole.researcher)
    
    def test_researcher_as_study_admin(self):
        self.set_session_study_relation(ResearcherRole.study_admin)
        r2 = self.generate_researcher(relation_to_session_study=ResearcherRole.researcher)
        self.smart_post_status_code(302, researcher_id=r2.id, study_id=self.session_study.id)
        self.assertEqual(r2.study_relations.get().relationship, ResearcherRole.researcher)
    
    def test_study_admin_as_admin(self):
        self.set_session_study_relation(ResearcherRole.study_admin)
        r2 = self.generate_researcher(relation_to_session_study=ResearcherRole.study_admin)
        self.smart_post_status_code(302, researcher_id=r2.id, study_id=self.session_study.id)
        self.assertEqual(r2.study_relations.get().relationship, ResearcherRole.researcher)
    
    def test_site_admin_as_study_admin(self):
        self.set_session_study_relation(ResearcherRole.study_admin)
        r2 = self.generate_researcher(relation_to_session_study=ResearcherRole.site_admin)
        self.smart_post_status_code(302, researcher_id=r2.id, study_id=self.session_study.id)
        self.assertFalse(r2.study_relations.exists())
        r2.refresh_from_db()
        self.assertTrue(r2.site_admin)
    
    def test_site_admin_as_site_admin(self):
        self.set_session_study_relation(ResearcherRole.site_admin)
        r2 = self.generate_researcher(relation_to_session_study=ResearcherRole.site_admin)
        self.smart_post_status_code(302, researcher_id=r2.id, study_id=self.session_study.id)
        self.assertFalse(r2.study_relations.exists())
        r2.refresh_from_db()
        self.assertTrue(r2.site_admin)


class TestAdministratorCreateNewResearcher(ResearcherSessionTest):
    """ Admins should be able to create and load the page. """
    ENDPOINT_NAME = "manage_researcher_endpoints.administrator_create_new_researcher"
    
    def test_load_page_at_endpoint(self):
        # This test should be transformed into a separate endpoint
        for user_role in ALL_RESEARCHER_TYPES:
            prior_researcher_count = Researcher.objects.count()
            self.assign_role(self.session_researcher, user_role)
            resp = self.smart_get()
            if user_role in ADMIN_ROLES:
                self.assertEqual(resp.status_code, 200)
            else:
                self.assertEqual(resp.status_code, 403)
            self.assertEqual(prior_researcher_count, Researcher.objects.count())
    
    def test_create_researcher(self):
        for user_role in ALL_RESEARCHER_TYPES:
            prior_researcher_count = Researcher.objects.count()
            self.assign_role(self.session_researcher, user_role)
            username = generate_easy_alphanumeric_string()
            password = generate_easy_alphanumeric_string()
            resp = self.smart_post(admin_id=username, password=password)
            
            if user_role in ADMIN_ROLES:
                self.assertEqual(resp.status_code, 302)
                self.assertEqual(prior_researcher_count + 1, Researcher.objects.count())
                self.assertTrue(Researcher.check_password(username, password))
            else:
                self.assertEqual(resp.status_code, 403)
                self.assertEqual(prior_researcher_count, Researcher.objects.count())


class TestAdministratorManageResearchers(ResearcherSessionTest):
    ENDPOINT_NAME = "manage_researcher_endpoints.administrator_manage_researchers_page"
    
    def test_researcher(self):
        self.smart_get_status_code(403)
    
    def test_study_admin(self):
        self.set_session_study_relation(ResearcherRole.study_admin)
        self.smart_get_status_code(200)
    
    def test_site_admin(self):
        self.set_session_study_relation(ResearcherRole.site_admin)
        self.smart_get_status_code(200)
    
    def test_render_study_admin(self):
        self.set_session_study_relation(ResearcherRole.study_admin)
        self._test_render_with_researchers()
        # make sure that site admins are not present
        r4 = self.generate_researcher(relation_to_session_study=ResearcherRole.site_admin)
        resp = self.smart_get_status_code(200)
        self.assert_not_present(r4.username, resp.content)
        
        # make sure that unaffiliated researchers are not present
        r5 = self.generate_researcher()
        resp = self.smart_get_status_code(200)
        self.assert_not_present(r5.username, resp.content)
    
    def test_render_site_admin(self):
        self.set_session_study_relation(ResearcherRole.site_admin)
        self._test_render_with_researchers()
        # make sure that site admins ARE present
        r4 = self.generate_researcher(relation_to_session_study=ResearcherRole.site_admin)
        resp = self.smart_get_status_code(200)
        self.assert_present(r4.username, resp.content)
        
        # make sure that unaffiliated researchers ARE present
        r5 = self.generate_researcher()
        resp = self.smart_get_status_code(200)
        self.assert_present(r5.username, resp.content)
    
    def _test_render_with_researchers(self):
        # render the page with a regular user
        r2 = self.generate_researcher(relation_to_session_study=ResearcherRole.researcher)
        resp = self.smart_get_status_code(200)
        self.assert_present(r2.username, resp.content)
        
        # render with 2 reseaorchers
        r3 = self.generate_researcher(relation_to_session_study=ResearcherRole.researcher)
        resp = self.smart_get_status_code(200)
        self.assert_present(r2.username, resp.content)
        self.assert_present(r3.username, resp.content)


class TestAdministratorResetResearcherMFA(ResearcherSessionTest):
    ENDPOINT_NAME = "manage_researcher_endpoints.administrator_reset_researcher_mfa"
    REDIRECT_ENDPOINT_NAME = "manage_researcher_endpoints.administrator_edit_researcher_page"
    
    def test_reset_as_site_admin(self):
        self._test_reset(self.generate_researcher(), ResearcherRole.site_admin)
    
    def test_reset_as_site_admin_on_site_admin(self):
        researcher = self.generate_researcher()
        self.generate_study_relation(researcher, self.session_study, ResearcherRole.site_admin)
        self._test_reset(researcher, ResearcherRole.site_admin)
    
    def test_reset_as_study_admin_on_good_researcher(self):
        researcher = self.generate_researcher()
        self.generate_study_relation(researcher, self.session_study, ResearcherRole.researcher)
        self._test_reset(researcher, ResearcherRole.study_admin)
    
    def test_reset_as_study_admin_on_bad_researcher(self):
        researcher = self.generate_researcher()
        self._test_reset_fail(researcher, ResearcherRole.study_admin, 403)
    
    def test_reset_as_study_admin_on_site_admin(self):
        researcher = self.generate_researcher()
        self.generate_study_relation(researcher, self.session_study, ResearcherRole.site_admin)
        self._test_reset_fail(researcher, ResearcherRole.study_admin, 403)
    
    def test_reset_study_admin_with_good_study_admin(self):
        researcher = self.generate_researcher()
        self.generate_study_relation(researcher, self.session_study, ResearcherRole.study_admin)
        self._test_reset(researcher, ResearcherRole.study_admin)
    
    def test_no_researcher(self):
        # basically, it should 404
        self.set_session_study_relation(ResearcherRole.site_admin)
        resp = self.smart_post(
            0
        )  # not the magic redirect smart post; 0 will always be an invalid researcher id
        self.assertEqual(404, resp.status_code)
    
    def _test_reset(self, researcher: Researcher, role: ResearcherRole):
        researcher.reset_mfa()
        self.set_session_study_relation(role)
        self.smart_post(researcher.id)  # magic redirect smart post, tests for a redirect
        researcher.refresh_from_db()
        self.assertIsNone(researcher.mfa_token)
        resp = self.easy_get(self.REDIRECT_ENDPOINT_NAME, researcher_pk=researcher.id)
        self.assert_present("MFA token cleared for researcher ", resp.content)
    
    def _test_reset_fail(self, researcher: Researcher, role: ResearcherRole, status_code: int):
        researcher.reset_mfa()
        self.set_session_study_relation(role)
        resp = self.smart_post(
            researcher.id
        )  # not the magic redirect smart post; 0 will always be an invalid researcher id
        self.assertEqual(status_code, resp.status_code)
        researcher.refresh_from_db()
        self.assertIsNotNone(researcher.mfa_token)
        resp = self.easy_get(self.REDIRECT_ENDPOINT_NAME, researcher_pk=researcher.id)
        if resp.status_code == 403:
            self.assert_present(MFA_RESET_BAD_PERMISSIONS, resp.content)


class TestAdministratorEditResearcher(ResearcherSessionTest):
    ENDPOINT_NAME = "manage_researcher_endpoints.administrator_edit_researcher_page"
    
    # render self
    def test_render_for_self_as_researcher(self):
        # should fail
        self.set_session_study_relation()
        self.smart_get_status_code(403, self.session_researcher.id)
    
    def test_render_for_self_as_study_admin(self):
        # ensure it renders (buttons will be disabled)
        self.set_session_study_relation(ResearcherRole.study_admin)
        self.smart_get_status_code(200, self.session_researcher.id)
    
    def test_render_for_self_as_site_admin(self):
        # ensure it renders (buttons will be disabled)
        self.set_session_study_relation(ResearcherRole.site_admin)
        self.smart_get_status_code(200, self.session_researcher.id)
    
    def test_render_for_researcher_as_researcher(self):
        # should fail
        self.set_session_study_relation()
        # set up, test when not on study
        r2 = self.generate_researcher()
        resp = self.smart_get_status_code(403, r2.id)
        self.assert_not_present(r2.username, resp.content)
        # attach other researcher and try again
        self.generate_study_relation(r2, self.session_study, ResearcherRole.researcher)
        resp = self.smart_get_status_code(403, r2.id)
        self.assert_not_present(r2.username, resp.content)
    
    # study admin, renders
    def test_render_valid_researcher_as_study_admin(self):
        self.set_session_study_relation(ResearcherRole.study_admin)
        self._test_render_generic_under_study()
    
    def test_render_researcher_with_no_study_as_study_admin(self):
        self.set_session_study_relation(ResearcherRole.study_admin)
        self._test_render_researcher_with_no_study()
    
    # site admin, renders
    def test_render_valid_researcher_as_site_admin(self):
        self.set_session_study_relation(ResearcherRole.site_admin)
        self._test_render_generic_under_study()
    
    def test_render_researcher_with_no_study_as_site_admin(self):
        self.set_session_study_relation(ResearcherRole.site_admin)
        self._test_render_researcher_with_no_study()
    
    def _test_render_generic_under_study(self):
        r2 = self.generate_researcher(relation_to_session_study=ResearcherRole.researcher)
        resp = self.smart_get_status_code(200, r2.id)
        self.assert_present(r2.username, resp.content)
    
    def _test_render_researcher_with_no_study(self):
        r2 = self.generate_researcher()
        resp = self.smart_get_status_code(200, r2.id)
        self.assert_present(r2.username, resp.content)


class TestAdministratorElevateResearcher(ResearcherSessionTest):
    ENDPOINT_NAME = "manage_researcher_endpoints.administrator_elevate_researcher_to_study_admin"
    
    # (this one is tedious.)
    
    def test_self_as_researcher_on_study(self):
        self.set_session_study_relation(ResearcherRole.researcher)
        self.smart_post_status_code(
            403, researcher_id=self.session_researcher.id, study_id=self.session_study.id
        )
    
    def test_self_as_study_admin_on_study(self):
        self.set_session_study_relation(ResearcherRole.study_admin)
        self.smart_post_status_code(
            403, researcher_id=self.session_researcher.id, study_id=self.session_study.id
        )
    
    def test_researcher_as_study_admin_on_study(self):
        # this is the only case that succeeds
        self.set_session_study_relation(ResearcherRole.study_admin)
        r2 = self.generate_researcher(relation_to_session_study=ResearcherRole.researcher)
        self.smart_post_status_code(302, researcher_id=r2.id, study_id=self.session_study.id)
        self.assertEqual(r2.study_relations.get().relationship, ResearcherRole.study_admin)
    
    def test_study_admin_as_study_admin_on_study(self):
        self.set_session_study_relation(ResearcherRole.study_admin)
        r2 = self.generate_researcher(relation_to_session_study=ResearcherRole.study_admin)
        self.smart_post_status_code(403, researcher_id=r2.id, study_id=self.session_study.id)
        self.assertEqual(r2.study_relations.get().relationship, ResearcherRole.study_admin)
    
    def test_site_admin_as_study_admin_on_study(self):
        self.session_researcher
        self.set_session_study_relation(ResearcherRole.study_admin)
        r2 = self.generate_researcher(relation_to_session_study=ResearcherRole.site_admin)
        self.smart_post_status_code(403, researcher_id=r2.id, study_id=self.session_study.id)
        self.assertFalse(r2.study_relations.filter(study=self.session_study).exists())
    
    def test_site_admin_as_site_admin(self):
        self.set_session_study_relation(ResearcherRole.site_admin)
        r2 = self.generate_researcher(relation_to_session_study=ResearcherRole.site_admin)
        self.smart_post_status_code(403, researcher_id=r2.id, study_id=self.session_study.id)
        self.assertFalse(r2.study_relations.filter(study=self.session_study).exists())


class TestAdministratorAddResearcherToStudy(ResearcherSessionTest):
    ENDPOINT_NAME = "manage_researcher_endpoints.administrator_add_researcher_to_study"
    REDIRECT_ENDPOINT_NAME = "study_endpoints.edit_study"
    
    def test_site_admin(self):
        self.set_session_study_relation(ResearcherRole.site_admin)
        self._test(None, 302, ResearcherRole.researcher)
        self._test(ResearcherRole.study_admin, 302, ResearcherRole.study_admin)
        self._test(ResearcherRole.researcher, 302, ResearcherRole.researcher)
    
    # # FIXME: test fails, need to fix data download bug on site admin users first
    # def test_site_admin_on_site_admin(self):
    #     self.set_session_study_relation(ResearcherRole.site_admin)
    #     self._test(ResearcherRole.site_admin, 403, ResearcherRole.site_admin)
    
    def test_study_admin_on_none(self):
        self.set_session_study_relation(ResearcherRole.study_admin)
        self._test(None, 302, ResearcherRole.researcher)
    
    def test_study_admin_on_study_admin(self):
        self.set_session_study_relation(ResearcherRole.study_admin)
        self._test(ResearcherRole.study_admin, 302, ResearcherRole.study_admin)
    
    def test_study_admin_on_researcher(self):
        self.set_session_study_relation(ResearcherRole.study_admin)
        self._test(ResearcherRole.researcher, 302, ResearcherRole.researcher)
    
    # FIXME: test fails, need to fix data download bug on site admin users first
    # def test_study_admin_on_site_admin(self):
    #     self.set_session_study_relation(ResearcherRole.study_admin)
    #     self._test(ResearcherRole.site_admin, 403, ResearcherRole.site_admin)
    
    def test_researcher(self):
        self.set_session_study_relation(ResearcherRole.researcher)
        self._test(ResearcherRole.researcher, 403, ResearcherRole.researcher)
        self._test(ResearcherRole.study_admin, 403, ResearcherRole.study_admin)
        self._test(None, 403, None)
        self._test(ResearcherRole.site_admin, 403, ResearcherRole.site_admin)
    
    def _test(self, r2_starting_relation, status_code, desired_relation):
        # setup researcher, do the post request
        r2 = self.generate_researcher(relation_to_session_study=r2_starting_relation)
        redirect_or_response = self.smart_post(
            study_id=self.session_study.id,
            researcher_id=r2.id,
            redirect_url=f"/edit_study/{self.session_study.id}"
        )
        # check status code, relation, and ~the redirect url.
        r2.refresh_from_db()
        self.assert_researcher_relation(r2, self.session_study, desired_relation)
        self.assertEqual(redirect_or_response.status_code, status_code)
        if isinstance(redirect_or_response, HttpResponseRedirect):
            self.assertEqual(redirect_or_response.url, f"/edit_study/{self.session_study.id}")


# trunk-ignore-all(ruff/B018)

#
## manage_study_endpoints
#
class TestAdministratorRemoveResearcherFromStudy(ResearcherSessionTest):
    ENDPOINT_NAME = "manage_researcher_endpoints.administrator_remove_researcher_from_study"
    REDIRECT_ENDPOINT_NAME = "study_endpoints.edit_study"
    
    def test_site_admin(self):
        self.set_session_study_relation(ResearcherRole.site_admin)
        self._test(None, 302)
        self._test(ResearcherRole.study_admin, 302)
        self._test(ResearcherRole.researcher, 302)
        self._test(ResearcherRole.site_admin, 302)
    
    def test_study_admin(self):
        self.set_session_study_relation(ResearcherRole.study_admin)
        self._test(None, 403)
        self._test(ResearcherRole.study_admin, 403)
        self._test(ResearcherRole.researcher, 302)
        self._test(ResearcherRole.site_admin, 403)
    
    def test_researcher(self):
        self.set_session_study_relation(ResearcherRole.researcher)
        r2 = self.generate_researcher(relation_to_session_study=None)
        self.smart_post_status_code(
            403,
            study_id=self.session_study.id,
            researcher_id=r2.id,
            redirect_url=f"/edit_study/{self.session_study.id}"
        )
    
    def _test(self, r2_starting_relation, status_code):
        if r2_starting_relation == ResearcherRole.site_admin:
            r2 = self.generate_researcher(relation_to_session_study=ResearcherRole.site_admin)
        else:
            r2 = self.generate_researcher(relation_to_session_study=r2_starting_relation)
        redirect = self.smart_post(
            study_id=self.session_study.id,
            researcher_id=r2.id,
            redirect_url=f"/edit_study/{self.session_study.id}"
        )
        # needs to be a None at the end
        self.assertEqual(redirect.status_code, status_code)
        if isinstance(redirect, HttpResponseRedirect):
            self.assert_researcher_relation(r2, self.session_study, None)
            self.assertEqual(redirect.url, f"/edit_study/{self.session_study.id}")


class TestAdministratorDeleteResearcher(ResearcherSessionTest):
    ENDPOINT_NAME = "manage_researcher_endpoints.administrator_delete_researcher"
    
    def test_site_admin(self):
        self._test_basics(302, ResearcherRole.site_admin, True)
    
    def test_study_admin(self):
        self._test_basics(403, ResearcherRole.study_admin, False)
    
    def test_researcher(self):
        self._test_basics(403, ResearcherRole.researcher, False)
    
    def test_no_relation(self):
        self._test_basics(403, None, False)
    
    def test_nonexistent(self):
        self.set_session_study_relation(ResearcherRole.site_admin)
        # 0 is not a valid database key.
        self.smart_get_status_code(404, 0)
    
    def _test_basics(self, status_code: int, relation: str, success: bool):
        self.set_session_study_relation(relation)  # implicitly creates self.session_researcher
        r2 = self.generate_researcher()
        self.smart_get_status_code(status_code, r2.id)
        self.assertEqual(Researcher.objects.filter(id=r2.id).count(), 0 if success else 1)
    
    def test_cascade(self):
        # first assert that this is actually all the relations:
        self.assertEqual(
            [obj.related_model.__name__ for obj in Researcher._meta.related_objects],
            ['StudyRelation', 'ResearcherSession', 'DataAccessRecord', 'ApiKey']
        )
        # we need the test to succeed...
        self.set_session_study_relation(ResearcherRole.site_admin)
        r2 = self.generate_researcher()

        # generate all possible researcher relations for r2 as determined above:
        ApiKey.generate(
            researcher=r2, readable_name="test_api_key"
        )
        relation_id = self.generate_study_relation(
            r2, self.default_study, ResearcherRole.researcher
        ).id
        record = DataAccessRecord.objects.create(
            researcher=r2, query_params="test_junk", username=r2.username
        )
        # for tests after deletion
        relation_id = r2.study_relations.get().id
        default_study_id = self.default_study.id
        # request
        self.smart_get_status_code(302, r2.id)
        # test that these were deleted
        self.assertFalse(Researcher.objects.filter(id=r2.id).exists())
        self.assertFalse(StudyRelation.objects.filter(id=relation_id).exists())
        # I can never remember the direction of cascade, confirm study is still there
        self.assertTrue(Study.objects.filter(id=default_study_id).exists())
        # and assert that the DataAccessRecord is still there with a null researcher and a username.
        self.assertTrue(DataAccessRecord.objects.filter(id=record.id).exists())
        self.assertTrue(ApiKey.objects.exists())
        record.refresh_from_db()
        self.assertIsNone(record.researcher)
        self.assertEqual(record.username, r2.username)


class TestAdministratorSetResearcherPassword(ResearcherSessionTest):
    ENDPOINT_NAME = "manage_researcher_endpoints.administrator_set_researcher_password"

    def test_site_admin_on_a_null_user(self):
        self.set_session_study_relation(ResearcherRole.site_admin)
        r2 = self.generate_researcher()
        self._test_successful_change(r2)

    def test_site_admin_on_researcher(self):
        self.set_session_study_relation(ResearcherRole.site_admin)
        r2 = self.generate_researcher()
        self.generate_study_relation(r2, self.default_study, ResearcherRole.researcher)
        self._test_successful_change(r2)

    def test_site_admin_on_study_admin(self):
        self.set_session_study_relation(ResearcherRole.site_admin)
        r2 = self.generate_researcher()
        self.generate_study_relation(r2, self.default_study, ResearcherRole.study_admin)
        self._test_successful_change(r2)

    def test_site_admin_on_site_admin(self):
        self.set_session_study_relation(ResearcherRole.site_admin)
        r2 = self.generate_researcher()
        self.generate_study_relation(r2, self.default_study, ResearcherRole.site_admin)
        self._test_cannot_change(r2, PASSWORD_RESET_FAIL_SITE_ADMIN)

    def _test_successful_change(self, r2: Researcher):
        self.smart_post(
            researcher_id=r2.id,
            password=self.DEFAULT_RESEARCHER_PASSWORD + "1",
        )
        # we are ... teleologically correct here mimicking the code...
        r2.refresh_from_db()
        self.assertTrue(r2.password_force_reset)
        self.assertTrue(r2.check_password(r2.username, self.DEFAULT_RESEARCHER_PASSWORD + "1"))
        self.assertFalse(r2.check_password(r2.username, self.DEFAULT_RESEARCHER_PASSWORD))
        self.assertEqual(r2.web_sessions.count(), 0)

    def _test_cannot_change(self, r2: Researcher, message: str = None):
        ret = self.smart_post(
            researcher_id=r2.id,
            password=self.DEFAULT_RESEARCHER_PASSWORD + "1",
        )
        if message:
            content = self.easy_get(
                "manage_researcher_endpoints.administrator_edit_researcher_page", researcher_pk=r2.id
            ).content
            self.assert_present(message, content)
        r2.refresh_from_db()
        self.assertFalse(r2.password_force_reset)
        self.assertFalse(r2.check_password(r2.username, self.DEFAULT_RESEARCHER_PASSWORD + "1"))
        self.assertTrue(r2.check_password(r2.username, self.DEFAULT_RESEARCHER_PASSWORD))
        self.assertEqual(self.session_researcher.web_sessions.count(), 1)
        return ret


class TestSelfManageCredentials(ResearcherSessionTest):
    ENDPOINT_NAME = "manage_researcher_endpoints.self_manage_credentials_page"
    
    def test_manage_credentials(self):
        self.session_study
        self.smart_get_status_code(200)
        api_key = ApiKey.generate(
            researcher=self.session_researcher,
            readable_name="not important",
        )
        response = self.smart_get_status_code(200)
        self.assert_present(api_key.access_key_id, response.content)
    
    def test_mfa_not_visible(self):
        # qr code should only be visible if mfa password was provided (and matches) and session does
        # not contain the MFA_CREATED key
        session = self.client.session  # this creates a new object
        self.session_researcher.reset_mfa()
        # use the alt text to test for presence of the qr code
        self.assert_not_present('alt="MFA QR Code"', self.smart_get_status_code(200).content)
        session[MFA_CREATED] = timezone.now() - timedelta(seconds=60)
        session.save()
        self.assert_not_present('alt="MFA QR Code"', self.smart_get_status_code(200).content)
    
    def test_mfa_visible_session_manip(self):
        session = self.client.session  # this creates a new object
        self.session_researcher.reset_mfa()
        session[MFA_CREATED] = timezone.now()
        session.save()  # save the session because this isn't inside the request/response cycle
        self.assert_present('alt="MFA QR Code"', self.smart_get_status_code(200).content)
    
    def test_mfa_visible_password(self):
        self.session_researcher.reset_mfa()
        resp = self.smart_post_status_code(200, view_mfa_password=self.DEFAULT_RESEARCHER_PASSWORD)
        self.assert_present('alt="MFA QR Code"', resp.content)


class TestSelfResetAdminPassword(ResearcherSessionTest):
    # test for every case and messages present on the page
    ENDPOINT_NAME = "manage_researcher_endpoints.self_change_password"
    REDIRECT_ENDPOINT_NAME = "manage_researcher_endpoints.self_manage_credentials_page"
    
    def test_self_change_password_success(self):
        resp = self.smart_post_status_code(
            302,
            current_password=self.DEFAULT_RESEARCHER_PASSWORD,
            new_password=self.DEFAULT_RESEARCHER_PASSWORD + "1",
            confirm_new_password=self.DEFAULT_RESEARCHER_PASSWORD + "1",
        )
        # the initial redirect is to the normal endpoint
        self.assertEqual(resp.url, easy_url(self.REDIRECT_ENDPOINT_NAME))
        
        r = self.session_researcher
        r.refresh_from_db()
        self.assertFalse(r.check_password(r.username, self.DEFAULT_RESEARCHER_PASSWORD))
        self.assertTrue(r.check_password(r.username, self.DEFAULT_RESEARCHER_PASSWORD + "1"))
        r.force_global_logout()
        self.assertEqual(self.session_researcher.web_sessions.count(), 0)
        resp = self.easy_get(self.REDIRECT_ENDPOINT_NAME, 302)
        self.assertEqual(resp.url, easy_url("login_endpoints.login_page"))
        resp = self.easy_get("login_endpoints.login_page", 200)
        self.assert_present(PASSWORD_RESET_SUCCESS, resp.content)
        
        # and in december 2023 added the auto logout.
        # test that the 10 second session timeout is working
        now = timezone.now()
        self.assertLess(self.session_researcher.password_last_changed, now + timedelta(seconds=10))
        with time_machine.travel(timezone.now() + timedelta(seconds=11)):
            # hit a page, confirm check expiry deleted
            resp: HttpResponseRedirect = self.easy_get("study_endpoints.choose_study_page")
            self.assertEqual(resp.status_code, 302)
            self.assertEqual(resp.url, easy_url("login_endpoints.login_page"))
            try:
                final_expiry = self.client.session[EXPIRY_NAME]
                self.fail("session expiry should have been deleted")
            except KeyError as e:
                self.assertEqual(e.args[0], EXPIRY_NAME)
    
    def test_self_change_password_wrong(self):
        self.smart_post(
            current_password=self.DEFAULT_RESEARCHER_PASSWORD + "1",
            new_password=self.DEFAULT_RESEARCHER_PASSWORD + "1",
            confirm_new_password=self.DEFAULT_RESEARCHER_PASSWORD + "1",
        )
        r = self.session_researcher
        self.assertTrue(r.check_password(r.username, self.DEFAULT_RESEARCHER_PASSWORD))
        self.assertFalse(r.check_password(r.username, self.DEFAULT_RESEARCHER_PASSWORD + "1"))
        self.assert_present(WRONG_CURRENT_PASSWORD, self.redirect_get_contents())
    
    def test_self_change_password_rules_fail(self):
        non_default = "abcdefghijklmnop"
        self.smart_post(
            current_password=self.DEFAULT_RESEARCHER_PASSWORD,
            new_password=non_default,
            confirm_new_password=non_default,
        )
        r = self.session_researcher
        self.assertTrue(r.check_password(r.username, self.DEFAULT_RESEARCHER_PASSWORD))
        self.assertFalse(r.check_password(r.username, non_default))
        self.assert_present(NEW_PASSWORD_RULES_FAIL, self.redirect_get_contents())
    
    def test_self_change_password_too_short(self):
        non_default = "a1#"
        self.smart_post(
            current_password=self.DEFAULT_RESEARCHER_PASSWORD,
            new_password=non_default,
            confirm_new_password=non_default,
        )
        r = self.session_researcher
        self.assertTrue(r.check_password(r.username, self.DEFAULT_RESEARCHER_PASSWORD))
        self.assertFalse(r.check_password(r.username, non_default))
        self.assert_present(NEW_PASSWORD_N_LONG.format(length=8), self.redirect_get_contents())
    
    def test_self_change_password_too_short_study_setting(self):
        self.session_study.update(password_minimum_length=20)
        self.set_session_study_relation(ResearcherRole.researcher)
        non_default = "aA1#aA1#aA1#aA1#"  # 10 chars
        self.smart_post(
            current_password=self.DEFAULT_RESEARCHER_PASSWORD,
            new_password=non_default,
            confirm_new_password=non_default,
        )
        r = self.session_researcher
        self.assertTrue(r.check_password(r.username, self.DEFAULT_RESEARCHER_PASSWORD))
        self.assertFalse(r.check_password(r.username, non_default))
        self.assert_present(NEW_PASSWORD_N_LONG.format(length=20), self.redirect_get_contents())
    
    def test_self_change_password_too_short_site_admin(self):
        self.set_session_study_relation(ResearcherRole.site_admin)
        non_default = "aA1#aA1#aA1#aA1#"  # 10 chars
        self.smart_post(
            current_password=self.DEFAULT_RESEARCHER_PASSWORD,
            new_password=non_default,
            confirm_new_password=non_default,
        )
        r = self.session_researcher
        self.assertTrue(r.check_password(r.username, self.DEFAULT_RESEARCHER_PASSWORD))
        self.assertFalse(r.check_password(r.username, non_default))
        self.assert_present(NEW_PASSWORD_N_LONG.format(length=20), self.redirect_get_contents())
    
    def test_self_change_password_mismatch(self):
        # has to pass the length and character checks
        self.smart_post(
            current_password=self.DEFAULT_RESEARCHER_PASSWORD,
            new_password="aA1#aA1#aA1#",
            confirm_new_password="aA1#aA1#aA1#aA1#",
        )
        researcher = self.session_researcher
        self.assertTrue(
            researcher.check_password(researcher.username, self.DEFAULT_RESEARCHER_PASSWORD)
        )
        self.assertFalse(researcher.check_password(researcher.username, "aA1#aA1#aA1#"))
        self.assertFalse(researcher.check_password(researcher.username, "aA1#aA1#aA1#aA1#"))
        self.assert_present(NEW_PASSWORD_MISMATCH, self.redirect_get_contents())


class TestSelfResetMFASelf(ResearcherSessionTest):
    ENDPOINT_NAME = "manage_researcher_endpoints.self_reset_mfa"
    REDIRECT_ENDPOINT_NAME = "manage_researcher_endpoints.self_manage_credentials_page"
    
    def test_no_password(self):
        session = self.client.session
        orig_mfa = self.session_researcher.reset_mfa()
        self.smart_post()  # magic redirect smart post, tests for the redirect
        resp = self.easy_get(self.REDIRECT_ENDPOINT_NAME)
        self.assert_present(MFA_SELF_NO_PASSWORD, resp.content)
        self.session_researcher.refresh_from_db()
        self.assertIsNotNone(self.session_researcher.mfa_token)
        self.assertEqual(orig_mfa, self.session_researcher.mfa_token)  # no change
        self.assertNotIn(MFA_CREATED, session)
    
    def test_bad_password(self):
        session = self.client.session
        orig_mfa = self.session_researcher.reset_mfa()
        self.smart_post(mfa_password="wrong_password")  # magic redirect smart post
        resp = self.easy_get(self.REDIRECT_ENDPOINT_NAME)
        self.assert_present(MFA_SELF_BAD_PASSWORD, resp.content)
        self.assertIsNotNone(self.session_researcher.mfa_token)
        self.assertEqual(orig_mfa, self.session_researcher.mfa_token)  # no change
        self.assertNotIn(MFA_CREATED, session)
    
    def test_mfa_reset_with_mfa_token(self):
        # case is not accessible from webpage
        session = self.client.session
        orig_mfa = self.session_researcher.reset_mfa()
        self.smart_post(mfa_password=self.DEFAULT_RESEARCHER_PASSWORD)  # magic redirect smart post
        resp = self.easy_get(self.REDIRECT_ENDPOINT_NAME)
        self.assert_present(MFA_SELF_SUCCESS, resp.content)
        self.session_researcher.refresh_from_db()
        self.assertIsNotNone(self.session_researcher.mfa_token)
        self.assertNotEqual(orig_mfa, self.session_researcher.mfa_token)  # change!
        self.assertIn(MFA_CREATED, session)
    
    def test_mfa_reset_without_mfa_token(self):
        session = self.client.session
        self.smart_post(mfa_password=self.DEFAULT_RESEARCHER_PASSWORD)  # magic redirect smart post
        resp = self.easy_get(self.REDIRECT_ENDPOINT_NAME)
        self.assert_present(MFA_SELF_SUCCESS, resp.content)
        self.session_researcher.refresh_from_db()
        self.assertIsNotNone(self.session_researcher.mfa_token)  # change!
        self.assertIn(MFA_CREATED, session)
    
    def test_mfa_clear_with_token(self):
        session = self.client.session
        orig_mfa = self.session_researcher.reset_mfa()
        # disable can be any non-falsy value
        self.smart_post(mfa_password=self.DEFAULT_RESEARCHER_PASSWORD, disable="true")
        resp = self.easy_get(self.REDIRECT_ENDPOINT_NAME)
        self.assert_present(MFA_SELF_DISABLED, resp.content)
        self.session_researcher.refresh_from_db()
        self.assertIsNone(self.session_researcher.mfa_token)
        self.assertNotEqual(orig_mfa, self.session_researcher.mfa_token)  # change!
        self.assertNotIn(MFA_CREATED, session)
    
    def test_mfa_clear_without_mfa_token(self):
        session = self.client.session
        # disable can be any non-falsy value
        self.smart_post(mfa_password=self.DEFAULT_RESEARCHER_PASSWORD, disable="true")
        resp = self.easy_get(self.REDIRECT_ENDPOINT_NAME)
        self.assert_present(MFA_SELF_DISABLED, resp.content)
        self.session_researcher.refresh_from_db()
        self.assertIsNone(self.session_researcher.mfa_token)  # change!
        self.assertNotIn(MFA_CREATED, session)


class TestSelfTestMFA(ResearcherSessionTest):
    ENDPOINT_NAME = "manage_researcher_endpoints.self_test_mfa"
    REDIRECT_ENDPOINT_NAME = "manage_researcher_endpoints.self_manage_credentials_page"
    
    def test_mfa_working_fails(self):
        self.session_researcher.reset_mfa()  # enable mfa
        if self.session_researcher._mfa_now == "123456":
            self.session_researcher.reset_mfa()  # ensure mfa code is not 123456
        
        self.smart_post()  # magic redirect smart post
        page = self.simple_get(easy_url("manage_researcher_endpoints.self_manage_credentials_page"), status_code=200).content
        self.assert_present(MFA_CODE_MISSING, page)  # missing mfa code
        
        self.smart_post(mfa_code="123456")  # wrong mfa code
        page = self.simple_get(easy_url("manage_researcher_endpoints.self_manage_credentials_page"), status_code=200).content
        self.assert_present(MFA_TEST_FAIL, page)
        
        self.smart_post(mfa_code="1234567")  # too long mfa code
        page = self.simple_get(easy_url("manage_researcher_endpoints.self_manage_credentials_page"), status_code=200).content
        self.assert_present(MFA_CODE_6_DIGITS, page)
        
        self.smart_post(mfa_code="abcdef")  # non-numeric mfa code
        page = self.simple_get(easy_url("manage_researcher_endpoints.self_manage_credentials_page"), status_code=200).content
        self.assert_present(MFA_CODE_DIGITS_ONLY, page)
        
        self.smart_post(mfa_code=self.session_researcher._mfa_now)  # correct mfa code
        page = self.simple_get(easy_url("manage_researcher_endpoints.self_manage_credentials_page"), status_code=200).content
        self.assert_present(MFA_TEST_SUCCESS, page)
        
        self.session_researcher.clear_mfa()  # disabled mfa
        self.smart_post(mfa_code="abcdef")
        page = self.simple_get(easy_url("manage_researcher_endpoints.self_manage_credentials_page"), status_code=200).content
        self.assert_present(MFA_TEST_DISABLED, page)


#
## The tableau stuff
#  We have ended up with multiple tests of the same endpoint after the merging formerly tableau 
#  and data access api keys. Tests still pass, tests are different, but the endpoints are the same.


class TestSelfNewAPIKeyTableau(ResearcherSessionTest):
    ENDPOINT_NAME = "manage_researcher_endpoints.self_generate_api_key"
    
    def test_generate_api_key(self):
        """ Asserts that:
            -one new api key is added to the database
            -that api key is linked to the logged in researcher
            -the correct readable name is associated with the key
            -no other api keys were created associated with that researcher
            -that api key is active and has tableau access  """
        self.assertEqual(ApiKey.objects.count(), 0)
        self.smart_post(readable_name="test_generated_api_key")
        self.assertEqual(ApiKey.objects.count(), 1)
        api_key = ApiKey.objects.get(readable_name="test_generated_api_key")
        self.assertEqual(api_key.researcher.id, self.session_researcher.id)
        self.assertTrue(api_key.is_active)


class TestSelfDisableAPIKeyTableau(TableauAPITest):
    ENDPOINT_NAME = "manage_researcher_endpoints.self_disable_api_key"
    
    def test_disable_api_key(self):
        """ Asserts that:
            -exactly one fewer active api key is present in the database
            -the api key is no longer active """
        self.assertEqual(ApiKey.objects.filter(is_active=True).count(), 1)
        self.smart_post(api_key_id=self.api_key_public)
        self.assertEqual(ApiKey.objects.filter(is_active=True).count(), 0)
        self.assertFalse(ApiKey.objects.get(access_key_id=self.api_key_public).is_active)


class TestSelfNewApiKey(ResearcherSessionTest):
    ENDPOINT_NAME = "manage_researcher_endpoints.self_generate_api_key"
    REDIRECT_ENDPOINT_NAME = "manage_researcher_endpoints.self_manage_credentials_page"
    
    # FIXME: add tests for sanitization of the input name
    def test_reset(self):
        self.assertIsNone(self.session_researcher.api_keys.first())
        self.smart_post(readable_name="new_name")
        self.assertIsNotNone(self.session_researcher.api_keys.first())
        self.assert_present(
            "New credentials have been generated for you", self.redirect_get_contents()
        )
        self.assertEqual(
            ApiKey.objects.filter(researcher=self.session_researcher,
                                  readable_name="new_name").count(), 1
        )


class TestSelfDisableApiKey(ResearcherSessionTest):
    ENDPOINT_NAME = "manage_researcher_endpoints.self_disable_api_key"
    REDIRECT_ENDPOINT_NAME = "manage_researcher_endpoints.self_manage_credentials_page"
    
    def test_disable_success(self):
        # basic test
        api_key = ApiKey.generate(
            researcher=self.session_researcher,
            readable_name="something",
        )
        self.smart_post(api_key_id=api_key.access_key_id)
        self.assertFalse(self.session_researcher.api_keys.first().is_active)
        content = self.redirect_get_contents()
        self.assert_present(api_key.access_key_id, content)
        self.assert_present("is now disabled", content)
    
    def test_no_match(self):
        # fail with empty and fail with success
        self.smart_post(api_key_id="abc")
        self.assert_present(NO_MATCHING_API_KEY, self.redirect_get_contents())
        api_key = ApiKey.generate(
            researcher=self.session_researcher,
            readable_name="something",
        )
        self.smart_post(api_key_id="abc")
        api_key.refresh_from_db()
        self.assertTrue(api_key.is_active)
        self.assert_present(NO_MATCHING_API_KEY, self.redirect_get_contents())
    
    def test_already_disabled(self):
        api_key = ApiKey.generate(
            researcher=self.session_researcher,
            readable_name="something",
        )
        api_key.update(is_active=False)
        self.smart_post(api_key_id=api_key.access_key_id)
        api_key.refresh_from_db()
        self.assertFalse(api_key.is_active)
        self.assert_present(API_KEY_IS_DISABLED, self.redirect_get_contents())

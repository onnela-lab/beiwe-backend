import logging
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import time_machine
from django.http.response import HttpResponse, HttpResponseRedirect
from django.urls import reverse
from django.utils import timezone

from constants.message_strings import (MFA_CODE_6_DIGITS, MFA_CODE_DIGITS_ONLY, MFA_CODE_MISSING,
    MFA_CODE_WRONG, MFA_CONFIGURATION_REQUIRED, MFA_CONFIGURATION_SITE_ADMIN, PASSWORD_EXPIRED,
    PASSWORD_RESET_FORCED, PASSWORD_RESET_SITE_ADMIN, PASSWORD_RESET_TOO_SHORT,
    PASSWORD_WILL_EXPIRE)
from constants.url_constants import LOGIN_REDIRECT_SAFE, urlpatterns
from constants.user_constants import EXPIRY_NAME, ResearcherRole
from database.study_models import Study
from database.system_models import GlobalSettings
from database.user_models_researcher import Researcher, ResearcherSession
from libs.utils.http_utils import easy_url
from tests.common import BasicSessionTestCase


# trunk-ignore-all(ruff/B018,bandit/B101)

#
## login_endpoints
#


class TestLoginPages(BasicSessionTestCase):
    """ Basic authentication test, make sure that the machinery for logging a user
    in and out are functional at setting and clearing a session. 
    THIS CLASS DOES NOT AUTO INSTANTIATE THE DEFAULT RESEARCHER, YOU MUST DO THAT MANUALLY. """
    
    THE_PAST = datetime(2010, 1, 1, tzinfo=timezone.utc)
    
    def test_load_login_page_while_not_logged_in(self):
        # make sure the login page loads without logging you in when it should not
        response = self.client.get(reverse("login_endpoints.login_page"))
        self.assertEqual(response.status_code, 200)
        # this should uniquely identify the login page
        self.assertIn(b'<form method="POST" action="/validate_login">', response.content)
    
    def test_load_login_page_while_logged_in(self):
        # make sure the login page loads without logging you in when it should not
        self.session_researcher  # create the default researcher
        self.do_default_login()
        response = self.client.get(reverse("login_endpoints.login_page"))
        self.assertEqual(response.status_code, 302)
        self.assert_response_url_equal(response.url, reverse("study_endpoints.choose_study_page"))
        # this should uniquely identify the login page
        self.assertNotIn(b'<form method="POST" action="/validate_login">', response.content)
    
    def test_logging_in_success(self):
        self.session_researcher  # create the default researcher
        # test last login time is recorded
        self.assertIsNone(self.session_researcher.last_login_time)
        r = self.do_default_login()
        self.assertEqual(r.status_code, 302)
        self.assert_response_url_equal(r.url, reverse("study_endpoints.choose_study_page"))
        self.session_researcher.refresh_from_db()
        self.assertIsNotNone(self.session_researcher.last_login_time)
    
    def test_logging_in_fail(self):
        r = self.do_default_login()
        self.assertEqual(r.status_code, 302)
        self.assert_response_url_equal(r.url, reverse("login_endpoints.login_page"))
    
    def test_logging_out(self):
        # create the default researcher, login, logout, attempt going to main page,
        self.session_researcher
        self.do_default_login()
        self.client.get(reverse("login_endpoints.logout_page"))
        r = self.client.get(reverse("study_endpoints.choose_study_page"))
        self.assertEqual(r.status_code, 302)
        self.assert_response_url_equal(r.url, reverse("login_endpoints.login_page"))
    
    # tests for different combinations of conditions to redirect someone to the password reset page
    # when a condition has been matched
    def test_password_reset_forced_empty_researcher(self):
        self.session_researcher.update(password_force_reset=True)
        self._test_password_reset_redirect_logic(PASSWORD_RESET_FORCED)
    
    def test_password_reset_forced_researcher_with_relation_no_details(self):
        self.session_researcher.update(password_force_reset=True)
        self.set_session_study_relation(ResearcherRole.researcher)
        self._test_password_reset_redirect_logic(PASSWORD_RESET_FORCED)
    
    def test_password_reset_forced_study_admin_with_relation_no_details(self):
        self.session_researcher.update(password_force_reset=True)
        self.set_session_study_relation(ResearcherRole.study_admin)
        self._test_password_reset_redirect_logic(PASSWORD_RESET_FORCED)
    
    def test_password_reset_forced_site_admin_with_relation_no_details(self):
        self.session_researcher.update(password_force_reset=True)
        self.set_session_study_relation(ResearcherRole.site_admin)
        self._test_password_reset_redirect_logic(PASSWORD_RESET_FORCED)
    
    def test_password_reset_forced_empty_researcher_2_studies(self):
        self.session_researcher.update(password_force_reset=True)
        self.generate_study("study 1")
        self.generate_study("study 2")
        self._test_password_reset_redirect_logic(PASSWORD_RESET_FORCED)
    
    def test_password_reset_forced_researcher_with_relation_no_details_2_studies(self):
        self.session_researcher.update(password_force_reset=True)
        self.set_session_study_relation(ResearcherRole.researcher)
        study2 = self.generate_study("study 2")
        self.generate_study_relation(self.session_researcher, study2, ResearcherRole.researcher)
        self._test_password_reset_redirect_logic(PASSWORD_RESET_FORCED)
    
    def test_password_reset_forced_study_admin_with_relation_no_details_2_studies(self):
        self.session_researcher.update(password_force_reset=True)
        self.set_session_study_relation(ResearcherRole.study_admin)
        study2 = self.generate_study("study 2")
        self.generate_study_relation(self.session_researcher, study2, ResearcherRole.study_admin)
        self._test_password_reset_redirect_logic(PASSWORD_RESET_FORCED)
    
    def test_password_reset_forced_site_admin_with_relation_no_details_2_studies(self):
        self.session_researcher.update(password_force_reset=True)
        self.set_session_study_relation(ResearcherRole.site_admin)
        self.generate_study("study 2")
        self._test_password_reset_redirect_logic(PASSWORD_RESET_FORCED)
    
    def test_password_expired(self):
        # test that there is a study with an expiration, and the password is expired
        self.session_researcher.update(password_last_changed=timezone.now() - timedelta(days=40))
        self.set_session_study_relation(ResearcherRole.researcher)
        self.session_study.update(password_max_age_enabled=True, password_max_age_days=30),
        self._test_password_reset_redirect_logic(PASSWORD_EXPIRED)
    
    def _test_password_reset_redirect_logic(self, error_message: str):
        # password reset redirects initially to the choose study page, then to the manage credentials page
        response = self.do_default_login()
        self.assert_response_url_equal(response.url, reverse("study_endpoints.choose_study_page"))
        response2 = self.client.get(response.url)
        self.assert_response_url_equal(response2.url, reverse("manage_researcher_endpoints.self_manage_credentials_page"))
        response3 = self.client.get(reverse("manage_researcher_endpoints.self_manage_credentials_page"))
        self.assertEqual(response3.status_code, 200)
        self.assert_present(error_message, response3.content)
    
    def test_password_not_even_expired(self):
        # test that there is not even a study with an expiration
        self.session_researcher.update(password_last_changed=self.THE_PAST)
        self.set_session_study_relation(ResearcherRole.researcher)
        self.session_study.update(password_max_age_enabled=False)  # its already false
        self._test_password_not_expired()
    
    def test_password_not_expired(self):
        # test that there is a study with an expiration, but the password is not expired
        self.session_researcher.update(password_last_changed=timezone.now() - timedelta(days=20))
        self.set_session_study_relation(ResearcherRole.researcher)
        self.session_study.update(password_max_age_enabled=True, password_max_age_days=30),
        self._test_password_not_expired()
    
    def test_password_almost_expired(self):
        # test that there is a study with an expiration, and the password is expired
        self.session_researcher.update(password_last_changed=timezone.now() - timedelta(days=25))
        self.set_session_study_relation(ResearcherRole.researcher)
        self.session_study.update(password_max_age_enabled=True, password_max_age_days=30),
        self._test_password_almost_expired()
    
    def test_password_almost_expired_2(self):
        # as above but with 2 studies with different password max age settings, get the lowest.
        self.session_researcher.update(password_last_changed=timezone.now() - timedelta(days=25))
        self.set_session_study_relation(ResearcherRole.researcher)
        self.session_study.update(password_max_age_enabled=True, password_max_age_days=40)
        study2 = self.generate_study("study 2")
        study2.update(password_max_age_enabled=True, password_max_age_days=30)
        self.generate_study_relation(self.session_researcher, study2, ResearcherRole.researcher)
        # ug this one bypasses all the special logic I put together and just loads the choose study
        # page with the message.
        response = self.do_default_login()
        self.assert_response_url_equal(response.url, reverse("study_endpoints.choose_study_page"))
        response2 = self.client.get(response.url)
        self.assertEqual(response2.status_code, 200)
        self.assert_present(PASSWORD_WILL_EXPIRE.format(days=5), response2.content)
        self.assert_not_present(PASSWORD_EXPIRED, response2.content)
        self.assert_not_present(PASSWORD_RESET_FORCED, response2.content)
    
    def _test_password_almost_expired(self):
        # this one redirects initially to choose study page,then to the view study page with a message.
        # expects a password to expire in 5 days
        response2 = self._test_initial_redirect()
        response3 = self.client.get(response2.url)
        self.assertEqual(response3.status_code, 200)
        self.assert_present(PASSWORD_WILL_EXPIRE.format(days=5), response3.content)
        self.assert_not_present(PASSWORD_EXPIRED, response3.content)
        self.assert_not_present(PASSWORD_RESET_FORCED, response3.content)
    
    def _test_password_not_expired(self):
        # password reset redirects initially to the choose study page, then to the view study page
        response2 = self._test_initial_redirect()
        response3 = self.client.get(response2.url)
        self.assertEqual(response3.status_code, 200)
        # none of the password messages should be visible
        self.assert_not_present(PASSWORD_WILL_EXPIRE, response3.content)
        self.assert_not_present(PASSWORD_EXPIRED, response3.content)
        self.assert_not_present(PASSWORD_RESET_FORCED, response3.content)
    
    def _test_initial_redirect(self):
        # these tests are a distraction from the details of tests that run other logic, but it
        # still needs to be tested.  Most of the complexity is in determine_2nd_redirect
        response = self.do_default_login()
        self.assert_response_url_equal(response.url, reverse("study_endpoints.choose_study_page"))
        response2 = self.client.get(response.url)
        # its not always a redirect, sometimes it actually loads the page, but if it is a redirect
        # we run the logic to test it.
        self.assert_response_url_equal(response2.url, self.determine_2nd_redirect)
        return response2
    
    @property
    def determine_2nd_redirect(self):
        # when there is 1 study redirect to that view study page, otherwise show the choose study
        # page (the login page always says redirect to choose_study, the logic to then redirect is
        # part of the choose study endpoint)
        # 2 cases: site admin, or not site admin
        if self.session_researcher.site_admin:
            if Study.objects.count() == 1:
                return easy_url("study_endpoints.view_study_page", study_id=self.session_study.id)
            else:
                return reverse("study_endpoints.choose_study_page")
        if self.session_researcher.study_relations.count() == 1:
            return easy_url("study_endpoints.view_study_page", study_id=self.session_study.id)
        else:
            return reverse("study_endpoints.choose_study_page")
    
    def test_password_redirect_ignored_endpoint_manage_credentials(self):
        # test that the password redirect is ignored for manage credentials page
        self.session_researcher
        self.do_default_login()
        self.session_researcher.update(password_force_reset=True)
        resp = self.client.get(reverse("manage_researcher_endpoints.self_manage_credentials_page"))
        self.assertIsInstance(resp, HttpResponse)
        self.assertEqual(resp.status_code, 200)
    
    def test_password_redirect_ignored_endpoint_logout(self):
        # test that the password redirect is ignored logout endpoint
        self.session_researcher
        self.do_default_login()
        self.session_researcher.update(password_force_reset=True)
        resp = self.client.get(reverse("login_endpoints.logout_page"))
        self.assertIsInstance(resp, HttpResponseRedirect)
        self.assertEqual(resp.status_code, 302)
        self.assertEqual(resp.url, reverse("login_endpoints.login_page"))  # that's the / endpoint
    
    def test_password_redirect_ignored_endpoint_reset_password(self):
        # test that the password redirect is ignored for the set password endpoint
        self.session_researcher
        self.do_default_login()
        self.session_researcher.update(password_force_reset=True)
        resp = self.client.post(reverse("manage_researcher_endpoints.self_change_password"))
        self.assertIsInstance(resp, HttpResponse)
        self.assertEqual(resp.status_code, 400)
    
    def test_admin_decorator(self):
        # We don't have an integration test for determine_password_reset_redirect, this is kind of a
        # proxy test that the already-tested logic of the password reset logic is included in the
        # authenticate_admin decorator
        self.session_researcher.update(password_force_reset=True)
        self.set_session_study_relation(ResearcherRole.site_admin)
        self.do_default_login()
        name = self.session_study.name
        resp = self.client.post(easy_url("manage_study_endpoints.rename_study", study_id=self.session_study.id))
        self.assertEqual(resp.status_code, 302)
        self.assertEqual(resp.url, reverse("manage_researcher_endpoints.self_manage_credentials_page"))
        self.session_study.refresh_from_db()
        self.assertEqual(self.session_study.name, name)  # assert name didn't change
    
    def test_session_expiry(self):
        # set up the current time, then we will travel into the future to test the session expiry
        # in a loop, hour by hour, testing that we redirect to the login page after 18 hours
        self.session_researcher
        THE_TIMEOUT_HOURS_VARIABLE_YOU_ARE_LOOKING_FOR = 2
        start = datetime.now()
        check_1_happened = False
        check_2_happened = False
        for hour in range(0, 24):
            self.do_default_login()
            # the +1 minute ensures we are passing the hour mark
            with time_machine.travel(start + timedelta(hours=hour, minutes=1)):
                if hour < THE_TIMEOUT_HOURS_VARIABLE_YOU_ARE_LOOKING_FOR:
                    resp = self.client.get(reverse("study_endpoints.choose_study_page"))
                    self.assertEqual(resp.status_code, 200, msg=f"hour={hour}")
                    check_1_happened = True
                else:
                    resp = self.client.get(reverse("study_endpoints.choose_study_page"))
                    self.assertEqual(resp.status_code, 302, msg=f"hour={hour}")
                    self.assertEqual(resp.url, reverse("login_endpoints.login_page"))
                    check_2_happened = True
        # make sure we actually tested both cases
        self.assertTrue(check_1_happened)
        self.assertTrue(check_2_happened)
    
    def test_sub_ten_second_whatever_the_opposite_of_a_grace_period_is(self):
        # we have a feature where within 10 seconds of your logout period your session will NOT be
        # extended.  This feature exists to allow us to have a you-will-be-logged-out mechanism.
        self.assertEqual(ResearcherSession.objects.count(), 0)  # check nothing weird in db.
        self.session_researcher
        self.do_default_login()
        original_expiry = self.client.session[EXPIRY_NAME]
        # assert session is currently 2 hours in the future (yep you have to update this test too)
        self.assertGreater(original_expiry, timezone.now() + timedelta(hours=1, minutes=59))
        self.assertLess(original_expiry, timezone.now() + timedelta(hours=2, minutes=1))
        # NOPE you can't do this
        # This doesn't work, manipulating the ssession directly is hard the assertEqual fails.
        #  within_ten_second_expiry = timezone.now() + timedelta(seconds=9)
        #  self.client.session[EXPIRY_NAME] = ten_second_expiry
        #  self.client.session.save()
        #  self.assertEqual(self.client.session[EXPIRY_NAME], ten_second_expiry)
        # So its easier time travel to the future to test this.
        # within 10 seconds:
        with time_machine.travel(original_expiry - timedelta(seconds=9)):
            # hit a page, confirm check expiry didn't change
            resp = self.client.get(reverse("study_endpoints.choose_study_page"))
            self.assertEqual(resp.status_code, 200)
            new_expiry = self.client.session[EXPIRY_NAME]
            self.assertEqual(new_expiry, original_expiry)
        # after 10 seconds:
        with time_machine.travel(original_expiry + timedelta(seconds=1)):
            # hit a page, confirm check expiry deleted
            resp: HttpResponseRedirect = self.easy_get("study_endpoints.choose_study_page")
            self.assertEqual(resp.status_code, 302)
            self.assertEqual(resp.url, easy_url("login_endpoints.login_page"))
            try:
                self.client.session[EXPIRY_NAME]
                self.fail("session expiry should have been deleted")
            except KeyError as e:
                self.assertEqual(e.args[0], EXPIRY_NAME)
    
    def test_password_too_short_site_admin(self):
        # test that the password too short redirect applies to admin endpoints
        self.assertEqual(
            self.session_researcher.password_min_length, len(self.DEFAULT_RESEARCHER_PASSWORD)
        )
        self.session_researcher.update_only(password_min_length=8)
        self.set_session_study_relation(ResearcherRole.site_admin)
        self.do_default_login()
        # random endpoint that will trigger a redirect
        resp = self.client.post(easy_url("manage_study_endpoints.rename_study", study_id=self.session_study.id))
        self.assertEqual(resp.status_code, 302)
        self.assertEqual(resp.url, reverse("manage_researcher_endpoints.self_manage_credentials_page"))
        page = self.simple_get(resp.url, status_code=200).content
        self.assert_present(PASSWORD_RESET_SITE_ADMIN, page)
        # assert that this behavior does not rely on the force reset flag
        self.assertFalse(self.session_researcher.password_force_reset)
    
    def test_password_too_short_researcher(self):
        # test that the password too short redirect applies to admin endpoints
        self.assertEqual(
            self.session_researcher.password_min_length, len(self.DEFAULT_RESEARCHER_PASSWORD)
        )
        self.session_study.update_only(password_minimum_length=20)
        self.session_researcher.update_only(password_min_length=8)
        self.set_session_study_relation(ResearcherRole.researcher)
        self.do_default_login()
        # random endpoint that will trigger a redirect
        resp = self.simple_get(easy_url("study_endpoints.choose_study_page"))
        self.assertEqual(resp.status_code, 302)
        self.assertEqual(resp.url, reverse("manage_researcher_endpoints.self_manage_credentials_page"))
        page = self.simple_get(resp.url, status_code=200).content
        self.assert_present(PASSWORD_RESET_TOO_SHORT, page)
        # assert that this behavior does not rely on the force reset flag
        self.assertFalse(self.session_researcher.password_force_reset)
    
    def test_password_too_short_bad_state(self):
        # we got an error report from the Researcher.check_password call inside the validate_login
        # function, a validation error password too short message.  Cannot reproduce.
        self.session_researcher._force_set_password("2short")
        # have to bypass the password min length check validator to set up bad database state.
        # default password minimum length is 8, 7 is the highest number to cause the redirect.
        Researcher.objects.filter(id=self.session_researcher.id).update(password_min_length=7)
        self.do_login(self.DEFAULT_RESEARCHER_NAME, "2short")
        
        # random endpoint that will trigger a redirect
        resp = self.simple_get(easy_url("study_endpoints.choose_study_page"))
        self.assertEqual(resp.status_code, 302)
        self.assertEqual(resp.url, reverse("manage_researcher_endpoints.self_manage_credentials_page"))
        page = self.simple_get(resp.url, status_code=200).content
        self.assert_present(PASSWORD_RESET_TOO_SHORT, page)
        # assert that this behavior does not rely on the force reset flag
        self.assertFalse(self.session_researcher.password_force_reset)
    
    def test_force_logout(self):
        self.session_researcher
        resp = self.do_default_login()
        self.assertEqual(resp.status_code, 302)
        self.assertEqual(resp.url, reverse("study_endpoints.choose_study_page"))
        self.simple_get(resp.url, status_code=200)  # page loads as normal
        self.session_researcher.force_global_logout()
        resp = self.simple_get(resp.url, status_code=302)  # page redirects to login
        self.session_researcher.refresh_from_db()
        self.assertEqual(self.session_researcher.web_sessions.count(), 0)
        self.assertEqual(resp.url, reverse("login_endpoints.login_page"))
    
    def test_mfa_login(self):
        self.session_researcher.reset_mfa()  # enable mfa
        if self.session_researcher._mfa_now == "123456":
            self.session_researcher.reset_mfa()  # ensure mfa code is not 123456
        
        r1 = self.do_default_login()
        self.assertEqual(r1.status_code, 302)  # assert login failure
        self.assertEqual(r1.url, "/")
        the_login_page = self.simple_get("/", status_code=200).content
        self.assert_present(MFA_CODE_MISSING, the_login_page)  # missing mfa
        self.do_login(
            self.DEFAULT_RESEARCHER_NAME, self.DEFAULT_RESEARCHER_PASSWORD, mfa_code="123456"
        )  # wrong mfa code
        the_login_page = self.simple_get("/", status_code=200).content
        self.assert_present(MFA_CODE_WRONG, the_login_page)
        self.do_login(
            self.DEFAULT_RESEARCHER_NAME, self.DEFAULT_RESEARCHER_PASSWORD, mfa_code="1234567"
        )  # too long mfa code
        the_login_page = self.simple_get("/", status_code=200).content
        self.assert_present(MFA_CODE_6_DIGITS, the_login_page)
        self.do_login(
            self.DEFAULT_RESEARCHER_NAME, self.DEFAULT_RESEARCHER_PASSWORD, mfa_code="abcdef"
        )  # non-numeric mfa code
        the_login_page = self.simple_get("/", status_code=200).content
        self.assert_present(MFA_CODE_DIGITS_ONLY, the_login_page)
    
    def test_mfa_required(self):
        self.session_researcher
        self.do_default_login()
        self.default_study.update_only(mfa_required=True)  # enable mfa
        self.set_session_study_relation()  # ensure researcher is on study
        resp = self.simple_get(
            easy_url("study_endpoints.choose_study_page"), status_code=302
        )  # page redirects
        self.assertEqual(resp.url, reverse("manage_researcher_endpoints.self_manage_credentials_page"))
        resp = self.simple_get(resp.url, status_code=200)  # page loads as normal
        self.assert_present(MFA_CONFIGURATION_REQUIRED, resp.content)
    
    @patch("authentication.admin_authentication.REQUIRE_SITE_ADMIN_MFA")
    @patch("database.user_models_researcher.REQUIRE_SITE_ADMIN_MFA")
    def test_mfa_required_site_admin_setting_only_affects_site_admins(
        self, patch1: MagicMock, patch2: MagicMock
    ):
        patch1.return_value = True
        patch2.return_value = True
        self.session_researcher.update(mfa_token=None)
        r1 = self.do_default_login()
        self.assertEqual(r1.status_code, 302)  # assert login failure
        # it redirects to choose study, choose study loads
        self.assertEqual(r1.url, easy_url("study_endpoints.choose_study_page"))
    
    @patch("authentication.admin_authentication.REQUIRE_SITE_ADMIN_MFA")
    @patch("database.user_models_researcher.REQUIRE_SITE_ADMIN_MFA")
    def test_mfa_required_site_admin_setting(self, patch1: MagicMock, patch2: MagicMock):
        patch1.return_value = True
        patch2.return_value = True
        self.session_researcher.update(mfa_token=None, site_admin=True)
        r1 = self.do_default_login()
        self.assertEqual(r1.status_code, 302)  # assert login failure
        # it redirects to choose study, then choose study should redirect to manage credentials
        self.assertEqual(r1.url, easy_url("study_endpoints.choose_study_page"))
        r2 = self.simple_get(
            easy_url("study_endpoints.choose_study_page"), status_code=302
        )  # page redirects
        self.assertEqual(r2.url, reverse("manage_researcher_endpoints.self_manage_credentials_page"))  # correct redirect
        r3 = self.simple_get(r2.url, status_code=200)  # page loads as normal
        self.assert_present(MFA_CONFIGURATION_REQUIRED, r3.content)
        self.assert_present(MFA_CONFIGURATION_SITE_ADMIN, r3.content)


class TestDowntime(BasicSessionTestCase):
    """ Tests our very basic downtime middleware """
    
    def test_downtime(self):
        # this test emits a logging statement `ERROR:django.request:Service Unavailable: /`
        # that we want to squash, but we want to set logging level back to normal when we are done.
        previous_logging_level = logging.getLogger("django.request").level
        try:
            logging.getLogger("django.request").setLevel(logging.CRITICAL)
            GlobalSettings.singleton().update(downtime_enabled=False)
            self.easy_get("login_endpoints.login_page", status_code=200)
            GlobalSettings.singleton().update(downtime_enabled=True)
            self.easy_get("login_endpoints.login_page", status_code=503)
            GlobalSettings.singleton().update(downtime_enabled=False)
            self.easy_get("login_endpoints.login_page", status_code=200)
        except Exception:
            raise
        finally:
            logging.getLogger("django.request").setLevel(previous_logging_level)


class TestResearcherRedirectionLogic(BasicSessionTestCase):
    # This needs to be comprehensive. It is checked for validity in one test and then used in the other.
    # This is a set because there are 2 entries for every endpoint, with and without slashes.
    LOCAL_COPY_WHITELIST = set(
        [
            "study_endpoints.view_study_page",
            "data_page_endpoints.dashboard_page",
            "data_page_endpoints.get_data_for_dashboard_datastream_display",
            "data_page_endpoints.dashboard_participant_page",
            "data_page_endpoints.data_api_web_form_page",
            "forest_endpoints.forest_tasks_progress",
            "forest_endpoints.task_log",
            "participant_endpoints.notification_history",
            "participant_endpoints.participant_page",
            "manage_study_endpoints.interventions_page",
            "manage_study_endpoints.study_fields",
            "survey_endpoints.render_edit_survey",
            "study_endpoints.device_settings",
            "manage_researcher_endpoints.administrator_edit_researcher_page",
            "study_endpoints.edit_study",
            "system_admin_endpoints.manage_firebase_credentials",
            "manage_researcher_endpoints.administrator_manage_researchers_page",
            "study_endpoints.manage_studies_page",
            "study_endpoints.study_security_page",
            "participant_endpoints.experiments_page",
        ]
    )
    
    @property
    def urls(self):
        # a list of ~valid test urls for every single url in LOGIN_REDIRECT_SAFE.
        # (these urls must start with a slash, ending slash shouldn't matter)
        return [
            f"/dashboard/{self.session_study.id}",
            f"/dashboard/{self.session_study.id}/patient/{self.default_participant.id}",
            f"/dashboard/{self.session_study.id}/data_stream/gps",
            "/data_access_web_form/",
            f"/device_settings/{self.session_study.id}",
            f"/edit_researcher/{self.session_researcher.id}",  # technically an invalid url on page load
            f"/edit_study/{self.session_study.id}",
            f"/edit_study_security/{self.session_study.id}",
            f"/edit_survey/{self.default_study.id}/{self.default_survey.id}",
            f"/interventions/{self.session_study.id}",
            # "/manage_credentials/",  # can't be in this list due to redirect safety sorta
            "/manage_firebase_credentials/",
            "/manage_researchers/",
            "/manage_studies/",  # url with both slashes
            "/manage_studies",  # url with no trailing slash
            "manage_studies/",  # url with no leading slash
            "manage_studies",  # url with no slashes
            f"/study_fields/{self.session_study.id}",
            f"/view_study/{self.session_study.id}",
            f'/view_study/{self.session_study.id}/participant/{self.default_participant.id}',
            f'/view_study/{self.session_study.id}/participant/{self.default_participant.id}/notification_history',
            f'/studies/{self.session_study.id}/forest/progress/',
            f'/studies/{self.session_study.id}/forest/tasks/',
            f'/view_study/{self.session_study.id}/participant/{self.default_participant.id}/experiments',
        ]
    
    @property
    def a_valid_redirect_url(self):
        return f"/edit_study/{self.default_study.id}"
    
    def assert_url_match(self, url: str, resp: HttpResponse):
        try:
            self._assertEqual(url, resp.url)
        except AssertionError:
            self._assertEqual("/" + url, resp.url)
    
    def test_page_list_is_correct(self):
        # Now go create an explicit test for that page. These tests exist to ensure we don't have
        # code rot on this feature.
        endpoint_names = set(urlpattern.name for urlpattern in LOGIN_REDIRECT_SAFE)
        self.assertEqual(self.LOCAL_COPY_WHITELIST, endpoint_names)
        
        # Check that every endpoint is in the whitelist, starts with a slash.
        endpoint_names = set()
        for url in self.urls:
            try:
                assert url.startswith("/"), f"url '{url}' does not start with a slash"
            except AssertionError:
                # case - we have a mildly illegal url that needs to be tested along with the others
                assert url in ("manage_studies/", "manage_studies")
            
            found_something = False
            for urlpattern in urlpatterns:
                if urlpattern.pattern.match(url.lstrip("/")):
                    endpoint_names.add(urlpattern.name)
                    found_something = True
            assert found_something, f"no urlpattern matched url '{url}'"
        
        # test that the list of url _names_ matches the whitelist so that you get a very convenient
        # error message stating what is missing in name form! :D
        self.assertEqual(self.LOCAL_COPY_WHITELIST, endpoint_names)
    
    def test_login_page_referral_but_with_most_recent_page(self):
        self.session_researcher.update_only(most_recent_page=self.a_valid_redirect_url)
        self.test_login_page_referral()
    
    def test_login_page_referral(self):
        # self.session_researcher.update_only(most_recent_page=url)
        self.session_researcher.update_only(site_admin=True)  # make sure we have permissions...
        self.do_researcher_logout()
        
        # test that every url has the parameterized referrer url, then test that the page loads with
        # the value embedded with the referrer post parameter.
        for url in self.urls + [self.a_valid_redirect_url]:
            resp = self.client.get(url)
            # case: you actually do need the url to be valid to get a redirect To The Login page
            if url[0] != "/":
                self.assertEqual(resp.status_code, 404)
                continue
            self.assertEqual(
                resp.url, "/?page=/" + url.lstrip("/")
            )  # ensure there is a leading slash
            page = resp.client.get(resp.url).content
            self.assert_present( # ensure there is a leading slash
                f'<input type="hidden" name="referrer" value="/{url.lstrip("/")}" />', page
            )
            # test that the negative tests be based in reality
            self.assert_present('name="referrer"', page)
        
        # whn there is no change to the url we don't get a url attribute
        # test junk
        resp = self.client.get("/?page=literally junk")
        self.assertFalse(hasattr(resp, "url"))
        self.assert_not_present('name="referrer"', resp.content)
        # test blank
        resp = self.client.get("/?page=")
        self.assertFalse(hasattr(resp, "url"))
        self.assert_not_present('name="referrer"', resp.content)
        # test "/"
        resp = self.client.get("/?page=/")
        self.assertFalse(hasattr(resp, "url"))
        self.assert_not_present('name="referrer"', resp.content)
        # test a url that doesn't redirect because its not on the whitelist
        resp = self.client.get("/?page=/manage_credentials/")
        self.assertFalse(hasattr(resp, "url"))
        self.assert_not_present('name="referrer"', resp.content)
        # test a url that doesn't redirect because its on the redirects-ignore list
        resp = self.client.get("/?page=/reset_download_api_credentials/")
        self.assertFalse(hasattr(resp, "url"))
        self.assert_not_present('name="referrer"', resp.content)
        # test an injection on a valid page
        resp = self.client.get(f"/?page={self.a_valid_redirect_url}'><script>alert('hi')</script>")
        self.assertFalse(hasattr(resp, "url"))
        self.assert_not_present('name="referrer"', resp.content)
        # test html escaped version of a valid page
        resp = self.client.get(f"/?page={self.a_valid_redirect_url}".replace("_", r"%5f"))
        self.assertFalse(hasattr(resp, "url"))
        self.assert_not_present('name="referrer"', resp.content)
    
    def test_redirect_works_on_all_valid_pages_probably(self):
        self.session_researcher.update_only(site_admin=True)  # make sure we have permissions...
        
        # test that every url actually works with the redirect.
        for url in self.urls:
            self.session_researcher.update_only(most_recent_page=url)
            resp = self.do_default_login()
            self.do_researcher_logout()
            self.assert_url_match(url, resp)
    
    def test_redirect_failures(self):
        self.session_researcher.update_only(site_admin=True)  # make sure we have permissions...
        
        # test junk doesn't crash it
        self.session_researcher.update_only(most_recent_page="literally junk")
        resp = self.do_default_login()
        self.do_researcher_logout()
        self.assertEqual(resp.url, "/choose_study")
        
        # test blank
        self.session_researcher.update_only(most_recent_page="")
        resp = self.do_default_login()
        self.do_researcher_logout()
        self.assertEqual(resp.url, "/choose_study")
        
        # test None
        self.session_researcher.update_only(most_recent_page=None)
        resp = self.do_default_login()
        self.do_researcher_logout()
        self.assertEqual(resp.url, "/choose_study")
        
        # test '/' ('/' is a valid url that shouldn't redirect but is also a weird one I guess?)
        self.session_researcher.update_only(most_recent_page="/")
        resp = self.do_default_login()
        self.do_researcher_logout()
        self.assertEqual(resp.url, "/choose_study")
        
        # test for an endpoint that DOESN'T redirect because its on the redirects-ignore list
        self.session_researcher.update_only(most_recent_page="/manage_credentials/")
        resp = self.do_default_login()
        self.do_researcher_logout()
        self.assertEqual(resp.url, "/choose_study")
        
        # test a valid url that doesn't redirect because its not on the whitelist
        self.session_researcher.update_only(most_recent_page="/reset_download_api_credentials/")
        resp = self.do_default_login()
        self.do_researcher_logout()
        self.assertEqual(resp.url, "/choose_study")
    
    def test_forwarding_works_on_all_valid_pages_no_most_recent_page(self):
        self.session_researcher.update_only(site_admin=True)  # make sure we have permissions...
        self.session_researcher.update_only(most_recent_page=None)  # disable
        # test that every url actually works with the redirect.
        for url in self.urls:
            resp = self.do_default_login(referrer=url)
            self.do_researcher_logout()
            # starting slashes get inserted by the redirect logic, assert exact or missing slash matches
            self.assert_url_match(url, resp)
    
    def test_forwarding_works_on_all_valid_pages_overrides_most_recent_page(self):
        self.session_researcher.update_only(site_admin=True)  # make sure we have permissions...
        self.session_researcher.update_only(most_recent_page=self.a_valid_redirect_url)
        # test that every url actually works with the redirect.
        for url in self.urls:
            resp = self.do_default_login(referrer=url)
            self.do_researcher_logout()
            self.assert_url_match(url, resp)
    
    def test_forwarding_fails(self):
        self.session_researcher.update_only(site_admin=True)  # make sure we have permissions...
        self.session_researcher.update_only(most_recent_page=None)
        
        # test junk doesn't crash it
        resp = self.do_default_login(referrer="literally junk")
        self.do_researcher_logout()
        self.assertEqual(resp.url, "/choose_study")
        
        # test blank
        resp = self.do_default_login(referrer="")
        self.do_researcher_logout()
        self.assertEqual(resp.url, "/choose_study")
        
        # test '/' ('/' is a valid url that shouldn't redirect but is also a weird one I guess?)
        resp = self.do_default_login(referrer="/")
        self.do_researcher_logout()
        self.assertEqual(resp.url, "/choose_study")
        
        # test for an endpoint that DOESN'T redirect because its on the redirects-ignore list
        resp = self.do_default_login(referrer="/manage_credentials/")
        self.do_researcher_logout()
        self.assertEqual(resp.url, "/choose_study")
        
        # test a valid url that doesn't redirect because its not on the whitelist
        resp = self.do_default_login(referrer="/reset_download_api_credentials/")
        self.do_researcher_logout()
        self.assertEqual(resp.url, "/choose_study")
    
    def test_forwarding_fails_with_most_recent_page(self):
        self.session_researcher.update_only(site_admin=True)  # make sure we have permissions...
        redirect_page = self.a_valid_redirect_url
        self.session_researcher.update_only(most_recent_page=redirect_page)
        
        # test junk doesn't crash it
        resp = self.do_default_login(referrer="literally junk")
        self.do_researcher_logout()
        self.assertEqual(resp.url, redirect_page)
        
        # test blank
        resp = self.do_default_login(referrer="")
        self.do_researcher_logout()
        self.assertEqual(resp.url, redirect_page)
        
        # test '/' ('/' is a valid url that shouldn't redirect but is also a weird one I guess?)
        resp = self.do_default_login(referrer="/")
        self.do_researcher_logout()
        self.assertEqual(resp.url, redirect_page)
        
        # test for an endpoint that DOESN'T redirect because its on the redirects-ignore list
        resp = self.do_default_login(referrer="/manage_credentials/")
        self.do_researcher_logout()
        self.assertEqual(resp.url, redirect_page)
        
        # test a valid url that doesn't redirect because its not on the whitelist
        resp = self.do_default_login(referrer="/reset_download_api_credentials/")
        self.do_researcher_logout()
        self.assertEqual(resp.url, redirect_page)

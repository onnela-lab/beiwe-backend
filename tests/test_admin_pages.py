from datetime import timedelta

import time_machine
from django.http import HttpResponseRedirect
from django.utils import timezone

from constants.message_strings import (API_KEY_IS_DISABLED, MFA_CODE_6_DIGITS, MFA_CODE_DIGITS_ONLY,
    MFA_CODE_MISSING, MFA_SELF_BAD_PASSWORD, MFA_SELF_DISABLED, MFA_SELF_NO_PASSWORD,
    MFA_SELF_SUCCESS, MFA_TEST_DISABLED, MFA_TEST_FAIL, MFA_TEST_SUCCESS, NEW_PASSWORD_MISMATCH,
    NEW_PASSWORD_N_LONG, NEW_PASSWORD_RULES_FAIL, NO_MATCHING_API_KEY, PASSWORD_RESET_SUCCESS,
    WRONG_CURRENT_PASSWORD)
from constants.security_constants import MFA_CREATED
from constants.user_constants import EXPIRY_NAME, ResearcherRole
from database.security_models import ApiKey
from libs.http_utils import easy_url
from tests.common import ResearcherSessionTest, TableauAPITest


class TestManageCredentials(ResearcherSessionTest):
    ENDPOINT_NAME = "admin_pages.manage_credentials"
    
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


class TestResetAdminPassword(ResearcherSessionTest):
    # test for every case and messages present on the page
    ENDPOINT_NAME = "admin_pages.researcher_change_my_password"
    REDIRECT_ENDPOINT_NAME = "admin_pages.manage_credentials"
    
    def test_researcher_change_my_password_success(self):
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
        self.assertEqual(resp.url, easy_url("login_pages.login_page"))
        resp = self.easy_get("login_pages.login_page", 200)
        self.assert_present(PASSWORD_RESET_SUCCESS, resp.content)
        
        # and in december 2023 added the auto logout.
        # test that the 10 second session timeout is working
        now = timezone.now()
        self.assertLess(self.session_researcher.password_last_changed, now + timedelta(seconds=10))
        with time_machine.travel(timezone.now() + timedelta(seconds=11)):
            # hit a page, confirm check expiry deleted
            resp: HttpResponseRedirect = self.easy_get("study_endpoints.choose_study_page")
            self.assertEqual(resp.status_code, 302)
            self.assertEqual(resp.url, easy_url("login_pages.login_page"))
            try:
                final_expiry = self.client.session[EXPIRY_NAME]
                self.fail("session expiry should have been deleted")
            except KeyError as e:
                self.assertEqual(e.args[0], EXPIRY_NAME)
    
    def test_researcher_change_my_password_wrong(self):
        self.smart_post(
            current_password=self.DEFAULT_RESEARCHER_PASSWORD + "1",
            new_password=self.DEFAULT_RESEARCHER_PASSWORD + "1",
            confirm_new_password=self.DEFAULT_RESEARCHER_PASSWORD + "1",
        )
        r = self.session_researcher
        self.assertTrue(r.check_password(r.username, self.DEFAULT_RESEARCHER_PASSWORD))
        self.assertFalse(r.check_password(r.username, self.DEFAULT_RESEARCHER_PASSWORD + "1"))
        self.assert_present(WRONG_CURRENT_PASSWORD, self.redirect_get_contents())
    
    def test_researcher_change_my_password_rules_fail(self):
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
    
    def test_researcher_change_my_password_too_short(self):
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
    
    def test_researcher_change_my_password_too_short_study_setting(self):
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
    
    def test_researcher_change_my_password_too_short_site_admin(self):
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
    
    def test_researcher_change_my_password_mismatch(self):
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


class TestResetMFASelf(ResearcherSessionTest):
    ENDPOINT_NAME = "admin_pages.reset_mfa_self"
    REDIRECT_ENDPOINT_NAME = "admin_pages.manage_credentials"
    
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


class TestTestMFA(ResearcherSessionTest):
    ENDPOINT_NAME = "admin_pages.test_mfa"
    REDIRECT_ENDPOINT_NAME = "admin_pages.manage_credentials"
    
    def test_mfa_working_fails(self):
        self.session_researcher.reset_mfa()  # enable mfa
        if self.session_researcher._mfa_now == "123456":
            self.session_researcher.reset_mfa()  # ensure mfa code is not 123456
        
        self.smart_post()  # magic redirect smart post
        page = self.simple_get(easy_url("admin_pages.manage_credentials"), status_code=200).content
        self.assert_present(MFA_CODE_MISSING, page)  # missing mfa code
        
        self.smart_post(mfa_code="123456")  # wrong mfa code
        page = self.simple_get(easy_url("admin_pages.manage_credentials"), status_code=200).content
        self.assert_present(MFA_TEST_FAIL, page)
        
        self.smart_post(mfa_code="1234567")  # too long mfa code
        page = self.simple_get(easy_url("admin_pages.manage_credentials"), status_code=200).content
        self.assert_present(MFA_CODE_6_DIGITS, page)
        
        self.smart_post(mfa_code="abcdef")  # non-numeric mfa code
        page = self.simple_get(easy_url("admin_pages.manage_credentials"), status_code=200).content
        self.assert_present(MFA_CODE_DIGITS_ONLY, page)
        
        self.smart_post(mfa_code=self.session_researcher._mfa_now)  # correct mfa code
        page = self.simple_get(easy_url("admin_pages.manage_credentials"), status_code=200).content
        self.assert_present(MFA_TEST_SUCCESS, page)
        
        self.session_researcher.clear_mfa()  # disabled mfa
        self.smart_post(mfa_code="abcdef")
        page = self.simple_get(easy_url("admin_pages.manage_credentials"), status_code=200).content
        self.assert_present(MFA_TEST_DISABLED, page)


#
## The tableau stuff
#  We have ended up with multiple tests of the same endpoint after the merging formerly tableau 
#  and data access api keys. Tests still pass, tests are different, but the endpoints are the same.


class TestNewAPIKeyTableau(ResearcherSessionTest):
    ENDPOINT_NAME = "admin_pages.new_api_key"
    
    def test_new_api_key(self):
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


class TestDisableAPIKeyTableau(TableauAPITest):
    ENDPOINT_NAME = "admin_pages.disable_api_key"
    
    def test_disable_api_key(self):
        """ Asserts that:
            -exactly one fewer active api key is present in the database
            -the api key is no longer active """
        self.assertEqual(ApiKey.objects.filter(is_active=True).count(), 1)
        self.smart_post(api_key_id=self.api_key_public)
        self.assertEqual(ApiKey.objects.filter(is_active=True).count(), 0)
        self.assertFalse(ApiKey.objects.get(access_key_id=self.api_key_public).is_active)


class TestNewApiKey(ResearcherSessionTest):
    ENDPOINT_NAME = "admin_pages.new_api_key"
    REDIRECT_ENDPOINT_NAME = "admin_pages.manage_credentials"
    
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


# admin_pages.disable_api_key
class TestDisableTableauApiKey(ResearcherSessionTest):
    ENDPOINT_NAME = "admin_pages.disable_api_key"
    REDIRECT_ENDPOINT_NAME = "admin_pages.manage_credentials"
    
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

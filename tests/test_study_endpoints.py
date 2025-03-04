# trunk-ignore-all(ruff/B018)
from copy import copy
from datetime import date, timedelta
from io import BytesIO
from unittest.mock import MagicMock, patch

import orjson
from django.db import models
from django.http.response import FileResponse, HttpResponse, HttpResponseRedirect
from django.utils import timezone

from constants.message_strings import DEVICE_SETTINGS_RESEND_FROM_0
from constants.testing_constants import ADMIN_ROLES, ALL_TESTING_ROLES
from constants.user_constants import ResearcherRole
from database.models import ScheduledEvent
from database.study_models import DeviceSettings, Study
from libs.endpoint_helpers.copy_study_helpers import format_study
from libs.utils.http_utils import easy_url
from tests.common import ResearcherSessionTest


class TestChooseStudy(ResearcherSessionTest):
    ENDPOINT_NAME = "study_endpoints.choose_study_page"
    # these tests tost behavior of redirection without anything in the most_recent_page tracking
    # or as forwarding from the login page via the referrer url parameter into the post parameter
    
    def test_2_studies(self):
        study2 = self.generate_study("study2")
        self.set_session_study_relation(ResearcherRole.researcher)
        self.generate_study_relation(self.session_researcher, study2, ResearcherRole.researcher)
        resp = self.smart_get_status_code(200)
        self.assert_present(self.session_study.name, resp.content)
        self.assert_present(study2.name, resp.content)
    
    def test_1_study(self):
        self.set_session_study_relation(ResearcherRole.researcher)
        resp = self.smart_get_status_code(302)
        self.assertEqual(
            resp.url, easy_url("study_endpoints.view_study_page", study_id=self.session_study.id)
        )
    
    def test_no_study(self):
        self.set_session_study_relation(None)
        resp = self.smart_get_status_code(200)
        self.assert_not_present(self.session_study.name, resp.content)


class TestViewStudy(ResearcherSessionTest):
    """ view_study is pretty simple, no custom content in the :
    tests push_notifications_enabled, study.forest_enabled
    populates html elements with custom field values
    populates html elements of survey buttons """
                                             
    ENDPOINT_NAME = "study_endpoints.view_study_page"
    
    def test_view_study_no_relation(self):
        self.smart_get_status_code(403, self.session_study.id)
    
    def test_view_study_researcher(self):
        # pretty much just tests that the page loads, removing is_test removed template customizations.
        study = self.session_study
        self.set_session_study_relation(ResearcherRole.researcher)
        self.smart_get_status_code(200, study.id)
    
    def test_view_study_study_admin(self):
        self.set_session_study_relation(ResearcherRole.study_admin)
        self.smart_get_status_code(200, self.session_study.id)
    
    @patch('endpoints.study_endpoints.check_firebase_instance')
    def test_view_study_site_admin(self, check_firebase_instance: MagicMock):
        study = self.session_study
        self.set_session_study_relation(ResearcherRole.site_admin)
    
        # test rendering with several specific values set to observe the rendering changes
        study.update(forest_enabled=False)
        check_firebase_instance.return_value = False
        response = self.smart_get_status_code(200, study.id)
        self.assertNotIn(
            b"Configure Interventions for use with Relative survey schedules", response.content
        )
        self.assertNotIn(b"View Forest Task Log", response.content)
    
        check_firebase_instance.return_value = True
        study.update(forest_enabled=True)
        response = self.smart_get_status_code(200, study.id)
        self.assertIn(
            b"Configure Interventions for use with Relative survey schedules", response.content
        )
        self.assertIn(b"View Forest Task Log", response.content)
        # assertInHTML is several hundred times slower but has much better output when it fails...
        # self.assertInHTML("Configure Interventions for use with Relative survey schedules", response.content.decode())


class TestManageStudies(ResearcherSessionTest):
    """ All we do with this page is make sure it loads... there isn't much to hook onto and
    determine a failure or a success... the study names are always present in the json on the
    html... """
    ENDPOINT_NAME = "study_endpoints.manage_studies_page"
    
    def test(self):
        for user_role in ALL_TESTING_ROLES:
            self.assign_role(self.session_researcher, user_role)
            resp = self.smart_get()
            if user_role in ADMIN_ROLES:
                self.assertEqual(resp.status_code, 200)
            else:
                self.assertEqual(resp.status_code, 403)


class TestEditStudy(ResearcherSessionTest):
    """ Test basics of permissions, test details of the study are appropriately present on page... """
    ENDPOINT_NAME = "study_endpoints.edit_study"
    
    def test_only_admins_allowed(self):
        for user_role in ALL_TESTING_ROLES:
            self.assign_role(self.session_researcher, user_role)
            self.smart_get_status_code(
                200 if user_role in ADMIN_ROLES else 403, self.session_study.id
            )
    
    def test_content_study_admin(self):
        """ tests that various important pieces of information are present """
        self.set_session_study_relation(ResearcherRole.study_admin)
        self.session_study.update(forest_enabled=False)
        resp1 = self.smart_get_status_code(200, self.session_study.id)
        self.assert_present("Enable Forest", resp1.content)
        self.assert_not_present("Disable Forest", resp1.content)
        self.assert_present(self.session_researcher.username, resp1.content)
    
        self.session_study.update(forest_enabled=True)
        r2 = self.generate_researcher(relation_to_session_study=ResearcherRole.researcher)
    
        # tests for presence of own username and other researcher's username in the html
        resp2 = self.smart_get_status_code(200, self.session_study.id)
        self.assert_present(self.session_researcher.username, resp2.content)
        self.assert_present(r2.username, resp2.content)


class TestUpdateEndDate(ResearcherSessionTest):
    ENDPOINT_NAME = "study_endpoints.update_end_date"
    REDIRECT_ENDPOINT_NAME = "study_endpoints.edit_study"
    
    invalid_message = "Invalid date format, expected YYYY-MM-DD."
    
    def test_researcher(self):
        self.set_session_study_relation(ResearcherRole.researcher)
        self.smart_post_status_code(403, self.session_study.id)
    
    def test_study_admin(self):
        self.set_session_study_relation(ResearcherRole.study_admin)
        self.smart_post_status_code(403, self.session_study.id)
    
    def test_site_admin(self):
        self.set_session_study_relation(ResearcherRole.site_admin)
        self.smart_post_redirect(self.session_study.id)
    
    def test_date_in_past(self):
        # this is valid, you can set it to the past and immediately stop a study.
        self.set_session_study_relation(ResearcherRole.site_admin)
        self.smart_post_redirect(self.session_study.id, end_date="2020-01-01")
        self.session_study.refresh_from_db()
        self.assertEqual(self.session_study.end_date, date(2020, 1, 1))
    
    def test_date_in_far_future(self):
        d = date.today()+timedelta(days=200)
        self.set_session_study_relation(ResearcherRole.site_admin)
        self.smart_post_redirect(self.session_study.id, end_date=d.isoformat())
        self.session_study.refresh_from_db()
        self.assertEqual(self.session_study.end_date, d)
    
    def test_bad_date_formats(self):
        self.set_session_study_relation(ResearcherRole.site_admin)
    
        self.smart_post_redirect(self.session_study.id, end_date="2020-01-31 00:00:00")
        self.assert_message(self.invalid_message)
        self.session_study.refresh_from_db()
        self.assertEqual(self.session_study.end_date, None)
    
        self.smart_post_redirect(self.session_study.id, end_date="2020-02-31T00:00:00")
        self.assert_message(self.invalid_message)
        self.session_study.refresh_from_db()
        self.assertEqual(self.session_study.end_date, None)
    
        self.smart_post_redirect(self.session_study.id, end_date="2020-03-31T00:00:00Z")
        self.assert_message(self.invalid_message)
        self.session_study.refresh_from_db()
        self.assertEqual(self.session_study.end_date, None)
    
        self.smart_post_redirect(self.session_study.id, end_date="1-31-2020")
        self.assert_message(self.invalid_message)
        self.session_study.refresh_from_db()
        self.assertEqual(self.session_study.end_date, None)
    
        self.smart_post_redirect(self.session_study.id, end_date="1-31-2020")
        self.assert_message(self.invalid_message)
        self.session_study.refresh_from_db()
        self.assertEqual(self.session_study.end_date, None)
    
        self.smart_post_redirect(self.session_study.id, end_date="31-1-2020")
        self.assert_message(self.invalid_message)
        self.session_study.refresh_from_db()
        self.assertEqual(self.session_study.end_date, None)
    
        self.smart_post_redirect(self.session_study.id, end_date="2020/1/31")
        self.assert_message(self.invalid_message)
        self.session_study.refresh_from_db()
        self.assertEqual(self.session_study.end_date, None)
    
    def test_ok_date_formats(self):
        self.set_session_study_relation(ResearcherRole.site_admin)
    
        # you don't need leading zeros
        self.smart_post_redirect(self.session_study.id, end_date="2020-1-4")
        self.session_study.refresh_from_db()
        self.assertEqual(self.session_study.end_date, date(2020, 1, 4))
        self.assertTrue(self.session_study.end_date_is_in_the_past)  # might as well test...
    
        # clears the date
        self.smart_post_redirect(self.session_study.id, end_date="")
        self.session_study.refresh_from_db()
        self.assertEqual(self.session_study.end_date, None)
    
    def test_no_params(self):
        self.set_session_study_relation(ResearcherRole.site_admin)
        self.session_study.update_only(end_date=date(2020, 1, 1))
        self.smart_post_redirect(self.session_study.id)
        self.assert_message("No date provided.")
        self.session_study.refresh_from_db()
        self.assertEqual(self.session_study.end_date, date(2020, 1, 1))


class TestToggleManuallyEndStudy(ResearcherSessionTest):
    ENDPOINT_NAME = "study_endpoints.toggle_end_study"
    REDIRECT_ENDPOINT_NAME = "study_endpoints.edit_study"
    
    def test_researcher(self):
        self.set_session_study_relation(ResearcherRole.researcher)
        self.smart_post_status_code(403, self.session_study.id)
        self.assertFalse(self.session_study.manually_stopped)
    
    def test_study_admin(self):
        self.set_session_study_relation(ResearcherRole.study_admin)
        self.smart_post_status_code(403, self.session_study.id)
        self.assertFalse(self.session_study.manually_stopped)
    
    def test_site_admin_end_study(self):
        self.set_session_study_relation(ResearcherRole.site_admin)
        self.assertFalse(self.session_study.manually_stopped)
        self.smart_post_redirect(self.session_study.id)
        self.session_study.refresh_from_db()
        self.assertTrue(self.session_study.manually_stopped)
        self.assert_message_fragment("has been manually stopped")
    
    def test_site_admin_unend_study(self):
        self.set_session_study_relation(ResearcherRole.site_admin)
        self.assertFalse(self.session_study.manually_stopped)
        self.session_study.update_only(manually_stopped=True)
        self.smart_post_redirect(self.session_study.id)
        self.session_study.refresh_from_db()
        self.assertFalse(self.session_study.manually_stopped)
        self.assert_message_fragment("has been manually re-opened")


# FIXME: need to implement tests for copy study.
# FIXME: this test is not well factored, it doesn't follow a common pattern.
class TestCreateStudy(ResearcherSessionTest):
    ENDPOINT_NAME = "study_endpoints.create_study"
    NEW_STUDY_NAME = "something anything"
    
    @property
    def get_the_new_study(self):
        return Study.objects.get(name=self.NEW_STUDY_NAME)
    
    @property
    def assert_no_new_study(self):
        self.assertFalse(Study.objects.filter(name=self.NEW_STUDY_NAME).exists())
    
    def create_study_params(self):
        """ keys are: name, encryption_key, copy_existing_study, forest_enabled """
        params = dict(
            name=self.NEW_STUDY_NAME,
            encryption_key="a" * 32,
            copy_existing_study="",
            forest_enabled="false",
        )
        return params
    
    def test_load_page(self):
        # only site admins can load the page
        for user_role in ALL_TESTING_ROLES:
            self.assign_role(self.session_researcher, user_role)
            self.smart_get_status_code(200 if user_role == ResearcherRole.site_admin else 403)
    
    def test_posts_redirect(self):
        # only site admins can load the page
        for user_role in ALL_TESTING_ROLES:
            self.assign_role(self.session_researcher, user_role)
            self.smart_post_status_code(
                302 if user_role == ResearcherRole.site_admin else 403, **self.create_study_params()
            )
    
    def test_create_study_researcher(self):
        self.set_session_study_relation(ResearcherRole.researcher)
        self._test_create_study(False)
    
    def test_create_study_study_admin(self):
        self.set_session_study_relation(ResearcherRole.study_admin)
        self._test_create_study(False)
    
    def test_create_study_site_admin(self):
        self.set_session_study_relation(ResearcherRole.site_admin)
        self._test_create_study(True)
    
    def test_create_study_researcher_and_site_admin(self):
        # unable to replicate hassan's bug - he must have his the special characters bug
        self.set_session_study_relation(ResearcherRole.researcher)
        self.session_researcher.update(site_admin=True)
        self._test_create_study(True)
    
    def test_create_study_study_admin_and_site_admin(self):
        # unable to replicate hassan's bug - he must have his the special characters bug
        self.set_session_study_relation(ResearcherRole.study_admin)
        self.session_researcher.update(site_admin=True)
        self._test_create_study(True)
    
    def _test_create_study(self, success):
        study_count = Study.objects.count()
        device_settings_count = DeviceSettings.objects.count()
        resp = self.smart_post_status_code(302 if success else 403, **self.create_study_params())
        if success:
            self.assertIsInstance(resp, HttpResponseRedirect)
            target_url = easy_url(
                "study_endpoints.device_settings", study_id=self.get_the_new_study.id
            )
            self.assert_response_url_equal(resp.url, target_url)
            resp = self.client.get(target_url)
            self.assertEqual(resp.status_code, 200)
            self.assert_present(
                f"Successfully created study {self.get_the_new_study.name}.", resp.content
            )
            self.assertEqual(study_count + 1, Study.objects.count())
            self.assertEqual(device_settings_count + 1, DeviceSettings.objects.count())
        else:
            self.assertIsInstance(resp, HttpResponse)
            self.assertEqual(study_count, Study.objects.count())
            self.assertEqual(device_settings_count, DeviceSettings.objects.count())
            self.assert_no_new_study
    
    def test_create_study_long_name(self):
        # this situation reports to sentry manually, the response is a hard 400, no calls to messages
        self.set_session_study_relation(ResearcherRole.site_admin)
        params = self.create_study_params()
        params["name"] = "a" * 10000
        resp = self.smart_post_status_code(302, **params)
        self.assertEqual(resp.url, easy_url("study_endpoints.create_study"))
        self.assert_present(
            resp.content, b"the study name you provided was too long and was rejected"
        )
        self.assert_no_new_study
    
    def test_create_study_bad_name(self):
        # this situation reports to sentry manually, the response is a hard 400, no calls to messages
        self.set_session_study_relation(ResearcherRole.site_admin)
        params = self.create_study_params()
        params["name"] = "&" * 50
        resp = self.smart_post_status_code(302, **params)
        self.assert_present(resp.content, b"you provided contained unsafe characters")
        self.assert_no_new_study
    
    def create_study_with_too_much_whitespace_gets_normalized(self):
        self.set_session_study_relation(ResearcherRole.site_admin)
        params = self.create_study_params()
        params["name"] = "name                             hello"
        resp = self.smart_post_status_code(302, **params)
        self.assertEqual(resp.url, easy_url("study_endpoints.create_study"))
        self.assert_not_present(
            resp.content, b"the study name you provided was too long and was rejected"
        )
        self.assertIsNotNone(Study.objects.get(name="name hello"))  # ok this will crash


class TestHideStudy(ResearcherSessionTest):
    ENDPOINT_NAME = "study_endpoints.hide_study"
    REDIRECT_ENDPOINT_NAME = "study_endpoints.manage_studies_page"
    
    def test_site_admin(self):
        self.set_session_study_relation(ResearcherRole.site_admin)
        resp = self.smart_post(self.session_study.id, confirmation="true")
        self.session_study.refresh_from_db()
        self.assertTrue(self.session_study.deleted)
        self.assertTrue(self.session_study.manually_stopped)
        self.assertEqual(resp.url, easy_url(self.REDIRECT_ENDPOINT_NAME))
        self.assert_present("has been hidden", self.redirect_get_contents())
        self.assert_message_fragment("has been hidden")
    
    def test_confirmation_must_be_true(self):
        self.set_session_study_relation(ResearcherRole.site_admin)
        resp = self.smart_post_status_code(400, self.session_study.id, confirmation="false")
        self.session_study.refresh_from_db()
        self.assertFalse(self.session_study.deleted)
    
    def test_study_admin(self):
        self.set_session_study_relation(ResearcherRole.study_admin)
        self._test_role_fail()
    
    def test_researcher(self):
        self.set_session_study_relation(ResearcherRole.researcher)
        self._test_role_fail()
    
    def _test_role_fail(self):
        self.smart_post_status_code(403, self.session_study.id)
        self.session_study.refresh_from_db()
        self.assertFalse(self.session_study.deleted)
        self.assertFalse(self.session_study.manually_stopped)
        self.smart_post(self.session_study.id, confirmation="true")
        self.session_study.refresh_from_db()
        self.assertFalse(self.session_study.deleted)


class TestEditStudySecuritySettings(ResearcherSessionTest):
    ENDPOINT_NAME = "study_endpoints.study_security_page"
    
    def test_researcher(self):
        self.set_session_study_relation(ResearcherRole.researcher)
        self.smart_get_status_code(403, self.session_study.id)
    
    def test_study_admin(self):
        self.set_session_study_relation(ResearcherRole.study_admin)
        self.smart_get_status_code(200, self.session_study.id)
    
    def test_site_admin(self):
        self.set_session_study_relation(ResearcherRole.site_admin)
        self.smart_get_status_code(200, self.session_study.id)


class TestChangeStudySecuritySettings(ResearcherSessionTest):
    ENDPOINT_NAME = "study_endpoints.change_study_security_settings"
    password_length = len(ResearcherSessionTest.DEFAULT_RESEARCHER_PASSWORD)
    
    @property
    def DEFAULTS(self):
        """ Default post params, all cause changes to database """
        return {
            "password_minimum_length": "15",
            "password_max_age_enabled": "on",
            "password_max_age_days": "90",
        }
    
    def test_valid_values(self):
        # need to set password length so that we don't crap out at password length of 13 due to a reset
        self.session_researcher.update_only(password_min_length=20)
        self.set_session_study_relation(ResearcherRole.study_admin)
        params = self.DEFAULTS
    
        # test all valid of password length plus 2 invalid values
        for i in range(8, 21):
            params["password_minimum_length"] = i
            self.smart_post_status_code(302, self.session_study.id, **params)
            self.session_study.refresh_from_db()
            self.assertEqual(self.session_study.password_minimum_length, i)
        for i in [0, 7, 21, 1000]:
            params["password_minimum_length"] = i
            self.smart_post_status_code(302, self.session_study.id, **params)
            self.session_study.refresh_from_db()
            self.assertEqual(self.session_study.password_minimum_length, 20)
        
        params["password_minimum_length"] = 15  # reset to something valid so the next test works
        
        # test valid selectable values for password age, then invalid values at the ends
        for i in ["30", "60", "90", "180", "365"]:
            params["password_max_age_days"] = i
            self.smart_post_status_code(302, self.session_study.id, **params)
            self.session_study.refresh_from_db()
            self.assertEqual(self.session_study.password_max_age_days, int(i))
        for i in ["0", "29", "366", "1000"]:
            params["password_max_age_days"] = i
            self.smart_post_status_code(302, self.session_study.id, **params)
            self.session_study.refresh_from_db()
            self.assertEqual(self.session_study.password_max_age_days, 365)
    
    def test_missing_all_fields(self):
        self.set_session_study_relation(ResearcherRole.study_admin)
        self.assertEqual(self.session_researcher.password_min_length, self.password_length)
        self.assertEqual(self.session_study.password_max_age_enabled, False)  # defaults
        self.assertEqual(self.session_study.password_minimum_length, 8)
        self.assertEqual(self.session_study.password_max_age_days, 365)
        ret = self.smart_post_status_code(302, self.session_study.id)
        self.assertEqual(
            ret.url, easy_url("study_endpoints.study_security_page", self.session_study.id)
        )
        page = self.easy_get(
            "study_endpoints.study_security_page", study_id=self.session_study.id
        ).content
        
        self.assert_present("Minimum Password Length", page)
        # self.assert_present("Enable Maximum Password Age", page)  # checkboxes do not provide a value if they are unchecked
        self.assert_present("Maximum Password Age (days)", page)
        self.session_study.refresh_from_db()
        # assert no changes
        self.assertEqual(self.session_study.password_max_age_enabled, False)
        self.assertEqual(self.session_study.password_minimum_length, 8)
        self.assertEqual(self.session_study.password_max_age_days, 365)
        self.session_researcher.refresh_from_db()
        self.assertEqual(self.session_researcher.password_force_reset, False)
    
    def test_enable_max_age_enabled(self):
        # set up a bunch of researchers
        r_not_related = self.generate_researcher("not related")
        r_related = self.generate_researcher("researcher")
        r_long = self.generate_researcher("longresearcher")  # won't require password reset
        r_long.set_password("a" * 20)
        self.generate_study_relation(r_related, self.session_study, ResearcherRole.researcher)
        self.generate_study_relation(r_long, self.session_study, ResearcherRole.researcher)
        self.assertEqual(self.session_researcher.password_min_length, self.password_length)
        self.assertEqual(r_not_related.password_min_length, self.password_length)
        self.assertEqual(r_related.password_min_length, self.password_length)
        self.assertEqual(r_long.password_min_length, 20)
        # setup and do post
        self.set_session_study_relation(ResearcherRole.study_admin)
        ret = self.smart_post_status_code(302, self.session_study.id, **self.DEFAULTS)
        self.assertEqual(ret.url, easy_url("study_endpoints.edit_study", self.session_study.id))
        self.session_study.refresh_from_db()
        # assert changes
        self.assertEqual(self.session_study.password_max_age_enabled, True)
        self.assertEqual(self.session_study.password_minimum_length, 15)
        self.assertEqual(self.session_study.password_max_age_days, 90)
        # session researcher should have a password reset
        self.session_researcher.refresh_from_db()
        r_not_related.refresh_from_db()
        r_related.refresh_from_db()
        r_long.refresh_from_db()
        # make sure force reset is not in use, we don't rely on it.
        self.assertEqual(self.session_researcher.password_force_reset, False)
        self.assertEqual(r_related.password_force_reset, False)
        self.assertEqual(r_not_related.password_force_reset, False)
        self.assertEqual(r_long.password_force_reset, False)


class TestDeviceSettings(ResearcherSessionTest):
    ENDPOINT_NAME = "study_endpoints.device_settings"
    
    CONSENT_SECTIONS = {
        'consent_sections.data_gathering.more': 'a',
        'consent_sections.data_gathering.text': 'b',
        'consent_sections.privacy.more': 'c',
        'consent_sections.privacy.text': 'd',
        'consent_sections.study_survey.more': 'e',
        'consent_sections.study_survey.text': 'f',
        'consent_sections.study_tasks.more': 'g',
        'consent_sections.study_tasks.text': 'h',
        'consent_sections.time_commitment.more': 'i',
        'consent_sections.time_commitment.text': 'j',
        'consent_sections.welcome.more': 'k',
        'consent_sections.welcome.text': 'l',
        'consent_sections.withdrawing.more': 'm',
        'consent_sections.withdrawing.text': 'n',
    }
    
    BOOLEAN_FIELD_NAMES = [
        field.name
        for field in DeviceSettings._meta.fields
        if isinstance(field, (models.BooleanField, models.NullBooleanField))
    ]
    
    def invert_boolean_checkbox_fields(self, some_dict):
        for field in self.BOOLEAN_FIELD_NAMES:
            if field in some_dict and bool(some_dict[field]):
                some_dict.pop(field)
            else:
                some_dict[field] = "true"
    
    def test_get(self):
        for role in ALL_TESTING_ROLES:
            self.assign_role(self.session_researcher, role)
            resp = self.smart_get(self.session_study.id)
            self.assertEqual(resp.status_code, 200 if role is not None else 403)
    
    def test_study_admin(self):
        self.set_session_study_relation(ResearcherRole.study_admin)
        self.do_test_update()
    
    def test_site_admin(self):
        self.set_session_study_relation(ResearcherRole.site_admin)
        self.do_test_update()
    
    def do_test_update(self):
        """ This test mimics the frontend input (checkboxes are a little strange and require setup).
        The test mutates all fields in the input that is sent to the backend, and confirms that every
        field pushed changed. """
        
        # extract data from database (it is all default values, unpacking jsonstrings)
        # created_on and last_updated are already absent
        post_params = self.session_device_settings.export()
        old_device_settings = copy(post_params)
        post_params.pop("id")
        post_params.pop("consent_sections")  # this is not present in the form
        post_params.update(**self.CONSENT_SECTIONS)
        
        # mutate everything
        post_params = {
            k: self.mutate_variable(v, ignore_bools=True) for k, v in post_params.items()
        }
        self.invert_boolean_checkbox_fields(post_params)
        
        # Hit endpoint
        self.smart_post_status_code(302, self.session_study.id, **post_params)
        
        # Test database update, get new data, extract consent sections.
        self.assertEqual(DeviceSettings.objects.count(), 1)
        new_device_settings = DeviceSettings.objects.first().export()
        new_device_settings.pop("id")
        old_consent_sections = old_device_settings.pop("consent_sections")
        new_consent_sections = new_device_settings.pop("consent_sections")
        
        for k, v in new_device_settings.items():
            # boolean values are set to true or false based on presence in the post request,
            # that's how checkboxes work.
            if k in self.BOOLEAN_FIELD_NAMES:
                if k not in post_params:
                    self.assertFalse(v)
                    self.assertTrue(old_device_settings[k])
                else:
                    self.assertTrue(v)
                    self.assertFalse(old_device_settings[k])
                continue
            
            # print(f"key: '{k}', DB: {type(v)}'{v}', post param: {type(post_params[k])} '{post_params[k]}'")
            self.assertEqual(v, post_params[k])
            self.assertNotEqual(v, old_device_settings[k])
        
        # FIXME: why does this fail?
        # Consent sections need to be unpacked, ensure they have the keys
        # self.assertEqual(set(old_consent_sections.keys()), set(new_consent_sections.keys()))
        
        for outer_key, a_dict_of_two_values in new_consent_sections.items():
            # this data structure is of the form:  {'more': 'aaaa', 'text': 'baaa'}
            self.assertEqual(len(a_dict_of_two_values), 2)
            
            # compare the inner values of every key, make sure they differ
            for inner_key, v2 in a_dict_of_two_values.items():
                self.assertNotEqual(old_consent_sections[outer_key][inner_key], v2)
    
    def test_resend_logic_side_effect_0_1(self):
        self.base_test_resend_logic_side_effect(0, 1, True)
    
    def test_resend_logic_side_effect_0_2000(self):
        self.base_test_resend_logic_side_effect(0, 2000, True)
    
    def test_resend_logic_side_effect_0_0(self):
        self.base_test_resend_logic_side_effect(0, 0, False)
    
    def test_resend_logic_side_effect_1_1(self):
        self.base_test_resend_logic_side_effect(1, 1, False)
    
    def test_resend_logic_side_effect_1_0(self):
        self.base_test_resend_logic_side_effect(1, 0, False)
    
    def test_resend_logic_side_effect_2000_9(self):
        self.base_test_resend_logic_side_effect(2000, 9, False)
    
    def test_resend_logic_side_effect_2000_2000(self):
        self.base_test_resend_logic_side_effect(2000, 2000, False)
    
    def base_test_resend_logic_side_effect(self, initial_resnd: int, updated_resend: int, applied: bool):
        """ tests the resend logic safety net feature.  Resends need to be selectively disabled when
        transitionaing from 0 to any value. """
        self.set_session_study_relation(ResearcherRole.study_admin)
        self.session_device_settings.update(resend_period_minutes=initial_resnd)
        
        now = timezone.now()
        the_past = now - timedelta(days=10)
        yesterday = now - timedelta(days=1)
        tomorrow = now + timedelta(days=1)
        event1 = self.generate_easy_absolute_scheduled_event_with_absolute_schedule(yesterday)
        event2 = self.generate_easy_absolute_scheduled_event_with_absolute_schedule(tomorrow)
        ScheduledEvent.objects.update(last_updated=the_past)
        event1.refresh_from_db()
        event2.refresh_from_db()
        
        post_params = self.session_device_settings.export()
        post_params.pop("id")
        post_params.pop("consent_sections")  # this is not present in the form
        post_params.update(**self.CONSENT_SECTIONS)
        post_params["resend_period_minutes"] = updated_resend
        
        # true at start always
        self.assertEqual(event1.last_updated, the_past)
        self.assertFalse(event1.no_resend)
        self.assertEqual(event2.last_updated, the_past)
        self.assertFalse(event2.no_resend)
        
        # hit the endpoint
        self.smart_post_status_code(302, self.session_study.id, **post_params)
        event1.refresh_from_db()
        event2.refresh_from_db()
        
        if applied:
            # schedule 1 should be affected, schedule 2 should not
            self.assertGreater(event1.last_updated, the_past)
            self.assertTrue(event1.no_resend)
            self.assertEqual(event2.last_updated, the_past)
            self.assertFalse(event2.no_resend)
            self.assertIn(DEVICE_SETTINGS_RESEND_FROM_0.format(updated_resend), self.messages)
        else:
            # same as true at start
            self.assertEqual(event1.last_updated, the_past)
            self.assertFalse(event1.no_resend)
            self.assertEqual(event2.last_updated, the_past)
            self.assertFalse(event2.no_resend)
            self.assertNotIn(DEVICE_SETTINGS_RESEND_FROM_0.format(updated_resend), self.messages)  # detected


class TestToggleForest(ResearcherSessionTest):
    ENDPOINT_NAME = "study_endpoints.toggle_study_forest_enabled"
    REDIRECT_ENDPOINT_NAME = "study_endpoints.edit_study"
    
    def test_toggle_on(self):
        resp = self._do_test_toggle(True)
        self.assert_present("Enabled Forest on", resp.content)
    
    def test_toggle_off(self):
        resp = self._do_test_toggle(False)
        self.assert_present("Disabled Forest on", resp.content)
    
    def _do_test_toggle(self, enable: bool):
        redirect_endpoint = easy_url(self.REDIRECT_ENDPOINT_NAME, study_id=self.session_study.id)
        self.set_session_study_relation(ResearcherRole.site_admin)
        self.session_study.update(forest_enabled=not enable)  # directly mutate the database.
        # resp = self.smart_post(study_id=self.session_study.id)  # nope this does not follow the normal pattern
        resp = self.smart_post(self.session_study.id)
        self.assert_response_url_equal(resp.url, redirect_endpoint)
        self.session_study.refresh_from_db()
        if enable:
            self.assertTrue(self.session_study.forest_enabled)
        else:
            self.assertFalse(self.session_study.forest_enabled)
        return self.client.get(redirect_endpoint)


#
## copy study api
#

# FIXME: add interventions and surveys to the export tests
class TestExportStudySettingsFile(ResearcherSessionTest):
    ENDPOINT_NAME = "study_endpoints.export_study_settings_file"
    
    def test(self):
        self.set_session_study_relation(ResearcherRole.study_admin)
        # FileResponse objects stream, which means you need to iterate over `resp.streaming_content``
        resp: FileResponse = self.smart_get(self.session_study.id)
        # sanity check...
        resp_string = b"".join(resp.streaming_content)
        self.assertNotEqual(len(resp_string), 0)
        # get survey, check device_settings, surveys, interventions are all present
        output_survey: dict = orjson.loads(resp_string)
        self.assertIn("device_settings", output_survey)
        self.assertIn("surveys", output_survey)
        self.assertIn("interventions", output_survey)
        output_device_settings: dict = output_survey["device_settings"]
        real_device_settings = self.session_device_settings.export()
        # confirm that all elements are equal for the dicts
        for k, v in output_device_settings.items():
            self.assertEqual(v, real_device_settings[k])


# FIXME: add interventions and surveys to the import tests
class TestImportStudySettingsFile(ResearcherSessionTest):
    ENDPOINT_NAME = "study_endpoints.import_study_settings_file"
    REDIRECT_ENDPOINT_NAME = "study_endpoints.edit_study"
    
    # other post params: device_settings, surveys
    
    def test_no_device_settings_no_surveys(self):
        content = self._test(False, False)
        self.assert_present("Did not alter", content)
        self.assert_present("Copied 0 Surveys and 0 Audio Surveys", content)
    
    def test_device_settings_no_surveys(self):
        content = self._test(True, False)
        self.assert_present("Settings with custom values.", content)
        self.assert_present("Copied 0 Surveys and 0 Audio Surveys", content)
    
    def test_device_settings_and_surveys(self):
        content = self._test(True, True)
        self.assert_present("Settings with custom values.", content)
        self.assert_present("Copied 0 Surveys and 0 Audio Surveys", content)
    
    def test_bad_filename(self):
        content = self._test(True, True, ".exe", success=False)
        # FIXME: this is not present in the html, it should be  - string doesn't appear in codebase...
        # self.assert_present("You can only upload .json files.", content)
    
    def _test(
        self,
        device_settings: bool,
        surveys: bool,
        extension: str = "json",
        success: bool = True
    ) -> bytes:
        self.set_session_study_relation(ResearcherRole.site_admin)
        study2 = self.generate_study("study_2")
        self.assertEqual(self.session_device_settings.gps, True)
        self.session_device_settings.update(gps=False)
        
        # this is the function that creates the canonical study representation wrapped in a burrito
        survey_json_file = BytesIO(format_study(self.session_study))
        survey_json_file.name = f"something.{extension}"  # ayup, that's how you add a name...
        
        self.smart_post_redirect(
            study2.id,
            upload=survey_json_file,
            device_settings="true" if device_settings else "false",
            surveys="true" if surveys else "false",
        )
        study2.device_settings.refresh_from_db()
        if success:
            self.assertEqual(study2.device_settings.gps, not device_settings)
        # return the page, we always need it
        return self.easy_get(
            self.REDIRECT_ENDPOINT_NAME, status_code=200, study_id=study2.id
        ).content

from copy import copy
from datetime import date, timedelta
from unittest.mock import MagicMock, patch

from django.core.files.uploadedfile import SimpleUploadedFile
from django.db import models
from django.forms.fields import NullBooleanField
from django.http.response import HttpResponse, HttpResponseRedirect

from constants.celery_constants import (ANDROID_FIREBASE_CREDENTIALS, BACKEND_FIREBASE_CREDENTIALS,
    IOS_FIREBASE_CREDENTIALS)
from constants.message_strings import MFA_RESET_BAD_PERMISSIONS
from constants.testing_constants import (ADMIN_ROLES, ALL_TESTING_ROLES, ANDROID_CERT, BACKEND_CERT,
    IOS_CERT)
from constants.user_constants import ALL_RESEARCHER_TYPES, ResearcherRole
from database.study_models import DeviceSettings, Study
from database.system_models import FileAsText
from database.user_models_researcher import Researcher
from libs.http_utils import easy_url
from libs.security import generate_easy_alphanumeric_string
from tests.common import ResearcherSessionTest

# trunk-ignore-all(ruff/B018)

#
## system_admin_pages
#


class TestDemoteStudyAdmin(ResearcherSessionTest):
    # FIXME: this endpoint does not test for site admin cases correctly, the test passes but is
    # wrong. Behavior is fine because it has no relevant side effects except for the know bug where
    # site admins need to be manually added to a study before being able to download data.
    ENDPOINT_NAME = "system_admin_pages.demote_study_admin"
    
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


class TestCreateNewResearcher(ResearcherSessionTest):
    """ Admins should be able to create and load the page. """
    ENDPOINT_NAME = "system_admin_pages.create_new_researcher"
    
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


class TestManageStudies(ResearcherSessionTest):
    """ All we do with this page is make sure it loads... there isn't much to hook onto and
    determine a failure or a success... the study names are always present in the json on the
    html... """
    ENDPOINT_NAME = "system_admin_pages.manage_studies"
    
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
    ENDPOINT_NAME = "system_admin_pages.edit_study"
    
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


# FIXME: need to implement tests for copy study.
# FIXME: this test is not well factored, it doesn't follow a common pattern.
class TestCreateStudy(ResearcherSessionTest):
    ENDPOINT_NAME = "system_admin_pages.create_study"
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
                "system_admin_pages.device_settings", study_id=self.get_the_new_study.id
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
        self.assertEqual(resp.url, easy_url("system_admin_pages.create_study"))
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


# FIXME: this test has the annoying un-factored url with post params and url params
class TestToggleForest(ResearcherSessionTest):
    ENDPOINT_NAME = "system_admin_pages.toggle_study_forest_enabled"
    REDIRECT_ENDPOINT_NAME = "system_admin_pages.edit_study"
    
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


class TestHideStudy(ResearcherSessionTest):
    ENDPOINT_NAME = "system_admin_pages.hide_study"
    REDIRECT_ENDPOINT_NAME = "system_admin_pages.manage_studies"
    
    def test_site_admin(self):
        self.set_session_study_relation(ResearcherRole.site_admin)
        resp = self.smart_post(self.session_study.id, confirmation="true")
        self.session_study.refresh_from_db()
        self.assertTrue(self.session_study.deleted)
        self.assertEqual(resp.url, easy_url(self.REDIRECT_ENDPOINT_NAME))
        self.assert_present("has been hidden", self.redirect_get_contents())
    
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
        self.smart_post(self.session_study.id, confirmation="true")
        self.session_study.refresh_from_db()
        self.assertFalse(self.session_study.deleted)


class TestDeviceSettings(ResearcherSessionTest):
    ENDPOINT_NAME = "system_admin_pages.device_settings"
    
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
        if isinstance(field, (models.BooleanField, NullBooleanField))
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


class TestChangeSurveySecuritySettings(ResearcherSessionTest):
    ENDPOINT_NAME = "system_admin_pages.change_study_security_settings"
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
            ret.url, easy_url("system_admin_pages.study_security_page", self.session_study.id)
        )
        page = self.easy_get(
            "system_admin_pages.study_security_page", study_id=self.session_study.id
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
        self.assertEqual(ret.url, easy_url("system_admin_pages.edit_study", self.session_study.id))
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


class TestEditSurveySecuritySettings(ResearcherSessionTest):
    ENDPOINT_NAME = "system_admin_pages.study_security_page"
    
    def test_researcher(self):
        self.set_session_study_relation(ResearcherRole.researcher)
        self.smart_get_status_code(403, self.session_study.id)
    
    def test_study_admin(self):
        self.set_session_study_relation(ResearcherRole.study_admin)
        self.smart_get_status_code(200, self.session_study.id)
    
    def test_site_admin(self):
        self.set_session_study_relation(ResearcherRole.site_admin)
        self.smart_get_status_code(200, self.session_study.id)


class TestManageFirebaseCredentials(ResearcherSessionTest):
    ENDPOINT_NAME = "system_admin_pages.manage_firebase_credentials"
    
    def test(self):
        # just test that the page loads, I guess
        self.set_session_study_relation(ResearcherRole.site_admin)
        self.smart_get_status_code(200)


# FIXME: implement tests for error cases
class TestUploadBackendFirebaseCert(ResearcherSessionTest):
    ENDPOINT_NAME = "system_admin_pages.upload_backend_firebase_cert"
    REDIRECT_ENDPOINT_NAME = "system_admin_pages.manage_firebase_credentials"
    
    @patch("pages.system_admin_pages.update_firebase_instance")
    @patch("pages.system_admin_pages.get_firebase_credential_errors")
    def test(self, get_firebase_credential_errors: MagicMock, update_firebase_instance: MagicMock):
        # test that the data makes it to the backend, patch out the errors that are sourced from the
        # firbase admin lbrary
        get_firebase_credential_errors.return_value = None
        update_firebase_instance.return_value = True
        # test upload as site admin
        self.set_session_study_relation(ResearcherRole.site_admin)
        file = SimpleUploadedFile("backend_cert.json", BACKEND_CERT.encode(), "text/json")
        self.smart_post(backend_firebase_cert=file)
        resp_content = self.redirect_get_contents()
        self.assert_present("New firebase credentials have been received", resp_content)


# FIXME: implement tests for error cases
class TestUploadIosFirebaseCert(ResearcherSessionTest):
    ENDPOINT_NAME = "system_admin_pages.upload_ios_firebase_cert"
    REDIRECT_ENDPOINT_NAME = "system_admin_pages.manage_firebase_credentials"
    
    def test(self):
        # test upload as site admin
        self.set_session_study_relation(ResearcherRole.site_admin)
        file = SimpleUploadedFile("ios_firebase_cert.plist", IOS_CERT.encode(), "text/json")
        self.smart_post(ios_firebase_cert=file)
        resp_content = self.redirect_get_contents()
        self.assert_present("New IOS credentials were received", resp_content)


# FIXME: implement tests for error cases
class TestUploadAndroidFirebaseCert(ResearcherSessionTest):
    ENDPOINT_NAME = "system_admin_pages.upload_android_firebase_cert"
    REDIRECT_ENDPOINT_NAME = "system_admin_pages.manage_firebase_credentials"
    
    def test(self):
        # test upload as site admin
        self.set_session_study_relation(ResearcherRole.site_admin)
        file = SimpleUploadedFile("android_firebase_cert.json", ANDROID_CERT.encode(), "text/json")
        self.smart_post(android_firebase_cert=file)
        resp_content = self.redirect_get_contents()
        self.assert_present("New android credentials were received", resp_content)


class TestDeleteFirebaseBackendCert(ResearcherSessionTest):
    ENDPOINT_NAME = "system_admin_pages.delete_backend_firebase_cert"
    REDIRECT_ENDPOINT_NAME = "system_admin_pages.manage_firebase_credentials"
    
    def test(self):
        self.set_session_study_relation(ResearcherRole.site_admin)
        FileAsText.objects.create(tag=BACKEND_FIREBASE_CREDENTIALS, text="any_string")
        self.smart_post()
        self.assertFalse(FileAsText.objects.exists())


class TestDeleteFirebaseIosCert(ResearcherSessionTest):
    ENDPOINT_NAME = "system_admin_pages.delete_ios_firebase_cert"
    REDIRECT_ENDPOINT_NAME = "system_admin_pages.manage_firebase_credentials"
    
    def test(self):
        self.set_session_study_relation(ResearcherRole.site_admin)
        FileAsText.objects.create(tag=IOS_FIREBASE_CREDENTIALS, text="any_string")
        self.smart_post()
        self.assertFalse(FileAsText.objects.exists())


class TestDeleteFirebaseAndroidCert(ResearcherSessionTest):
    ENDPOINT_NAME = "system_admin_pages.delete_android_firebase_cert"
    REDIRECT_ENDPOINT_NAME = "system_admin_pages.manage_firebase_credentials"
    
    def test(self):
        self.set_session_study_relation(ResearcherRole.site_admin)
        FileAsText.objects.create(tag=ANDROID_FIREBASE_CREDENTIALS, text="any_string")
        self.smart_post()
        self.assertFalse(FileAsText.objects.exists())


class TestManageResearchers(ResearcherSessionTest):
    ENDPOINT_NAME = "system_admin_pages.manage_researchers"
    
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


class TestResetResearcherMFA(ResearcherSessionTest):
    ENDPOINT_NAME = "system_admin_pages.reset_researcher_mfa"
    REDIRECT_ENDPOINT_NAME = "system_admin_pages.edit_researcher"
    
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


class TestEditResearcher(ResearcherSessionTest):
    ENDPOINT_NAME = "system_admin_pages.edit_researcher"
    
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


class TestElevateResearcher(ResearcherSessionTest):
    ENDPOINT_NAME = "system_admin_pages.elevate_researcher"
    
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


# update_end_date
class TestUpdateEndDate(ResearcherSessionTest):
    ENDPOINT_NAME = "system_admin_pages.update_end_date"
    REDIRECT_ENDPOINT_NAME = "system_admin_pages.edit_study"
    
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
    
    def test_has_valid_endpoint_name_and_is_placed_in_correct_file(self):
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

# toggle_end_study

class TestToggleManuallyEndStudy(ResearcherSessionTest):
    ENDPOINT_NAME = "system_admin_pages.toggle_end_study"
    REDIRECT_ENDPOINT_NAME = "system_admin_pages.edit_study"
    
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
        
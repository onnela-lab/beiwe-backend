import requests
from unittest.mock import MagicMock

from api.tableau_api.base import TableauApiView, AuthenticationFailed, PermissionDenied
from app import app
from database.study_models import Study, DeviceSettings
from database.tests.authentication_tests.django_flask_hybrid_test_framework import HybridTest
from database.tests.authentication_tests.testing_constants import TEST_USERNAME, TEST_PASSWORD, BASE_URL, \
    TEST_STUDY_NAME, TEST_STUDY_ENCRYPTION_KEY
from database.security_models import ApiKey
from database.user_models import Researcher, StudyRelation
from time import sleep


class TableauApiAuthTests(HybridTest):
    """
    Test methods of the api authentication system
    """

    def setup(self, researcher=True, apikey=True, study=True):
        if apikey and not researcher:
            raise Exception("invalid setup criteria")
        if researcher:
            self.researcher = Researcher.create_with_password(username=TEST_USERNAME, password=TEST_PASSWORD)
        if apikey:
            api_key = ApiKey.generate(self.researcher, has_tableau_api_permissions=True)
            self.api_key_public = api_key.access_key_id
            self.api_key_private = api_key.access_key_secret_plaintext
        if study:
            self.study = Study.create_with_object_id(device_settings=DeviceSettings(), encryption_key=TEST_STUDY_ENCRYPTION_KEY, name=TEST_STUDY_NAME)
            if researcher:
                StudyRelation(study=self.study, researcher=self.researcher, relationship="researcher").save()


    @staticmethod
    def login(session=None):
        if session is None:
            session = requests.Session()
        session.post(BASE_URL + "/validate_login", data={'username': TEST_USERNAME, 'password': TEST_PASSWORD})
        return session

    def test_new_api_key(self):
        """
        Asserts that:
            -one new api key is added to the database
            -that api key is linked to the logged in researcher
            -the correct readable name is associated with the key
            -no other api keys were created associated with that researcher
            -that api key is active and has tableau access
        """
        self.setup(apikey=False, researcher=True, study=False)
        session = self.login()
        api_key_count = len(ApiKey.objects.all())
        response = session.post(BASE_URL + "/new_api_key", data={'readable_name': 'test_generated_api_key'})
        sleep(1)
        self.assertEqual(api_key_count + 1, len(ApiKey.objects.all()))
        ApiKey.objects.get(readable_name='test_generated_api_key')
        key = ApiKey.objects.get(researcher__username=TEST_USERNAME)
        self.assertTrue(key.is_active)
        self.assertTrue(key.has_tableau_api_permissions)
        return True

    def test_disable_api_key(self):
        """
        Asserts that:
            -exactly one fewer active api key is present in the database
            -the api key is no longer active
        """
        self.setup(researcher=True, apikey=True, study=False)
        session = self.login()
        api_key_count = len(ApiKey.objects.filter(is_active=True))
        response = session.post(BASE_URL + "/disable_api_key", data={'api_key_id': self.api_key_public})
        key = ApiKey.objects.get(access_key_id=self.api_key_public)
        self.assertEqual(api_key_count - 1, len(ApiKey.objects.filter(is_active=True)))
        self.assertFalse(key.is_active)
        return True

    def test_check_permissions_working(self):
        self.setup(researcher=True, apikey=True, study=True)
        headers = {"X-Access-Key-Id": self.api_key_public, "X-Access-Key-Secret": self.api_key_private}
        with app.test_request_context(headers=headers):
            self.assertTrue(TableauApiView().check_permissions(study_id=self.study.object_id))

    def test_check_permissions_none(self):
        self.setup(researcher=True, apikey=True, study=True)
        headers = {}
        with self.assertRaises(AuthenticationFailed) as cm:
            with app.test_request_context(headers=headers):
                TableauApiView().check_permissions(study_id=self.study.object_id)

    def test_check_permissions_inactive(self):
        self.setup(researcher=True, apikey=True, study=True)
        ApiKey.objects.filter(access_key_id=self.api_key_public).update(is_active=False)
        headers = {"X-Access-Key-Id": self.api_key_public, "X-Access-Key-Secret": self.api_key_private}
        with self.assertRaises(AuthenticationFailed) as cm:
            with app.test_request_context(headers=headers):
                TableauApiView().check_permissions(study_id=self.study.object_id)

    def test_check_permissions_bad_secret(self):
        self.setup(researcher=True, apikey=True, study=True)
        headers = {"X-Access-Key-Id": self.api_key_public, "X-Access-Key-Secret": ':::' + self.api_key_private[3:]}
        with self.assertRaises(AuthenticationFailed) as cm:
            with app.test_request_context(headers=headers):
                TableauApiView().check_permissions(study_id=self.study.object_id)

    def test_check_permissions_no_tableau(self):
        self.setup(researcher=True, apikey=True, study=True)
        ApiKey.objects.filter(access_key_id=self.api_key_public).update(has_tableau_api_permissions=False)
        headers = {"X-Access-Key-Id": self.api_key_public, "X-Access-Key-Secret": self.api_key_private}
        with self.assertRaises(PermissionDenied) as cm:
            with app.test_request_context(headers=headers):
                TableauApiView().check_permissions(study_id=self.study.object_id)

    def test_check_permissions_bad_study(self):
        self.setup(researcher=True, apikey=True, study=True)
        headers = {"X-Access-Key-Id": self.api_key_public, "X-Access-Key-Secret": self.api_key_private}
        with self.assertRaises(PermissionDenied) as cm:
            with app.test_request_context(headers=headers):
                TableauApiView().check_permissions(study_id=" bad study id ")

    def test_check_permissions_no_study_permission(self):
        self.setup(researcher=True, apikey=True, study=True)
        StudyRelation.objects.filter(study=self.study, researcher=self.researcher).delete()
        headers = {"X-Access-Key-Id": self.api_key_public, "X-Access-Key-Secret": self.api_key_private}
        with self.assertRaises(PermissionDenied) as cm:
            with app.test_request_context(headers=headers):
                TableauApiView().check_permissions(study_id=self.study.object_id)

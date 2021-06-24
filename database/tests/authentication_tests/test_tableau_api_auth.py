import mock
import requests

from api.tableau_api.base import AuthenticationFailed, PermissionDenied, TableauApiView
from api.tableau_api.constants import X_ACCESS_KEY_ID, X_ACCESS_KEY_SECRET
from api.tableau_api.views import SummaryStatisticDailySerializer
from app import app
from database.security_models import ApiKey
from database.study_models import DeviceSettings, Study
from database.tableau_api_models import ForestParam
from database.tests.authentication_tests.django_flask_hybrid_test_framework import HybridTest
from database.tests.authentication_tests.testing_constants import (BASE_URL, TEST_PASSWORD,
    TEST_STUDY_ENCRYPTION_KEY, TEST_STUDY_NAME, TEST_USERNAME)
from database.user_models import Researcher, StudyRelation


class TableauApiAuthTests(HybridTest):
    """
    Test methods of the api authentication system
    """

    def setup(self, researcher=True, apikey=True, study=True):
        """
        This function is used to initialize that database for each test.

        This was written in this fashion because of a known issue with the HybridTest class that does not consistently
        use database changes that persist between tests
        """
        if apikey and not researcher:
            raise Exception("invalid setup criteria")
        if researcher:
            self.researcher = Researcher.create_with_password(
                username=TEST_USERNAME, password=TEST_PASSWORD
            )
        if apikey:
            self.api_key = ApiKey.generate(
                self.researcher, has_tableau_api_permissions=True
            )
            self.api_key_public = self.api_key.access_key_id
            self.api_key_private = self.api_key.access_key_secret_plaintext
        if study:
            ForestParam.objects.get_or_create(
                default=True,
                defaults={
                    "jasmine_json_string": "{}",
                    "willow_json_string": "{}",
                })
            self.study = Study.create_with_object_id(
                device_settings=DeviceSettings(),
                encryption_key=TEST_STUDY_ENCRYPTION_KEY,
                forest_enabled=True,
                name=TEST_STUDY_NAME,
            )
            if researcher:
                self.study_relation = StudyRelation(
                    study=self.study,
                    researcher=self.researcher,
                    relationship="researcher",
                ).save()

    def login(self, session=None):
        if session is None:
            session = requests.Session()
        session.post(
            self.url_for("login_pages.login"),
            data={"username": TEST_USERNAME, "password": TEST_PASSWORD},
        )
        return session

    @property
    def default_header(self):
        # intentionally returns a new object every time, just in case it is mutated.
        return {
            X_ACCESS_KEY_ID: self.api_key_public,
            X_ACCESS_KEY_SECRET: self.api_key_private,
        }

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
        api_key_count = ApiKey.objects.count()
        response = session.post(
            self.url_for("admin_pages.new_api_key"),
            data={"readable_name": "test_generated_api_key"},
        )
        self.assertEqual(api_key_count + 1, ApiKey.objects.count())
        api_key = ApiKey.objects.get(readable_name="test_generated_api_key")
        self.assertEqual(api_key.researcher.username, TEST_USERNAME)
        self.assertTrue(api_key.is_active)
        self.assertTrue(api_key.has_tableau_api_permissions)
        return True

    def test_disable_api_key(self):
        """
        Asserts that:
            -exactly one fewer active api key is present in the database
            -the api key is no longer active
        """
        self.setup(researcher=True, apikey=True, study=False)
        session = self.login()
        api_key_count = ApiKey.objects.filter(is_active=True).count()
        response = session.post(
            self.url_for("admin_pages.disable_api_key"),
            data={"api_key_id": self.api_key_public},
        )
        key = ApiKey.objects.get(access_key_id=self.api_key_public)
        self.assertEqual(api_key_count - 1, ApiKey.objects.filter(is_active=True).count())
        self.assertFalse(key.is_active)
        return True

    def test_check_permissions_working(self):
        self.setup(researcher=True, apikey=True, study=True)
        with app.test_request_context(headers=self.default_header):
            self.assertTrue(
                TableauApiView().check_permissions(study_object_id=self.study.object_id)
            )

    def test_check_permissions_none(self):
        self.setup(researcher=True, apikey=True, study=True)
        with self.assertRaises(AuthenticationFailed) as cm:
            with app.test_request_context(headers={}):
                TableauApiView().check_permissions(study_object_id=self.study.object_id)

    def test_check_permissions_inactive(self):
        self.setup(researcher=True, apikey=True, study=True)
        self.api_key.update(is_active=False)
        with self.assertRaises(AuthenticationFailed) as cm:
            with app.test_request_context(headers=self.default_header):
                TableauApiView().check_permissions(study_object_id=self.study.object_id)

    def test_check_permissions_bad_secret(self):
        self.setup(researcher=True, apikey=True, study=True)
        # note that ':' does not appear in base64 encoding, preventing any collision errors based on
        # the current implementation
        headers = {
            X_ACCESS_KEY_ID: self.api_key_public,
            X_ACCESS_KEY_SECRET: ":::" + self.api_key_private[3:],
        }
        with self.assertRaises(AuthenticationFailed) as cm:
            with app.test_request_context(headers=headers):
                TableauApiView().check_permissions(study_object_id=self.study.object_id)

    def test_check_permissions_no_tableau(self):
        self.setup(researcher=True, apikey=True, study=True)
        ApiKey.objects.filter(access_key_id=self.api_key_public).update(
            has_tableau_api_permissions=False
        )
        with self.assertRaises(PermissionDenied) as cm:
            with app.test_request_context(headers=self.default_header):
                TableauApiView().check_permissions(study_object_id=self.study.object_id)

    def test_check_permissions_bad_study(self):
        self.setup(researcher=True, apikey=True, study=True)
        self.assertFalse(ApiKey.objects.filter(access_key_id=" bad study id ").exists())
        with self.assertRaises(PermissionDenied) as cm:
            with app.test_request_context(headers=self.default_header):
                TableauApiView().check_permissions(study_object_id=" bad study id ")

    def test_check_permissions_no_study_permission(self):
        self.setup(researcher=True, apikey=True, study=True)
        StudyRelation.objects.filter(
            study=self.study, researcher=self.researcher
        ).delete()
        with self.assertRaises(PermissionDenied) as cm:
            with app.test_request_context(headers=self.default_header):
                TableauApiView().check_permissions(study_object_id=self.study.object_id)

    def test_tableau_api_dispatch(self):
        """
        Ensures the Tableau Api checks permissions during dispatch
        """
        with app.test_request_context():
            with mock.patch.object(
                TableauApiView, "check_permissions", return_value=True
            ) as mock_method:
                TableauApiView().check_permissions()
        self.assertTrue(mock_method.called)

    def test_summary_statistic_daily_serializer(self):
        serializer = SummaryStatisticDailySerializer()
        self.assertFalse("created_on" in serializer.fields)
        self.assertFalse("last_updated" in serializer.fields)

    def test_summary_statistics_daily_view(self):
        self.setup(apikey=True, researcher=True, study=True)
        session = requests.Session()
        response = session.get(
            BASE_URL
            + "/api/v0/studies/<string:study_object_id>/summary-statistics/daily".replace(
                "<string:study_object_id>", self.study.object_id
            ),
            headers=self.default_header,
        )
        self.assertEqual(response.status_code, 200)

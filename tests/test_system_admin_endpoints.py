from unittest.mock import MagicMock, patch

from django.core.files.uploadedfile import SimpleUploadedFile

from constants.celery_constants import (ANDROID_FIREBASE_CREDENTIALS, BACKEND_FIREBASE_CREDENTIALS,
    IOS_FIREBASE_CREDENTIALS)
from constants.testing_constants import ANDROID_CERT, BACKEND_CERT, IOS_CERT
from constants.user_constants import ResearcherRole
from database.system_models import FileAsText
from tests.common import ResearcherSessionTest


class TestManageFirebaseCredentials(ResearcherSessionTest):
    ENDPOINT_NAME = "system_admin_endpoints.manage_firebase_credentials"
    
    def test(self):
        # just test that the page loads, I guess
        self.set_session_study_relation(ResearcherRole.site_admin)
        self.smart_get_status_code(200)


# FIXME: implement tests for error cases
class TestUploadBackendFirebaseCert(ResearcherSessionTest):
    ENDPOINT_NAME = "system_admin_endpoints.upload_backend_firebase_cert"
    REDIRECT_ENDPOINT_NAME = "system_admin_endpoints.manage_firebase_credentials"
    
    @patch("endpoints.system_admin_endpoints.update_firebase_instance")
    @patch("endpoints.system_admin_endpoints.get_firebase_credential_errors")
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
    ENDPOINT_NAME = "system_admin_endpoints.upload_ios_firebase_cert"
    REDIRECT_ENDPOINT_NAME = "system_admin_endpoints.manage_firebase_credentials"
    
    def test(self):
        # test upload as site admin
        self.set_session_study_relation(ResearcherRole.site_admin)
        file = SimpleUploadedFile("ios_firebase_cert.plist", IOS_CERT.encode(), "text/json")
        self.smart_post(ios_firebase_cert=file)
        resp_content = self.redirect_get_contents()
        self.assert_present("New IOS credentials were received", resp_content)


# FIXME: implement tests for error cases
class TestUploadAndroidFirebaseCert(ResearcherSessionTest):
    ENDPOINT_NAME = "system_admin_endpoints.upload_android_firebase_cert"
    REDIRECT_ENDPOINT_NAME = "system_admin_endpoints.manage_firebase_credentials"
    
    def test(self):
        # test upload as site admin
        self.set_session_study_relation(ResearcherRole.site_admin)
        file = SimpleUploadedFile("android_firebase_cert.json", ANDROID_CERT.encode(), "text/json")
        self.smart_post(android_firebase_cert=file)
        resp_content = self.redirect_get_contents()
        self.assert_present("New android credentials were received", resp_content)


class TestDeleteFirebaseBackendCert(ResearcherSessionTest):
    ENDPOINT_NAME = "system_admin_endpoints.delete_backend_firebase_cert"
    REDIRECT_ENDPOINT_NAME = "system_admin_endpoints.manage_firebase_credentials"
    
    def test(self):
        self.set_session_study_relation(ResearcherRole.site_admin)
        FileAsText.objects.create(tag=BACKEND_FIREBASE_CREDENTIALS, text="any_string")
        self.smart_post()
        self.assertFalse(FileAsText.objects.exists())


class TestDeleteFirebaseIosCert(ResearcherSessionTest):
    ENDPOINT_NAME = "system_admin_endpoints.delete_ios_firebase_cert"
    REDIRECT_ENDPOINT_NAME = "system_admin_endpoints.manage_firebase_credentials"
    
    def test(self):
        self.set_session_study_relation(ResearcherRole.site_admin)
        FileAsText.objects.create(tag=IOS_FIREBASE_CREDENTIALS, text="any_string")
        self.smart_post()
        self.assertFalse(FileAsText.objects.exists())


class TestDeleteFirebaseAndroidCert(ResearcherSessionTest):
    ENDPOINT_NAME = "system_admin_endpoints.delete_android_firebase_cert"
    REDIRECT_ENDPOINT_NAME = "system_admin_endpoints.manage_firebase_credentials"
    
    def test(self):
        self.set_session_study_relation(ResearcherRole.site_admin)
        FileAsText.objects.create(tag=ANDROID_FIREBASE_CREDENTIALS, text="any_string")
        self.smart_post()
        self.assertFalse(FileAsText.objects.exists())

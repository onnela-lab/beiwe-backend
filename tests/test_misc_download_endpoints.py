from django.http.response import HttpResponseRedirect

from tests.common import ResearcherSessionTest


class TestPrivacyPolicy(ResearcherSessionTest):
    ENDPOINT_NAME = "misc_download_endpoints.download_privacy_policy"
    
    def test(self):
        # just test that it loads without breaking
        redirect = self.smart_get()
        self.assertIsInstance(redirect, HttpResponseRedirect)

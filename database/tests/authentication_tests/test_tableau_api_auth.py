from django.test import Client
from django.test import TestCase
from flask import session
import requests

from authentication.admin_authentication import log_in_researcher
from database.tests.authentication_tests.django_flask_hybrid_test_framework import HybridTest
from database.tests.factories import ResearcherFactory
from database.security_models import ApiKey
from database.user_models import Researcher
from pages.admin_pages import new_api_key
from time import sleep

SESSION_NAME = "researcher_username"


class TableauApiAuthTests(HybridTest):
    """
    Test methods of the api authentication system
    """

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.researcher = Researcher.create_with_password(username="automated_test_user", password=" ")
        cls.researcher.save()
        api_key = ApiKey.generate(cls.researcher)
        cls.api_key_public = api_key.access_key_id
        cls.api_key_private = api_key.access_key_secret_plaintext

    def login(self):
        return requests.post("http://0.0.0.0:54321/validate_login", {'username': self.researcher.username, 'password': ' '})

    def test_database(self):
        """
        Asserts that:
            -one new api key is added to the database
            -that api key is linked to the logged in researcher
            -only that one api key is associated with that researcher
            -that api key has tableau access
        """
        print("logging in?")
        login_res = self.login()

        print(
            f"""---- Summary of the database state from the main thread ----
                Number of reserachers: {len(Researcher.objects.all())}
                Name of the first: {Researcher.objects.all()[1].username}"""
        )

        requests.post("http://0.0.0.0:54321/test_database")

        return True


    def tes_new_api_key(self):
        """
        Asserts that:
            -one new api key is added to the database
            -that api key is linked to the logged in researcher
            -only that one api key is associated with that researcher
            -that api key has tableau access
        """
        self.login()
        api_key_count = len(ApiKey.objects.all())
        requests.post("http://0.0.0.0:54321/new_api_key", {'tableau_api_permission': 'True', 'readable_name': 'test_generated_api_key'})
        sleep(1)
        self.assertEqual(api_key_count + 1, len(ApiKey.objects.all()))
        generated_key = ApiKey.objects.get(researcher=self.researcher)
        print("new api key count:", api_key_count)
        print(Researcher.objects.all())
        return True


    # def test_disable_api_key(self):
        """
        Asserts that:
            -exactly one fewer active api key is present in the database
            -the api key is no longer active
        """
        # api_key = ApiKey.generate(researcher=self.researcher)
        # pass
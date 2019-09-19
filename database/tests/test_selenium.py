import threading
import traceback
from time import sleep
from pprint import pprint

import re

import subprocess

from api import mobile_api
#from config import load_django

from selenium import webdriver
from selenium.common.exceptions import (ElementNotVisibleException, WebDriverException,
                                        NoSuchElementException, InvalidElementStateException)
from selenium.webdriver.common.by import By
from selenium.webdriver.support.expected_conditions import visibility_of_element_located
from selenium.webdriver.support.select import Select
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.options import Options

from django.test import TransactionTestCase, SimpleTestCase
from flask import Flask, request
from app import app as flask_app, subdomain
from config.django_settings import WEBDRIVER_LOC
from database.study_models import Study
from database.user_models import Researcher
from pages import admin_pages

PAGE_RESPONSES = {
    '400': '</head><body><h1>Bad Request</h1>',
    '401': "<p>The server could not verify that you are authorized to access the URL requested.  You either supplied the wrong credentials (e.g. a bad password), or your browser doesn't understand how to supply the credentials required.</p>",
    '403': "<p>You don't have the permission to access the requested resource. It is either read-protected or not readable by the server.</p>",
    '404': '<h1 class="center">404 Page Not Found</h1>',
    '405': "<p>The method is not allowed for the requested URL.</p>",
    '500': '<p>The server encountered an internal error and was unable to complete your request.  Either the server is overloaded or there is an error in the application.</p>',
}

ADMIN_PAGES = {
    '/reset_download_api_credentials': {'method': 'pass'},
    '/reset_admin_password': {'method': 'pass'},
    '/manage_credentials': {'method': 'get'},
    '/validate_login': {'method': 'pass'},
    '/choose_study': {'method': 'get'},
    '/logout': {'method': 'pass'},
    '/admin': {'method': 'get'},
    '/': {'method': 'get'},
    '/data-pipeline/<string:study_id>': {'method': 'get_param'},
    '/view_study/<string:study_id>': {'method': 'get_param'},
    '/static/<path:filename>': {'method': 'pass'},
}


def pause_if_error(func):
    def inner_pause_if_error(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            print "=============================================="
            if isinstance(e, AssertionError):
                pprint(e)
                print "=============================================="
                input()
                raise

            traceback.print_exc()
            print "=============================================="
            print "Press enter to exit"
            input()
            raise

    return inner_pause_if_error


class FlaskTest(TransactionTestCase):
    @classmethod
    def setUpClass(cls):
        chrome_options = Options()
        cls.selenium = webdriver.Chrome(WEBDRIVER_LOC,
                                        chrome_options=chrome_options)
        cls.selenium.set_page_load_timeout(10)

        cls.flask_task = threading.Thread(target=run_flask)

        # Make thread a deamon so the main thread won't wait for it to close
        cls.flask_task.daemon = True

        # Start thread
        cls.flask_task.start()
        sleep(1)
        super(FlaskTest, cls).setUpClass()

    @classmethod
    def tearDownClass(cls):
        # end flask thread by giving it a timeout
        cls.flask_task.join(.1)
        cls.selenium.close()
        super(FlaskTest, cls).tearDownClass()


class TestRoutes(FlaskTest):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    @pause_if_error
    def test_all_routes(self):
        """
        Tests urls
        """
        app2 = subdomain("frontend")
        app2.register_blueprint(admin_pages.admin_pages)

        long_encryption_key = 'aabbccddefggiijjkklmnoppqqrrsstt'

        researcher = Researcher.create_with_password('test_user', 'test_password')
        researcher.admin = True
        researcher.reset_access_credentials()
        researcher.save()

        study = Study.create_with_object_id(name='test_study', encryption_key=long_encryption_key)
        researcher.studies.add(study)

        self.selenium.get("localhost:54321")
        self.selenium.find_element_by_name('username').send_keys('test_user')
        self.selenium.find_element_by_name('password').send_keys('test_password')
        self.selenium.find_element_by_name('submit').click()

        for rule in app2.url_map.iter_rules():
            str_rule = str(rule)
            self.assertIn(str_rule, ADMIN_PAGES.keys())

            if ADMIN_PAGES[str_rule]['method'] == 'get':
                self.selenium.get("localhost:54321" + str_rule)
            elif ADMIN_PAGES[str_rule]['method'] == 'post':
                continue
            elif ADMIN_PAGES[str_rule]['method'] == 'get_param':
                str_rule_formatted = re.sub(r"<\w+:\w+>", str(study.id), str_rule)
                self.selenium.get("localhost:54321" + str_rule_formatted)
            else:
                continue

            response = self.determine_errors()
            self.assertEqual(response, '200')

    def determine_errors(self):

        responses = []
        for response_code, page_response in PAGE_RESPONSES.items():
            if page_response in self.selenium.page_source:
                responses.append(response_code)

        if len(responses) > 1:
            raise Exception("One page has indicators of multiple page responses")
        elif len(responses) == 1:
            return responses[0]
        else:
            return "200"


def run_flask():
    flask_app.run(host='0.0.0.0', port=54321, debug=False)


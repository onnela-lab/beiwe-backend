import threading

from flask import url_for as flask_url_for

from app import app as flask_app
from django.test import TransactionTestCase

from database.tests.authentication_tests.testing_constants import HOST, PORT


class HybridTest(TransactionTestCase):
    """
    This class extends the django testing classes to function within the hybrid django/flask environment

    These tests function by forking a thread to run the flask server. These tests are not compatible with the django
    test client. The Requests module is recommended for accessing server endpoints.

    for reasons that are not fully understood, further overriding the setUpClass with functionality to initialize the
    database behaves in odd ways, and often differently between tests. It is recommended to run code to setup each test
    within in that test to prevent these errors
    """

    @classmethod
    def setUpClass(cls):
        cls.flask_task = threading.Thread(target=cls.run_flask)

        # Make thread a deamon so the main thread won't wait for it to close
        cls.flask_task.daemon = True

        # Start thread
        cls.flask_task.start()
        super(HybridTest, cls).setUpClass()

    @classmethod
    def tearDownClass(cls):
        # end flask thread by giving it a timeout
        cls.flask_task.join(0.1)
        super(HybridTest, cls).tearDownClass()

    @staticmethod
    def run_flask():
        flask_app.run(host=HOST, port=PORT, debug=False)

    @staticmethod
    def url_for(endpoint, **values):
        flask_app.config["SERVER_NAME"] = f"{HOST}:{PORT}"
        with flask_app.app_context():
            return flask_url_for(endpoint, **values)

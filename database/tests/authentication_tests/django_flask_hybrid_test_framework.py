import threading

from app import app as flask_app
from django.test import TransactionTestCase
from time import sleep

from database.tests.authentication_tests.testing_constants import HOST, PORT


class HybridTest(TransactionTestCase):
    """
    This class extends the django testing classes to function within the hybrid django/flask environment

    These tests function by forking a thread to run the flask server. These tests are not compatible with the django
    test client. The Requests module is recommended for accessing server endpoints.
    """
    @classmethod
    def setUpClass(cls):
        cls.flask_task = threading.Thread(target=cls.run_flask)

        # Make thread a deamon so the main thread won't wait for it to close
        cls.flask_task.daemon = True

        # Start thread
        cls.flask_task.start()
        sleep(1)
        super(HybridTest, cls).setUpClass()

    @classmethod
    def tearDownClass(cls):
        # end flask thread by giving it a timeout
        cls.flask_task.join(.1)
        super(HybridTest, cls).tearDownClass()

    @staticmethod
    def run_flask():
        flask_app.run(host=HOST, port=PORT, debug=False)


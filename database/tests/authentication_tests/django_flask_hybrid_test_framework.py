import threading
import traceback

from app import app as flask_app, subdomain
from django.test import TransactionTestCase
from time import sleep

from pages import admin_pages


def run_flask():
    flask_app.run(host='0.0.0.0', port=54321, debug=False)


class HybridTest(TransactionTestCase):
    @classmethod
    def setUpClass(cls):
        cls.flask_task = threading.Thread(target=run_flask)

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

# names in this file's scope are passed in to the django settings.configure command in load_django.

import os
import sys
from os.path import dirname, join

from config import DB_MODE, DB_MODE_POSTGRES, DB_MODE_SQLITE
from config.settings import FLASK_SECRET_KEY

DB_PATH = join(dirname(dirname(__file__)), "private/beiwe_db.sqlite")
TEST_DATABASE_PATH = join(dirname(dirname(__file__)), 'private/tests_db.sqlite')

# SECRET KEY is required by the django management commands, using the flask key is fine because
# we are not actually using it in any server runtime capacity.
SECRET_KEY = FLASK_SECRET_KEY
if DB_MODE == DB_MODE_SQLITE:
    DATABASES = {
        'default': {
            'ENGINE': 'django.db.backends.sqlite3',
            'NAME': DB_PATH,
            'CONN_MAX_AGE': None,
        },
    }
elif DB_MODE == DB_MODE_POSTGRES:
    DATABASES = {
        'default': {
            'ENGINE': 'django.db.backends.postgresql',
            'NAME': os.environ['RDS_DB_NAME'],
            'USER': os.environ['RDS_USERNAME'],
            'PASSWORD': os.environ['RDS_PASSWORD'],
            'HOST': os.environ['RDS_HOSTNAME'],
            'CONN_MAX_AGE': None,
            'OPTIONS': {'sslmode': 'require'},
        },
    }
else:
    from django.core.exceptions import ImproperlyConfigured
    raise ImproperlyConfigured("server not running as expected, could not find environment variable DJANGO_DB_ENV")

TIME_ZONE = 'UTC'
USE_TZ = True

INSTALLED_APPS = [
    'database.apps.DatabaseConfig',
    'django_extensions',
    'timezone_field',
    'rest_framework',
]

SHELL_PLUS = "ipython"

SHELL_PLUS_POST_IMPORTS = [
    "pytz",
    "json",
    ["libs.s3", ("s3_list_files",)],
    ["pprint", ("pprint",)],
    ["datetime", ("date", "datetime", "timedelta", "tzinfo")],
    ["collections", ("Counter", "defaultdict")],
    ["django.utils.timezone", ("localtime", "make_aware", "make_naive")],
    ["time", ("sleep",)],
    ["libs.shell_utils", "*"],
    ["dateutil", ('tz',)],
    ['libs.dev_utils', "GlobalTimeTracker"],
    # ['libs.celery_control', (
    #     "get_notification_scheduled_job_ids",
    #     "get_notification_reserved_job_ids",
    #     "get_notification_active_job_ids",
    #     "get_processing_scheduled_job_ids",
    #     "get_processing_reserved_job_ids",
    #     "get_processing_active_job_ids",
    # )]
]
SHELL_PLUS_PRE_IMPORTS = []

# Using the default test runner
TEST_RUNNER = 'django.test.runner.DiscoverRunner'

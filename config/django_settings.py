import os
import sys
from os.path import dirname, join

from config.settings import FLASK_SECRET_KEY

DB_PATH = join(dirname(dirname(__file__)), "private/beiwe_db.sqlite")
TEST_DATABASE_PATH = join(dirname(dirname(__file__)), 'private/tests_db.sqlite')

# SECRET KEY is required by the django management commands, using the flask key is fine because
# we are not actually using it in any server runtime capacity.
SECRET_KEY = FLASK_SECRET_KEY
if 'test' in sys.argv:
    WEBDRIVER_LOC = os.environ.get('WEBDRIVER_LOC', '')
    DATABASES = {
        'default': {
            'ENGINE': 'django.db.backends.sqlite3',
            'NAME': TEST_DATABASE_PATH,
            'TEST_NAME': TEST_DATABASE_PATH,
            'TEST': {'NAME': TEST_DATABASE_PATH},
            'CONN_MAX_AGE': None,
        }
    }
elif os.environ['DJANGO_DB_ENV'] == "local":
    DATABASES = {
        'default': {
            'ENGINE': 'django.db.backends.sqlite3',
            'NAME': DB_PATH,
            'CONN_MAX_AGE': None,
        },
    }
elif os.environ['DJANGO_DB_ENV'] == "remote":
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
]

SHELL_PLUS = "ipython"

SHELL_PLUS_PRE_IMPORTS = []
SHELL_PLUS_POST_IMPORTS = [
    ["libs.s3", ("s3_list_files")],
    ["pprint", ("pprint")],
    ["datetime", ("date", "datetime", "timedelta")],
]

# Using the default test runner
TEST_RUNNER = 'django.test.runner.DiscoverRunner'

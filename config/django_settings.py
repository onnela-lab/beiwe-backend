import os
import platform
from os.path import join

import sentry_sdk
from django.core.exceptions import ImproperlyConfigured
from sentry_sdk.integrations.celery import CeleryIntegration
from sentry_sdk.integrations.django import DjangoIntegration

from config import DB_MODE, DB_MODE_POSTGRES, DB_MODE_SQLITE
from config.settings import DOMAIN_NAME, FLASK_SECRET_KEY, SENTRY_ELASTIC_BEANSTALK_DSN
from constants.common_constants import BEIWE_PROJECT_ROOT
from libs.sentry import normalize_sentry_dsn


# SECRET KEY is required by the django management commands, using the flask key is fine because
# we are not actually using it in any server runtime capacity.
SECRET_KEY = FLASK_SECRET_KEY

# TODO: remove sqlite support entirely.
if DB_MODE == DB_MODE_SQLITE:
    DATABASES = {
        'default': {
            'ENGINE': 'django.db.backends.sqlite3',
            'NAME': join(BEIWE_PROJECT_ROOT, "private/beiwe_db.sqlite"),
            'CONN_MAX_AGE': None,
            'TEST': {
                'NAME': join(BEIWE_PROJECT_ROOT, "private/beiwe_test_db.sqlite"),
            },
        },
    }
elif DB_MODE == DB_MODE_POSTGRES:
    CONN_HEALTH_CHECKS = True  # new in django 4.1, allows for health checks on the database connection
    DATABASES = {
        'default': {
            'ENGINE': 'django.db.backends.postgresql',
            'NAME': os.environ['RDS_DB_NAME'],
            'USER': os.environ['RDS_USERNAME'],
            'PASSWORD': os.environ['RDS_PASSWORD'],
            'HOST': os.environ['RDS_HOSTNAME'],
            'CONN_MAX_AGE': 0,
            'OPTIONS': {'sslmode': 'require'},
            "ATOMIC_REQUESTS": True,  # default is True, just being explicit
            'TEST': {
                'MIGRATE': True,
            }
        },
    }
else:
    raise ImproperlyConfigured("server not running as expected, could not find environment variable DJANGO_DB_ENV")

# database primary key setting
DEFAULT_AUTO_FIELD = "django.db.models.AutoField"

DEBUG = 'localhost' in DOMAIN_NAME or '127.0.0.1' in DOMAIN_NAME or '::1' in DOMAIN_NAME

SECURE_SSL_REDIRECT = not DEBUG
SECURE_PROXY_SSL_HEADER = ('HTTP_X_FORWARDED_PROTO', 'https')
CSRF_COOKIE_SECURE = not DEBUG
SESSION_COOKIE_SECURE = not DEBUG

# mac os homebrew postgres has configuration complexities that are not worth the effort to resolve.
if (not SECURE_SSL_REDIRECT and DB_MODE == DB_MODE_POSTGRES and platform.system() == "Darwin") or os.environ.get("RUNNING_IN_DOCKER", False):
    DATABASES['default']['OPTIONS']['sslmode'] = 'disable'

MIDDLEWARE = [
    'middleware.downtime_middleware.DowntimeMiddleware',  # does a single database call
    'django.middleware.security.SecurityMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    # 'django.middleware.csrf.CsrfViewMiddleware',
    # 'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
    'middleware.abort_middleware.AbortMiddleware',
    # "middleware.request_to_curl.CurlMiddleware",  # uncomment to enable a debugging tool
]

TIME_ZONE = 'UTC'
USE_TZ = True

INSTALLED_APPS = [
    'database.apps.DatabaseConfig',
    'django.contrib.sessions',
    'django_extensions',
    'timezone_field',
    'django.contrib.staticfiles'
    # 'static_files',
]

SHELL_PLUS = "ipython"
# if we enable sql printing, don't truncate the output.
RUNSERVER_PLUS_PRINT_SQL_TRUNCATE = None
SHELL_PLUS_PRINT_SQL_TRUNCATE = None
# SHELL_PLUS_PRINT_SQL = True

SHELL_PLUS_POST_IMPORTS = [
    # generic
    "json",
    "orjson",
    ["collections", ("Counter", "defaultdict")],
    ["pprint", ("pprint",)],
    
    # datetimezone
    "dateutil",  # do not add pytz it is deprecated
    ["dateutil", ('tz',)],
    ["dateutil.tz", ('UTC',)],
    ["time", ("sleep",)],
    ["datetime", ("date", "datetime", "timedelta", "tzinfo")],
    ["django.utils.timezone", ("localtime", "make_aware", "make_naive")],
    
    # shell
    ["libs.shell_support", "*"],
    ['libs.utils.dev_utils', "GlobalTimeTracker"],
    
    # s3
    [
        "libs.s3",
        (
            "s3_list_files", "s3_upload", "s3_upload_plaintext", "s3_retrieve",
            "s3_retrieve_plaintext"
        )
    ],
    
    # I need to be able to paste code >_O
    ["typing", ("List", "Dict", "Tuple", "Union", 'Counter', 'Deque', 'Dict', 'DefaultDict')],
    
    # really useful constants
    ["constants.user_constants", ("ANDROID_API", "IOS_API", "NULL_OS", "ResearcherRole")],
]
SHELL_PLUS_PRE_IMPORTS = []

# Using the default test runner
TEST_RUNNER = 'django.test.runner.DiscoverRunner'

# server settings....
if DEBUG:
    ALLOWED_HOSTS = ("*",)
else:
    # we only allow the domain name to be the referrer
    ALLOWED_HOSTS = [DOMAIN_NAME]

PROJECT_ROOT = "."
ROOT_URLCONF = "urls"
STATIC_ROOT = "staticfiles"
STATIC_URL = "/static/"
STATICFILES_DIRS = [
    "frontend/static/"
]

TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.jinja2.Jinja2',
        'APP_DIRS': False,
        'DIRS': [
            "frontend/templates/",
        ],
        'OPTIONS': {
            'autoescape': True,
            'context_processors': [
                "middleware.context_processors.researcher_context_processor",
                "django.contrib.messages.context_processors.messages",
            ],
            "environment": "config.jinja2.environment",
        },
    },
]

# json serializer crashes with module object does not have attribute .dumps
# or it cannot serialize a datetime object.
SESSION_SERIALIZER = 'django.contrib.sessions.serializers.PickleSerializer'
SESSION_ENGINE = "database.user_models_researcher"

# https-only
# SESSION_COOKIE_SECURE = True

# Changing this causes a runtime warning, but has no effect. Enabling this feature is not equivalent
# to the feature in urls.py.
APPEND_SLASH = False

# We need this to be fairly large, if users ever encounter a problem with this please report it
DATA_UPLOAD_MAX_MEMORY_SIZE = 128 * 1024 * 1024  # 128 MB

# enable Sentry error reporting
our_sentry_dsn = normalize_sentry_dsn(SENTRY_ELASTIC_BEANSTALK_DSN)

# We encounter the starlette integration bug _at least_ when running tasks in celery.
# https://github.com/getsentry/sentry-python/issues/1603
# None of the fixes work, so we are going with the nuclear option of purging the integration from
# _AUTO_ENABLING_INTEGRATIONS inside the integrations code. This is very bad form, but without it
# file processing errors in a weird/unpredictable way. (Possibly after the first page of data? it's
# not clear.)
from sentry_sdk.integrations import _AUTO_ENABLING_INTEGRATIONS


if "sentry_sdk.integrations.starlette.StarletteIntegration" not in _AUTO_ENABLING_INTEGRATIONS:
    raise ImproperlyConfigured(
        "We have a bug where the starlette integration is getting auto enabling and then raising "
        "an error. There is no good option here, but this message is better than the next line"
        "failing. Sorry future person!"
    )
_AUTO_ENABLING_INTEGRATIONS.remove("sentry_sdk.integrations.starlette.StarletteIntegration")

# Ok now we can
sentry_sdk.init(
    dsn=our_sentry_dsn,
    enable_tracing=False,
    ignore_errors=["WorkerLostError", "DisallowedHost"],
    # auto_enabling_integrations=False,  # this was one of the fixes for the starlette bug that didn't work.
    integrations=[
        DjangoIntegration(
            transaction_style='url',
            middleware_spans=False,
            signals_spans=False,
            cache_spans=False,
        ),
        CeleryIntegration(
            propagate_traces=False,
            monitor_beat_tasks=False,
            exclude_beat_tasks=True
        )
    ],
)

# I don't know what this does after replacing raven with sentry_sdk...
if not DEBUG and SENTRY_ELASTIC_BEANSTALK_DSN:
    # custom tags have been disabled
    LOGGING = {
        'version': 1,
        'disable_existing_loggers': False,
        'formatters':
            {
                'verbose':
                    {
                        'format':
                            '%(levelname)s %(asctime)s %(module)s '
                            '%(process)d %(thread)d %(message)s'
                    },
            },
        'handlers':
            {
                'console':
                    {
                        'level': 'DEBUG',
                        'class': 'logging.StreamHandler',
                        'formatter': 'verbose'
                    }
            },
        'loggers':
            {
                'root': {
                    'level': 'WARNING',
                    'handlers': ['console'],
                },
                'django.db.backends':
                    {
                        'level': 'ERROR',
                        'handlers': ['console'],
                        'propagate': True,
                    },
                'sentry.errors': {
                    'level': 'WARNING',
                    'handlers': ['console'],
                    'propagate': True,
                },
            },
    }

TEST_RUNNER = "redgreenunittest.django.runner.RedGreenDiscoverRunner"


def assert_assertions_not_disabled():
    """ stick in function to keep namespace clear. """
    success = False
    try:
        assert False
    except AssertionError:
        success = True
    
    if not success:
        raise ImproperlyConfigured(
            "Assertions are disabled. Assertions are a completely reasonable control flow "
            "construct and need to be enabled for proper functioning of The Beiwe Platform."
        )


assert_assertions_not_disabled()

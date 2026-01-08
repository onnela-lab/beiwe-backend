import os
import platform
import warnings

import sentry_sdk
from django.core.exceptions import ImproperlyConfigured
from sentry_sdk.integrations import _AUTO_ENABLING_INTEGRATIONS
from sentry_sdk.integrations.celery import CeleryIntegration
from sentry_sdk.integrations.django import DjangoIntegration

from config.settings import DOMAIN_NAME, FLASK_SECRET_KEY, SENTRY_ELASTIC_BEANSTALK_DSN
from libs.sentry import normalize_sentry_dsn


# before anything else, determine if we are running in debug / development mode based off the domain
DEBUG = 'localhost' in DOMAIN_NAME or '127.0.0.1' in DOMAIN_NAME or '::1' in DOMAIN_NAME

####################################################################################################
################################### General Web Connections ########################################
####################################################################################################

# We need this to be fairly large, if users ever encounter a problem with this please report it
DATA_UPLOAD_MAX_MEMORY_SIZE = 128 * 1024 * 1024  # 128 MB

SECURE_SSL_REDIRECT = not DEBUG
SECURE_PROXY_SSL_HEADER = ('HTTP_X_FORWARDED_PROTO', 'https')  # (when running on Elastic Beanstalk)

####################################################################################################
##################################### Django Session Backend #######################################
####################################################################################################

# json serializer crashes with module object does not have attribute .dumps
# or it cannot serialize a datetime object.
SESSION_SERIALIZER = 'django.contrib.sessions.serializers.JSONSerializer'
SESSION_ENGINE = "database.user_models_researcher"

SECRET_KEY = FLASK_SECRET_KEY  # "~FLASK~" is because we started as a flask app many years ago

CSRF_COOKIE_SECURE = not DEBUG
SESSION_COOKIE_SECURE = not DEBUG

####################################################################################################
#################################### Database Configuration ########################################
####################################################################################################

DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.postgresql',
        'NAME': os.environ['RDS_DB_NAME'],
        'USER': os.environ['RDS_USERNAME'],
        'PASSWORD': os.environ['RDS_PASSWORD'],
        'HOST': os.environ['RDS_HOSTNAME'],
        'CONN_MAX_AGE': 0,
        'CONN_HEALTH_CHECKS': True,
        'OPTIONS': {
            'sslmode': 'require',
            # connection pools appear to be fully  broken, resulting in connection errors after a
            # few hours. There are definitely options to explore, but it worked for ages without the
            # pool, so who cares, we are done with it this.
            # "pool": {
            #     # settings of min_size: 0, max_size: 30, timeout: 10, had connection errors after 6 hours
            #     "min_size": 0,       # scale down minimum
            #     "max_size": 60,      # scale up maximum
            #     "timeout": 10,       # seconds to wait for a connection in the pool before giving up.
            #     # "open": False,       # create new connections on initialization -- ok you cannot set this to false here
            #     "max_waiting": 0,    # number of waiting operations, set to unlimited.
            #     # "max_lifetime": X, # seconds allowed for age of a connection, default one hour.
            #     "max_idle": 60,      # seconds until inactive connection close, default is 10 _minutes_.
            #     "reconnect_timeout": 5,  # seconds to wait before retrying a connection. default is 5 _minutes_
            #     # "num_workers": 3,  # number of cleanup worker threads to use, default is 3.
            # },
            'client_encoding': 'UTF-8',
        },
        "ATOMIC_REQUESTS": True,  # default is True, just being explicit
        'TEST': {
            'MIGRATE': True,
        }
    },
}

DEFAULT_AUTO_FIELD = "django.db.models.AutoField"  # database primary key setting

# mac os homebrew postgres has configuration complexities that are not worth the effort to resolve.
if (not SECURE_SSL_REDIRECT and platform.system() == "Darwin") or os.environ.get("RUNNING_IN_DOCKER", False):
    DATABASES['default']['OPTIONS']['sslmode'] = 'disable'  # type: ignore

# This is from testing postgres connection pools, which caused too many issues.
# if DEBUG:
#     DATABASES['default']['OPTIONS']['pool']['min_size'] = 1  # single connection pool

####################################################################################################
##################################### Shell Plus Config ############################################
####################################################################################################

SHELL_PLUS = "ipython"

SHELL_PLUS_POST_IMPORTS = [
    # generic
    "json",
    "orjson",
    ["collections", ("Counter", "defaultdict")],
    ["pprint", ("pprint", "pp", "pformat")],
    
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
    
    # honestly misc stuff
    ['libs.utils.http_utils', "numformat"],
    ["libs.efficient_paginator", "EfficientQueryPaginator"],
    
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
    ["database.models", "dbt"],
    
    # really useful constants
    ["constants.user_constants", ("ANDROID_API", "IOS_API", "NULL_OS", "ResearcherRole")],
    ["constants.data_stream_constants", ("ALL_DATA_STREAMS", )],
]
SHELL_PLUS_PRE_IMPORTS = []

# if we enable sql printing, don't truncate the output.
RUNSERVER_PLUS_PRINT_SQL_TRUNCATE = None
SHELL_PLUS_PRINT_SQL_TRUNCATE = None
# SHELL_PLUS_PRINT_SQL = True

####################################################################################################
######################################## Testing Settings ##########################################
####################################################################################################

# TEST_RUNNER = 'django.test.runner.DiscoverRunner'  # default test runner
TEST_RUNNER = "redgreenunittest.django.runner.RedGreenDiscoverRunner"  # colored output!

####################################################################################################
#################################### Django Core Settings ##########################################
####################################################################################################

if DEBUG:
    ALLOWED_HOSTS = ("*",)  # when running in development allow any domain name
else:
    ALLOWED_HOSTS = [DOMAIN_NAME]  # when a server only allow the real domain name

PROJECT_ROOT = "."

TIME_ZONE = 'UTC'
USE_TZ = True

INSTALLED_APPS = [
    'database.apps.DatabaseConfig',
    'django.contrib.sessions',
    'django_extensions',
    'django.contrib.staticfiles'
    # 'static_files',
]

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
    'middleware.minified_css_bug_middleware.MinifiedCSSMiddleware',
    # "middleware.request_to_curl.CurlMiddleware",  # uncomment to enable a debugging tool
]

####################################################################################################
################################### Django Static Files ############################################
####################################################################################################

STATIC_ROOT = "staticfiles"
STATIC_URL = "/static/"
STATICFILES_DIRS = ["frontend/static/"]

####################################################################################################
################################ Django Template Configuration #####################################
####################################################################################################

TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.jinja2.Jinja2',
        'APP_DIRS': False,
        'DIRS': [
            "frontend/templates",
            "frontend/static/javascript",
            "frontend/static/css",
            "frontend/static",
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

####################################################################################################
###################################### URL Routing #################################################
####################################################################################################

# Changing this causes a runtime warning, but has no effect. Enabling this feature is not equivalent
# to the feature in urls.py.  Leave False.
APPEND_SLASH = False

ROOT_URLCONF = "urls"

####################################################################################################
############################################# Sentry ###############################################
####################################################################################################

# We encounter this starlette integration bug _at least_ when running tasks in celery.
#   https://github.com/getsentry/sentry-python/issues/1603
# No fixes work, but purging the it from _AUTO_ENABLING_INTEGRATIONS in the integrations code does.
# (This is ... bad, but without it file processing errors in a weird/unpredictable way. (Possibly
# after the first page of data? It's not clear.)
if "sentry_sdk.integrations.starlette.StarletteIntegration" not in _AUTO_ENABLING_INTEGRATIONS:
    raise ImproperlyConfigured(
        "We have a bug where the starlette integration is getting auto enabling and then raising "
        "an error. There is no good option here, but this message is better than the next line"
        "failing. Sorry future person!"
    )
_AUTO_ENABLING_INTEGRATIONS.remove("sentry_sdk.integrations.starlette.StarletteIntegration")


# def filter_junk_errors(event: Event, hint: Hint) -> Event | None:
#     """ Docs: https://docs.sentry.io/platforms/python/configuration/filtering/ """

#     if 'exc_info' not in hint:  # we only care about errors
#         from pprint import pprint, pp, pformat
#         pprint(hint)
#         return event

#     exception: Exception = hint['exc_info'][1]  # unfathomable but docs say this is what you do.

#     # this never prints
#     #
#     print("exception name!")
#     print(exception)
#     print("exception name!")

#     if "was sent code 134!" in str(exception):  # when gunicorn kills a slow worker thread on deploy
#         return None

#     return event

warnings.filterwarnings("ignore", category=DeprecationWarning, module="sentry_sdk.client")
# TODO: get error filtering working, the above never prints
sentry_sdk.init(
    dsn=normalize_sentry_dsn(SENTRY_ELASTIC_BEANSTALK_DSN),  # type: ignore - this can take a None
    enable_tracing=False,
    ignore_errors=["WorkerLostError", "DisallowedHost"],
    # auto_enabling_integrations=False,  # this was one of the fixes for the starlette bug that didn't work.
    # before_send=filter_junk_errors,
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
            exclude_beat_tasks=True,  # type: ignore - this value seems to work fine
        )
    ],
)
####################################################################################################
######################################### Django Logging ###########################################
####################################################################################################


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


####################################################################################################
############################### Ensure Assertions Are Enabled ######################################
####################################################################################################

def assert_assertions_not_disabled():
    """ Keep that success variable out of the namespace """
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

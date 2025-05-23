import time
from datetime import date, datetime, timedelta
from posixpath import abspath
from sys import argv as _argv

from dateutil.tz import gettz

from config.settings import DEVELOPER_TIMEZONE


#
### ITEMS DECLARED IN THIS FILE WILL BE IMPORTED INTO THE TERMINAL SHELL. 
#


#
## Date and Time canstants - just keep them here, we had a DT constants file but meh.
#

# timezones
THE_ONE_TRUE_TIMEZONE = DEV_TIMEZONE = DEVELOPER_TIMEZONE = gettz(DEVELOPER_TIMEZONE) # hilarious.
EASTERN = EDT = EST = gettz("America/New_York")
UTC = gettz("UTC")

API_TIME_FORMAT = "%Y-%m-%dT%H:%M:%S"  # isoformat without timezone, YYYY-MM-DDThh:mm:ss
API_TIME_FORMAT_WITH_TZ = "%Y-%m-%dT%H:%M:%S (%Z)"
API_DATE_FORMAT = "%Y-%m-%d"

DEV_TIME_FORMAT = "%Y-%m-%d %H:%M (%Z)"
DEV_TIME_FORMAT3 = "%Y-%m-%d %H:%M:%S . %f (%Z)"  # leave the extra spaces
DEV_TIME_FORMAT4 = LEGIBLE_DT_FORMAT = "%Y-%m-%d %H:%M:%S (%Z)"

T_24HR_W_TZ_N_SEC_N_PAREN = "%H:%M %Z"
T_24HR_W_TZ_W_SEC_N_PAREN = "%H:%M:%S %Z"

DT_12HR_N_TZ_N_SEC_N_PAREN = "%Y-%m-%d %-I:%M %p"
DT_24HR_N_TZ_N_SEC_N_PAREN = "%Y-%m-%d %H:%M"

DT_12HR_N_TZ_N_SEC_W_PAREN = "%Y-%m-%d %-I:%M%p (%Z)"
DT_24HR_N_TZ_N_SEC_W_PAREN = "%Y-%m-%d %H:%M %Z"

DT_24HR_N_TZ_W_SEC_N_PAREN = "%Y-%m-%d %H:%M:%S %Z"

DT_12HR_W_TZ_W_SEC_N_PAREN = "%Y-%m-%d %-I:%M:%S%p %Z"
DT_24HR_W_TZ_W_SEC_N_PAREN = "%Y-%m-%d %H:%M:%S %Z"
DT_24HR_W_TZ_W_SEC_N_PAREN_WITH_LINE_BREAK = "%Y-%m-%d<br>%H:%M:%S %Z"
DT_12HR_W_TZ_W_SEC_W_PAREN = "%Y-%m-%d %-I:%M:%S%p (%Z)"
DT_24HR_W_TZ_W_SEC_W_PAREN = "%Y-%m-%d %H:%M:%S (%Z)"

DT_WDS_12HR_N_TZ_N_SEC_N_PAREN = "%a %Y-%m-%d %-I:%M %p"
DT_WDS_12HR_W_TZ_N_SEC_N_PAREN = "%a %Y-%m-%d %-I:%M:%S %Z"

# full date requires a comma
DT_WDS_12HR_W_TZ_N_SEC_W_PAREN = "%a %b %-d, %Y, %-I:%M%p (%Z)"
DT_WDL_12HR_W_TZ_N_SEC_W_PAREN = "%A %b %-d, %Y, %-I:%M%p (%Z)"
DT_WDL_12HR_W_TZ_W_SEC_W_PAREN = "%A %b %-d, %Y, %-I:%M:%S%p (%Z)"


#
## Common file paths
#

BEIWE_PROJECT_ROOT = abspath(__file__.rsplit("/", 2)[0] + "/")  # this does not end in a slash...
PROJECT_PARENT_FOLDER = BEIWE_PROJECT_ROOT.rsplit("/", 2)[0] + "/"


## All s3 file paths must be declared here so that we know where they are for participant data purge.
CHUNKS_FOLDER = "CHUNKED_DATA"
PROBLEM_UPLOADS = "PROBLEM_UPLOADS"
# this folder contains logs that we save to s3 for debugging purposes
LOGS_FOLDER = "LOGS"

# file path for custom ondeploy script
CUSTOM_ONDEPLOY_PREFIX = "CUSTOM_ONDEPLOY_SCRIPT"
CUSTOM_ONDEPLOY_SCRIPT_EB = "CUSTOM_ONDEPLOY_SCRIPT/EB"
CUSTOM_ONDEPLOY_SCRIPT_PROCESSING = "CUSTOM_ONDEPLOY_SCRIPT/PROCESSING"

REQUIRED_PARTICIPANT_PURGE_AGE_MINUTES = 30


#
## Runtime info (potentially unique to individual deployments)
#

RUNNING_SHELL = any(key in _argv for key in ("shell_plus", "--ipython", "ipython"))
RUNNING_TESTS = "test" in _argv
RUNNING_RUNSERVER_CLI = "runserver" in _argv
RUNNING_TEST_OR_FROM_A_SHELL = RUNNING_SHELL or RUNNING_TESTS or RUNNING_RUNSERVER_CLI
RUNNING_TESTS = "test" in _argv

# roughly one month before the initial deploy of the first Beiwe instance.
EARLIEST_POSSIBLE_DATA_DATE = date(2014, 8, 1)
EARLIEST_POSSIBLE_DATA_DATETIME = datetime(year=2014, month=8, day=1, tzinfo=UTC)
# this is the integer output of time.mktime(EARLIEST_POSSIBLE_DATA_DATETIME.timetuple())
EARLIEST_POSSIBLE_DATA_TIMESTAMP = 1406851200

# in order to solve a nasty problem that is stalling all data processing in rare cases due to
# corrupted data, we need a sanity check inside data processing.  This value is updated at runtime,
# just in case 90 days somehow isn't enough.
LATEST_POSSIBLE_DATA_TIMESTAMP = int(time.mktime((datetime.utcnow() + timedelta(days=90)).timetuple()))

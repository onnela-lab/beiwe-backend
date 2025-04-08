import os
import sys

# hack that inserts the root of the project folder into the python path so we can import the codebase
sys.path.insert(0, os.path.abspath(__file__).rsplit('/', 2)[0])

from time import sleep

from django.utils import timezone

import database  # this loads the database, don't remove it.
from libs.sentry import get_sentry_client, SentryTypes
from libs.utils.http_utils import fancy_dt_format_with_tz_and_seconds


script = sys.argv[1]
t_start = fancy_dt_format_with_tz_and_seconds(timezone.now(), "UTC")
print("\nStarting script: ", script, "on", t_start, "\n\n")

try:
    with get_sentry_client(SentryTypes.script_runner):
        __import__(f"scripts.{script}")
finally:
    t_end = fancy_dt_format_with_tz_and_seconds(timezone.now(), "UTC")
    print("\n\nFinished script: ", script, "on", t_start, "\n\n")
    
    sleep(2) # we need to wait for the sentry client to flush before we exit.

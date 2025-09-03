import os
import sys
import traceback

# hack that inserts the root of the project folder into the python path so we can import the codebase
repo_root = os.path.abspath(__file__).rsplit('/', 2)[0]
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

from time import sleep

from django.utils import timezone

import database  # this loads the database, don't remove it.
from libs.sentry import SentryUtils
from libs.utils.http_utils import fancy_dt_format_with_tz_and_seconds


class TaskRunnerError(Exception): pass

def main():
    
    script = sys.argv[1]
    t_start = fancy_dt_format_with_tz_and_seconds(timezone.now(), "UTC")
    print("\nStarting script: ", script, "on", t_start, "\n\n")
    
    try:
        with SentryUtils.report_script_runner():
            try:
                a_module = __import__(f"{script}")
                if not hasattr(a_module, "main"):
                    raise TaskRunnerError(f"Module {script} does not have a main function.")
                
                a_module.main()
            except Exception:
                traceback.print_exc()
                raise
    finally:
        t_end = fancy_dt_format_with_tz_and_seconds(timezone.now(), "UTC")
        print("\n\nFinished script: ", script, "on", t_start, "\n\n")
        
        sleep(2) # we need to wait for the sentry client to flush before we exit.


if __name__ == "__main__":
    main()
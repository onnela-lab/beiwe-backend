# trunk-ignore-all(isort,black,ruff/E402)
# add the root of the project into the path to allow cd-ing into this folder and running the script.
from os.path import abspath
from sys import argv, path


path.insert(0, abspath(__file__).rsplit('/', 2)[0])  # let us import the project

from cronutils import run_tasks

from libs.sentry import SentryTypes
from services.celery_data_processing import create_file_processing_tasks
from services.celery_forest import create_forest_celery_tasks
from services.celery_push_notifications import (create_heartbeat_tasks,
    create_survey_push_notification_tasks)
from services.scripts_runner import (enqueue_daily_script_tasks, enqueue_hourly_scripts_tasks,
    enqueue_six_minute_scripts_tasks)


class CronException(Exception): pass
OVERTIME_MESSAGE = "The Cron Script Itself took more than 5 minutes"

FIVE_MINUTES = "five_minutes"
HOURLY = "hourly"
FOUR_HOURLY = "four_hourly"
DAILY = "daily"
WEEKLY = "weekly"
MONTHLY = "monthly"
VALID_ARGS = [FIVE_MINUTES, HOURLY, FOUR_HOURLY, DAILY, WEEKLY, MONTHLY]

TASKS = {
    FIVE_MINUTES:
        [
            # uploaded data processing:
            create_file_processing_tasks,
            # notifications:
            create_survey_push_notification_tasks,
            create_heartbeat_tasks,
            # forest:
            create_forest_celery_tasks,
            # scripts:
            enqueue_six_minute_scripts_tasks,
        ],
    HOURLY: [enqueue_hourly_scripts_tasks],
    FOUR_HOURLY: [],
    DAILY: [enqueue_daily_script_tasks],
    WEEKLY: [],
    MONTHLY: [],
}

KILL_TIMES = TIME_LIMITS = {
    FIVE_MINUTES: 50,              # we only enqueue celery tasks.  if this takes more than a minute something is wrong.
    HOURLY: 10*60*60*24*365,       # 10 years (never kill)
    FOUR_HOURLY: 10*60*60*24*365,  # 10 years (never kill)
    DAILY: 10*60*60*24*365,        # 10 years (never kill)
    WEEKLY: 10*60*60*24*365,       # 10 years (never kill)
}

if __name__ == "__main__":
    cron_type = "None"
    error_handler = SentryTypes.error_handler_data_processing(cron_script=cron_type)
    
    if len(argv) <= 1:
        # this is quite
        with SentryTypes.error_handler_data_processing(cron_script=cron_type) as error_handler:
            raise CronException("Not enough arguments to cron\n")
    
    elif (cron_type:=argv[1]) in VALID_ARGS:
        
        # Run tasks in some nice wrappers to tell us when things break.
        
        run_tasks_wrapped_in_a_warning_timer = SentryTypes.timer_warning_data_processing(
            message=OVERTIME_MESSAGE, seconds=5 * 60, cron_script=cron_type
        )(run_tasks)
        
        with SentryTypes.error_handler_data_processing(cron_script=cron_type) as error_handler:
            run_tasks_wrapped_in_a_warning_timer(
                TASKS[cron_type], TIME_LIMITS[cron_type], cron_type, KILL_TIMES[cron_type]
            )
        
        error_handler.raise_errors()
    
    else:
        with SentryTypes.error_handler_data_processing(cron_script=cron_type) as error_handler:
            raise CronException("Invalid argument to cron\n")
    
    error_handler.raise_errors()

# add the root of the project into the path to allow cd-ing into this folder and running the script.
from os.path import abspath
from sys import path

path.insert(0, abspath(__file__).rsplit('/', 2)[0])

# start actual cron-related code here
from sys import argv
from cronutils import run_tasks
from services.celery_data_processing import create_file_processing_tasks
from services.celery_push_notifications import create_push_notification_tasks
from services.celery_forest import create_forest_celery_tasks

FIVE_MINUTES = "five_minutes"
HOURLY = "hourly"
FOUR_HOURLY = "four_hourly"
DAILY = "daily"
WEEKLY = "weekly"
MONTHLY = "monthly"
VALID_ARGS = [FIVE_MINUTES, HOURLY, FOUR_HOURLY, DAILY, WEEKLY, MONTHLY]

TASKS = {
    FIVE_MINUTES: [create_file_processing_tasks, create_push_notification_tasks, create_forest_celery_tasks],
    HOURLY: [],
    FOUR_HOURLY: [],
    DAILY: [],
    WEEKLY: [],
    MONTHLY: [],
}

TIME_LIMITS = {
    FIVE_MINUTES: 50,              # we only enqueue celery tasks.  if this takes more than a minute something is wrong.
    HOURLY: 10*60*60*24*365,       # 10 years (never kill)
    FOUR_HOURLY: 10*60*60*24*365,  # 10 years (never kill)
    DAILY: 10*60*60*24*365,        # 10 years (never kill)
    WEEKLY: 10*60*60*24*365,       # 10 years (never kill)
}

KILL_TIMES = TIME_LIMITS

if __name__ == "__main__":
    if len(argv) <= 1:
        raise Exception("Not enough arguments to cron\n")
    elif argv[1] in VALID_ARGS:
        cron_type = argv[1]
        if cron_type in KILL_TIMES:
            run_tasks(TASKS[cron_type], TIME_LIMITS[cron_type], cron_type, KILL_TIMES[cron_type])
        else:
            run_tasks(TASKS[cron_type], TIME_LIMITS[cron_type], cron_type)
    else:
        raise Exception("Invalid argument to cron\n")


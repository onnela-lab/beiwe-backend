# add the root of the project into the path to allow cd-ing into this folder and running the script.
from os.path import abspath
from sys import path

path.insert(0, abspath(__file__).rsplit('/', 2)[0])

# start actual cron-related code here
from sys import argv
from cronutils import run_tasks
from libs.file_processing import process_file_chunks

FIVE_MINUTES = "five_minutes"
HOURLY = "hourly"
FOUR_HOURLY = "four_hourly"
DAILY = "daily"
WEEKLY = "weekly"
VALID_ARGS = [FIVE_MINUTES, HOURLY, FOUR_HOURLY, DAILY, WEEKLY]

TASKS = {
    FIVE_MINUTES: [process_file_chunks],
    HOURLY: [],
    FOUR_HOURLY: [],
    DAILY: [],
    WEEKLY: []
}

# we never want to kill or cap runtime of our cron jobs.
TIME_LIMITS = {
    FIVE_MINUTES: 1000*60*60*24*365,    # 1000 years (never kill)
    HOURLY: 1000*60*60*24*365,          # 1000 years (never kill)
    FOUR_HOURLY: 1000*60*60*24*365,     # 1000 years (never kill)
    DAILY: 1000*60*60*24*365,           # 1000 years (never kill)
    WEEKLY: 1000*60*60*24*365,          # 1000 years (never kill)
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

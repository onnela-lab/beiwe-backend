# add the root of the project into the path to allow cd-ing into this folder and running the script.
from sys import path
from os.path import abspath
path.insert(0, abspath(__file__).rsplit('/', 2)[0])

from psutil import process_iter

for python_process in process_iter():
    # find python processes, identify celery push notification processes, and kill any zombies.
    # filter out non python3 and irrelevant celery tasks, we only care about  push notifications.
    if "python3" != python_process.name().lower():
        continue

    # command is a list of all commandline parameters that were passed into the process.
    command = python_process.cmdline()
    if "celery" not in command or "services.celery_push_notifications" not in command:
        continue

    # we now have only have the celery processes for push notifications.
    # check that each one has a supervisord parent process.
    supervisord_parent = False
    supervisor_process = None
    for parent_process in python_process.parents():
        if parent_process.name().lower() == "supervisord":
            supervisord_parent = True
            supervisor_process = parent_process

    # If it doesn't have a supervisor process, or if it doesbut its not running, terminate it.
    if supervisord_parent == False or not supervisor_process.is_running():
        python_process.terminate()

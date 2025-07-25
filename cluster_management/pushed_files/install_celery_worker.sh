#!/bin/bash

# literally just run this script and celery will work.
# tails the celery log, ctrl-c to exit the tail

# There are weird failure modes when these files don't exist or exist with the wrong permissions.
rm -f /home/ubuntu/celery_processing.log*
touch /home/ubuntu/celery_processing.log
chmod 666 /home/ubuntu/celery_processing.log
chgrp ubuntu /home/ubuntu/celery_processing.log
chown ubuntu /home/ubuntu/celery_processing.log

rm -f /home/ubuntu/celery_push_send.log*
touch /home/ubuntu/celery_push_send.log
chmod 666 /home/ubuntu/celery_push_send.log
chgrp ubuntu /home/ubuntu/celery_push_send.log
chown ubuntu /home/ubuntu/celery_push_send.log

rm -f /home/ubuntu/celery_scripts.log*
touch /home/ubuntu/celery_scripts.log
chmod 666 /home/ubuntu/celery_scripts.log
chgrp ubuntu /home/ubuntu/celery_scripts.log
chown ubuntu /home/ubuntu/celery_scripts.log

rm -f /home/ubuntu/celery_forest.log*
touch /home/ubuntu/celery_forest.log
chmod 666 /home/ubuntu/celery_forest.log
chgrp ubuntu /home/ubuntu/celery_forest.log
chown ubuntu /home/ubuntu/celery_forest.log

# using this folder supervisord runs our code at boot - we can restart the server.
tee /etc/supervisor/conf.d/beiwe.conf > /dev/null <<EOL

[program:celery_processing]
# the queue and app names are declared in constants.py.
directory = /home/ubuntu/beiwe-backend/
command = /home/ubuntu/.pyenv/versions/beiwe/bin/python -m celery \
    -A services.celery_data_processing worker \
    -Q data_processing \
    --loglevel=info \
    -Ofair \
    --hostname=%%h_processing \
    --autoscale=10,2
stdout_logfile = /home/ubuntu/celery_processing.log
stderr_logfile = /home/ubuntu/celery_processing.log
autostart = true
logfile_maxbytes = 10MB
logfile_backups = 1
#stopwaitsecs = 30
stopasgroup = true
startsecs = 5
user=ubuntu
chown=ubuntu


[program:celery_forest]
# the queue and app names are declared in constants.py.
directory = /home/ubuntu/beiwe-backend/
command = /home/ubuntu/.pyenv/versions/beiwe/bin/python -m celery \
    -A services.celery_forest worker \
    -Q forest_queue \
    --loglevel=info \
    -Ofair \
    --hostname=%%h_forest \
    --concurrency=1
stdout_logfile = /home/ubuntu/celery_forest.log
stderr_logfile = /home/ubuntu/celery_forest.log
autostart = true
logfile_maxbytes = 10MB
logfile_backups = 1
#stopwaitsecs = 30
stopasgroup = true
startsecs = 5
user=ubuntu
chown=ubuntu


[program:script_tasks]
# the queue and app names are declared in constants.py.
directory = /home/ubuntu/beiwe-backend/
command = /home/ubuntu/.pyenv/versions/beiwe/bin/python -m celery \
    -A services.scripts_runner worker \
    -Q scripts_queue \
    --loglevel=info \
    -Ofair \
    --hostname=%%h_scripts \
    --autoscale=10,2
stdout_logfile = /home/ubuntu/celery_scripts.log
stderr_logfile = /home/ubuntu/celery_scripts.log
autostart = true
logfile_maxbytes = 10MB
logfile_backups = 1
#stopwaitsecs = 30
stopasgroup = true
user=ubuntu
chown=ubuntu


[program:celery_push_send]
# the queue and app names are declared in constants.py.
directory = /home/ubuntu/beiwe-backend/
command = /home/ubuntu/.pyenv/versions/beiwe/bin/python -m celery \
    -A services.celery_push_notifications worker \
    -Q push_notifications \
    --loglevel=info \
    -Ofair \
    --hostname=%%h_notifications \
    --concurrency=20 --pool=threads
stdout_logfile = /home/ubuntu/celery_push_send.log
stderr_logfile = /home/ubuntu/celery_push_send.log
autostart = true
logfile_maxbytes = 10MB
logfile_backups = 1
#stopwaitsecs = 30
stopasgroup = true
# startsecs = 5
user=ubuntu
chown=ubuntu


EOL

# start data processing
# supervisord

#echo "Use 'supervisord' or 'processing-start' to start the celery data processing service,"
#echo "use 'killall supervisord' or 'processing-stop' to stop it."
#echo "Note: you should not run supervisord as the superuser."

# uncomment when debugging:
#logc

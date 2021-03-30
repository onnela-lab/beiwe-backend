#!/bin/bash

#you may need to run this from inside the repository folder and/or change the directory setting under program:celery
# (looks like almost all logging happens in celery.err)

#literally just run this script and celery will work.
# tails the celery log, ctrl-c to exit the tail

# This looks like overkill, but resetting logs and permissions is super helpful
sudo mkdir -p /etc/supervisor/conf.d/

# Files need to have permissions 777 in order for the supervisor and celery
# processes to have full permissions to write regardless of the runtime user
# they occur under (supervisord may run as a root service, causing problems).
sudo rm -f /home/ubuntu/supervisord.log
sudo touch /home/ubuntu/supervisord.log
sudo chmod 777 /home/ubuntu/supervisord.log
sudo chgrp adm /home/ubuntu/supervisord.log

sudo rm -f /home/ubuntu/celery_processing.log
sudo touch /home/ubuntu/celery_processing.log
sudo chmod 777 /home/ubuntu/celery_processing.log
sudo chgrp adm /home/ubuntu/celery_processing.log

sudo rm -f /home/ubuntu/celery_push_send.log
sudo touch /home/ubuntu/celery_push_send.log
sudo chmod 777 /home/ubuntu/celery_push_send.log
sudo chgrp adm /home/ubuntu/celery_push_send.log

sudo tee /etc/supervisord.conf >/dev/null <<EOL
[supervisord]
logfile = /home/ubuntu/supervisord.log
logfile_maxbytes = 10MB
logfile_backups = 1
loglevel = info
pidfile = /tmp/supervisord.pid
nodaemon = false
minfds = 1024
minprocs = 200
umask = 022
identifier = supervisor
directory = /tmp
childlogdir = /tmp
strip_ansi = false

[inet_http_server]
port = 127.0.0.1:50001

[supervisorctl]
serverurl = http://127.0.0.1:50001

[program:celery_processing]
# the queue and app names are declared in constants.py.
directory = /home/ubuntu/beiwe-backend/
command = python3 -m celery -A services.celery_data_processing worker -Q data_processing --loglevel=info -Ofair --hostname=%%h_processing
stdout_logfile = /home/ubuntu/celery_processing.log
stderr_logfile = /home/ubuntu/celery_processing.log
autostart = true
logfile_maxbytes = 10MB
logfile_backups = 1
#stopwaitsecs = 30
stopasgroup = true
startsecs = 5

[program:celery_forest]
# the queue and app names are declared in constants.py.
directory = /home/ubuntu/beiwe-backend/
command = python3 -m celery -A services.celery_forest worker -Q forest_queue --loglevel=info -Ofair --hostname=%%h_forest
stdout_logfile = /home/ubuntu/celery_forest.log
stderr_logfile = /home/ubuntu/celery_forest.log
autostart = true
logfile_maxbytes = 10MB
logfile_backups = 1
#stopwaitsecs = 30
stopasgroup = true
startsecs = 5

[program:celery_push_send]
# the queue and app names are declared in constants.py.
directory = /home/ubuntu/beiwe-backend/
command = python3 -m celery -A services.celery_push_notifications worker -Q push_notifications --loglevel=info -Ofair --hostname=%%h_notifications --concurrency=20 --pool=threads
stdout_logfile = /home/ubuntu/celery_push_send.log
stderr_logfile = /home/ubuntu/celery_push_send.log
autostart = true
logfile_maxbytes = 10MB
logfile_backups = 1
#stopwaitsecs = 30
stopasgroup = true
# startsecs = 5
EOL

# start data processing
# supervisord

#echo "Use 'supervisord' or 'processing-start' to start the celery data processing service,"
#echo "use 'killall supervisord' or 'processing-stop' to stop it."
#echo "Note: you should not run supervisord as the superuser."

# uncomment when debugging:
#logc

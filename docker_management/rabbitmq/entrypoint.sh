#!/bin/sh

# Create Rabbitmq user
( rabbitmqctl wait --timeout 60 $RABBITMQ_PID_FILE ; \
rabbitmqctl add_user $RABBITMQ_USERNAME $RABBITMQ_PASSWORD 2>/dev/null ; \
rabbitmqctl set_permissions -p / $RABBITMQ_USERNAME  ".*" ".*" ".*" ; ) &

# $"$@" is used to pass arguments to the rabbitmq-server command.
# For example if you use it like this: docker run -d rabbitmq arg1 arg2,
# it will be as you run in the container rabbitmq-server arg1 arg2
rabbitmq-server "$@"
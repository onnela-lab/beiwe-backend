FROM rabbitmq:3-management

ARG RABBITMQ_USERNAME
ARG RABBITMQ_PASSWORD
ENV RABBITMQ_PID_FILE $RABBITMQ_MNESIA_DIR.pid

COPY ./cluster_management/pushed_files/rabbitmq_configuration.txt /etc/rabbitmq/rabbitmq-env.conf

COPY ./docker_config/rabbitmq/entrypoint.sh .
RUN sed -i 's/\r$//g' ./entrypoint.sh
RUN chmod +x ./entrypoint.sh

RUN ls

ENTRYPOINT ["./entrypoint.sh"]
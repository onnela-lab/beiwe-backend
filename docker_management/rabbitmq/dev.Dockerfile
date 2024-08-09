FROM rabbitmq:3-management-alpine

ARG RABBITMQ_USERNAME
ARG RABBITMQ_PASSWORD
ENV RABBITMQ_PID_FILE $RABBITMQ_MNESIA_DIR.pid

COPY ./cluster_management/pushed_files/rabbitmq_configuration.txt /etc/rabbitmq/rabbitmq-env.conf
COPY ./docker_management/rabbitmq/entrypoint.sh .

RUN sed -i 's/\r$//g' ./entrypoint.sh && \
    chmod +x ./entrypoint.sh

ENTRYPOINT ["./entrypoint.sh"]
FROM ubuntu:latest

ENV DEBIAN_FRONTEND=noninteractive
ARG RABBITMQ_PORT
ARG RABBITMQ_PASSWORD

RUN apt-get update && \
    apt-get install -y supervisor moreutils nload htop ack-grep silversearcher-ag libpq-dev zstd cron vim
RUN apt-get install -y build-essential curl gcc git libbz2-dev libffi-dev liblzma-dev libncurses5-dev \
    libncursesw5-dev libreadline-dev libsqlite3-dev libssl-dev make zlib1g-dev wget xz-utils zlib1g-dev

RUN mkdir -p /home/ubuntu/beiwe-backend
WORKDIR /home/ubuntu/beiwe-backend

COPY . .

RUN chmod +x ./cluster_management/pushed_files/install_celery_worker.sh
RUN echo "rabbitmq:${RABBITMQ_PORT}\n${RABBITMQ_PASSWORD}" > ./manager_ip

RUN touch server_log.log
RUN chmod 777 server_log.log

USER ubuntu

ARG pyenv="/home/ubuntu/.pyenv/bin/pyenv"
ARG pyenv_env_name="beiwe"
ARG python_version="3.8.19"
ENV python="/home/ubuntu/.pyenv/versions/$python_version/envs/beiwe/bin/python"
RUN curl https://pyenv.run | bash >> server_log.log
RUN $pyenv update >> server_log.log
RUN $pyenv install -v $python_version >> server_log.log
RUN $pyenv virtualenv $python_version $pyenv_env_name >> server_log.log
RUN $python -m pip install --upgrade pip setuptools wheel >> server_log.log
RUN $python -m pip install -r ./requirements.txt >> server_log.log
RUN $python -m pip install python-dotenv

USER root
RUN ./cluster_management/pushed_files/install_celery_worker.sh >> server_log.log
RUN cp /etc/supervisord.conf /etc/supervisor/supervisord.conf

COPY ./.envs/.env.prod .env

RUN crontab ./cluster_management/pushed_files/cron_manager.txt
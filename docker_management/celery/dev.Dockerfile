FROM ubuntu:latest

ARG RABBITMQ_PORT
ARG RABBITMQ_PASSWORD
ARG pyenv="/home/ubuntu/.pyenv/bin/pyenv"
ARG pyenv_env_name="beiwe"
ARG python_version="3.8.19"

ENV DEBIAN_FRONTEND=noninteractive
ENV python="/home/ubuntu/.pyenv/versions/$python_version/envs/beiwe/bin/python"

RUN apt-get update && \
    apt-get install -y cron curl gcc git make build-essential gdb lcov pkg-config \
      libbz2-dev libffi-dev libgdbm-dev libgdbm-compat-dev liblzma-dev \
      libncurses5-dev libreadline6-dev libsqlite3-dev libssl-dev supervisor \
      lzma lzma-dev tk-dev uuid-dev zlib1g-dev libpq-dev xz-utils zlib1g-dev && \
    mkdir -p /home/ubuntu/beiwe-backend \

WORKDIR /home/ubuntu/beiwe-backend

COPY . .

RUN chmod +x ./cluster_management/pushed_files/install_celery_worker.sh && \
    echo "rabbitmq:${RABBITMQ_PORT}\n${RABBITMQ_PASSWORD}" > ./manager_ip && \
    touch server_log.log && \
    chmod 777 server_log.log

USER ubuntu
RUN curl https://pyenv.run | bash >> server_log.log && \
    $pyenv update >> server_log.log && \
    $pyenv install -v $python_version >> server_log.log && \
    $pyenv virtualenv $python_version $pyenv_env_name >> server_log.log && \
    $python -m pip install --upgrade pip setuptools wheel >> server_log.log && \
    $python -m pip install -r ./requirements.txt >> server_log.log && \
    $python -m pip install python-dotenv

COPY ./.envs/.env.dev .env

USER root
RUN ./cluster_management/pushed_files/install_celery_worker.sh >> server_log.log && \
    cp /etc/supervisord.conf /etc/supervisor/supervisord.conf && \
    crontab ./cluster_management/pushed_files/cron_manager.txt
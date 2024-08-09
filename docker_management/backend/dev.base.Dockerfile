FROM python:3.8.19-slim AS beiwe-server-dev-base

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /usr/src/app

COPY ../../requirements.txt .
RUN apt-get update && \
    apt-get install -y git gcc && \
    pip install --upgrade pip && \
    pip wheel --no-cache-dir --no-deps --wheel-dir /usr/src/app/wheels -r requirements.txt
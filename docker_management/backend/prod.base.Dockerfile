FROM python:3.11.10-slim AS beiwe-server-prod-base

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /usr/src/app

COPY ../../requirements.txt .
RUN apt-get update && \
    apt-get install -y git gcc && \
    pip install --upgrade pip && \
    pip wheel --no-cache-dir --no-deps --wheel-dir /usr/src/app/wheels -r requirements.txt
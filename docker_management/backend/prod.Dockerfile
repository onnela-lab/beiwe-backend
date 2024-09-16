FROM python:3.11.10

ENV APP_HOME=/home/app/web

RUN mkdir -p $APP_HOME && \
    addgroup --system app && adduser --system --group app

WORKDIR $APP_HOME

COPY --from=beiwe-server-prod-base /usr/src/app/wheels /wheels
COPY --from=beiwe-server-prod-base /usr/src/app/requirements.txt .

COPY . $APP_HOME

RUN pip install --upgrade pip && \
    pip install --no-cache /wheels/* && \
    chown -R app:app $APP_HOME

USER app
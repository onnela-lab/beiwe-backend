FROM python:3.8.19

RUN mkdir -p /home/app

RUN addgroup --system app && adduser --system --group app

ENV HOME=/home/app
ENV APP_HOME=/home/app/web
RUN mkdir $APP_HOME
WORKDIR $APP_HOME

COPY --from=beiwe-server-dev-base /usr/src/app/wheels /wheels
COPY --from=beiwe-server-dev-base /usr/src/app/requirements.txt .
RUN pip install --upgrade pip
RUN pip install --no-cache /wheels/*

COPY . $APP_HOME

RUN chown -R app:app $APP_HOME

USER app
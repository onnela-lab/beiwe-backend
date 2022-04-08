FROM python:3.7

RUN apt-get update && apt-get install -y postgresql libpq-dev && \
    pip install --upgrade pip setuptools wheel

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /code

COPY . requirements.txt app.py manage.py wsgi.py /code/

RUN pip install -r requirements.txt

CMD python manage.py migrate && python app.py
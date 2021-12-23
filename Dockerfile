FROM python:3.8

WORKDIR /app

COPY . /app

RUN pip install -r requirements.txt

EXPOSE 9088
CMD gunicorn lot_aggregation_django.wsgi:application --bind 0.0.0.0:9088

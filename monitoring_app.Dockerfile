FROM python:3.12

COPY . /app/sc_wgs_monitoring

RUN ["pip3", "install", "-r", "/app/sc_wgs_monitoring/requirements.txt"]
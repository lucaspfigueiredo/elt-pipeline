FROM apache/airflow:2.2.4-python3.8

LABEL maintainer="Lucas Figueiredo"

USER root:root

RUN apt-get update && \
    apt-get install -y openjdk-11-jre-headless && \
    apt-get clean

USER airflow

RUN pip install --upgrade pip

COPY requirements.txt /opt/airflow

WORKDIR /opt/airflow

RUN pip install -r requirements.txt
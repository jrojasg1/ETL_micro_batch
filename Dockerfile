FROM apache/airflow:latest

USER root
RUN apt-get update && \
    apt-get install -y gcc python3-dev openjdk-17-jdk-headless && \
    apt-get clean

# Set JAVA_HOME environment variable
ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-arm64

USER airflow

# Copiar el archivo requirements.txt dentro del contenedor
COPY requirements.txt /tmp/requirements.txt

RUN pip install --upgrade pip

USER airflow

# Install the Docker provider for Airflow
RUN pip install apache-airflow apache-airflow-providers-apache-spark psycopg2-binary

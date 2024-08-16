FROM apache/airflow:latest-python3.11

# Install JDK
USER root
RUN apt-get update
RUN apt install -y default-jdk
RUN apt-get autoremove -yqq --purge
RUN apt-get install figlet
RUN apt-get clean
RUN rm -rf /var/lib/apt/lists/*
USER airflow
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
RUN export JAVA_HOME

RUN pip install apache-airflow apache-airflow-providers-apache-spark pyspark

FROM apache/airflow:2.7.2-python3.8

USER root

# Instaliraj samo JDK i nano
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk nano && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64/
ENV PATH="$PATH:$JAVA_HOME/bin"

# Pređi nazad na airflow user pre pip install
USER airflow

COPY requirements.txt .

RUN pip install --default-timeout=1000 -r requirements.txt

USER root
RUN mkdir /shared && chown -R airflow /shared

USER airflow
WORKDIR /opt/airflow

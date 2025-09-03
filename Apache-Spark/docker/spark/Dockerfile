FROM python:3.8-slim-bullseye

USER root

RUN apt-get update && apt-get install -y openjdk-11-jdk curl tar bash procps \
    && rm -rf /var/lib/apt/lists/*

ENV SPARK_VERSION=3.0.1
ENV HADOOP_VERSION=3.2


RUN curl -L https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    | tar -xz -C /opt/ && \
    ln -s /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark

ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

RUN pip install --no-cache-dir --upgrade pip setuptools wheel && \
    pip install --no-cache-dir "numpy<2.0" pyspark==3.0.1 && \
    pip install --no-cache-dir h3 pandas==1.1.5 pyarrow==1.0.1 && \
    pip install --no-cache-dir psycopg2-binary==2.9.9


WORKDIR /opt/spark

EXPOSE 7077 8080 8081

#!/bin/bash

# Set up connections
echo ">>> Setting up Airflow connections"

airflow connections add 'AIRFLOW_DB_CONNECTION' \
    --conn-json '{
        "conn_type": "postgres",
        "login": "airflow",
        "password": "airflow",
        "host": "postgres",
        "port": 5432,
        "schema": "airflow"
    }'

airflow connections add 'HIVE_CONNECTION' \
    --conn-json '{
        "conn_type": "hive_cli",
        "host": "hive-server",
        "port": 10000,
        "schema": "default"
    }'

airflow connections add 'SPARK_CONNECTION' \
    --conn-json '{
        "conn_type": "spark",
        "host": "spark://spark-master",
        "port": 7077
    }'

airflow connections add 'LOCAL_FS_FILES' \
    --conn-json '{
        "conn_type": "fs",
        "extra": "{ \"path\": \"/opt/airflow/files\"}"
    }'

# Citus
airflow connections add CITUS_DEFAULT \
  --conn-uri postgresql://admin:admin@citus:5432/weather_bi


# Set up variables
# echo ">> Setting up airflow variables"
# airflow variables set SQL_CHUNK_SIZE 20
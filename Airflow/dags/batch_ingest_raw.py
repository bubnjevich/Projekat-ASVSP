from datetime import datetime

from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.ssh.hooks.ssh import SSHHook

from airflow.decorators import dag


@dag(
    dag_id="batch_ingest_raw",
    description="Ingest CSV in RAW zone on HDFS",
    start_date=datetime(2024, 11, 5),
    schedule_interval=None,
    max_active_runs=1,
    catchup=False,
)
def batch_ingest_raw():
    load_data = SSHOperator(
        task_id="load_data",
        ssh_conn_id="name_node_ssh_conn",
        command="""
            /opt/hadoop-3.2.1/bin/hdfs dfs -mkdir -p /data/weather/raw/{{ ds }} && \
            /opt/hadoop-3.2.1/bin/hdfs dfs -copyFromLocal -f /opt/airflow/files/WeatherEvents.csv /data/weather/raw/{{ ds }}/WeatherEvents.csv && \
            /opt/hadoop-3.2.1/bin/hdfs dfs -ls /data/weather/raw/{{ ds }}
        """,
        cmd_timeout=600,
        ssh_hook=SSHHook(
            remote_host="namenode", username="root", password="asvsp", banner_timeout=10
        ),
    )

    load_data


batch_ingest_raw()

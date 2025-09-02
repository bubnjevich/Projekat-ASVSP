from datetime import datetime
from airflow.decorators import dag, task
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.apache.hive.hooks.hive import HiveServer2Hook
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.ssh.hooks.ssh import SSHHook

@dag(
    dag_id="batch_transform",
    description="Transform RAW CSV -> TRANSFORMATION Parquet (typed, cleaned, enriched)",
    start_date=datetime(2025, 8, 25),
    schedule_interval=None,
    max_active_runs=1,
    catchup=False,
)
def batch_transform():
    spark_transform = SparkSubmitOperator(
    task_id="spark_transform",
    application="/opt/airflow/dags/jobs/batch/transform_weather.py",
    conn_id="SPARK_CONNECTION",
    application_args=[
        "--run-date", "{{ ds }}",
        "--raw-path", "hdfs://namenode:9000/data/weather/raw/2025-09-01/WeatherEvents.csv",
        "--out-base", "hdfs://namenode:9000/data/weather/transform/batch",
    ])

    @task(task_id="hive_create_events_clean")
    def hive_create_events_clean():
        sql = """
            CREATE EXTERNAL TABLE IF NOT EXISTS events_clean (
                event_id            string,
                type                string,
                severity            string,
                start_time_utc      timestamp,
                end_time_utc        timestamp,
                duration_min        double,
                timezone            string,
                airport_code        string,
                state               string,
                county              string,
                city                string,
                zip_code            string,
                location_lat        double,
                location_lng        double,
                precipitation_in    double,
                event_date          date,
                event_day           int,
                hour_of_day         int,
                dow                 int,
                season              string,
                is_night            boolean,
                local_start_ts      timestamp,
                local_end_ts        timestamp
            )
            PARTITIONED BY (event_year int, event_month int)
            STORED AS PARQUET
            LOCATION 'hdfs://namenode:9000/data/weather/transform/batch/events_clean'
            """
        hook = HiveServer2Hook(hiveserver2_conn_id="HIVE_CONNECTION")
        hook.run(sql)

    @task(task_id="hive_msck_repair")
    def hive_msck_repair():
        hook = HiveServer2Hook(hiveserver2_conn_id="HIVE_CONNECTION")
        hook.run("MSCK REPAIR TABLE events_clean")

    spark_transform >> hive_create_events_clean() >> hive_msck_repair()

batch_transform()

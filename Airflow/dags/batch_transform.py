from datetime import datetime
from airflow.decorators import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

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
    application="/opt/airflow/dags/jobs/transform_weather.py",
    conn_id="SPARK_CONNECTION",
    application_args=[
        "--run-date", "{{ ds }}",
        "--raw-path", "hdfs://namenode:9000/data/weather/raw/{{ ds }}/WeatherEvents.csv",
        "--out-base", "hdfs://namenode:9000/data/weather/transform",
    ])


    spark_transform

batch_transform()

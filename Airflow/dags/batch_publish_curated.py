# dags/batch_publish_curated.py
from datetime import datetime
from airflow.decorators import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from ddl_commands import *

CITUS_CONN_ID = "CITUS_DEFAULT"
POSTGRES_JDBC_JAR = "/shared/postgresql-42.7.3.jar"
SPARK_CONN_ID = "SPARK_CONNECTION"

@dag(
    dag_id="batch_publish_curated",
    description="Publish curated metric exposure_hours_daily to Citus (distributed)",
    start_date=datetime(2025, 8, 25),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["publish", "citus"],
)
def batch_publish_curated():

    citus_prepare_exposure = PostgresOperator(
        task_id="citus_prepare_exposure_hours",
        postgres_conn_id=CITUS_CONN_ID,
        sql=exposure_sql,
    )

    publish_exposure_hours = SparkSubmitOperator(
        task_id="publish_exposure_hours",
        application="/opt/airflow/dags/jobs/publish_exposure_hours.py",
        name="publish_exposure_hours",
        conn_id=SPARK_CONN_ID,
        application_args=[
            "--jdbc-url", "jdbc:postgresql://citus:5432/weather_bi",
            "--dbtable", "curated.exposure_hours_daily",
            "--dbuser", "admin",
            "--dbpassword", "admin",
            "--input-path", "hdfs://namenode:9000/data/weather/transform/events_clean",
        ],
        jars=POSTGRES_JDBC_JAR,
        verbose=False
    )

    citus_prepare_longest = PostgresOperator(
        task_id="citus_prepare_longest_episodes",
        postgres_conn_id=CITUS_CONN_ID,
        sql=longest_sql,
    )

    publish_longest_episodes = SparkSubmitOperator(
        task_id="publish_longest_episodes",
        application="/opt/airflow/dags/jobs/publish_longest_episodes.py",
        name="publish_longest_episodes",
        conn_id=SPARK_CONN_ID,
        application_args=[
            "--jdbc-url", "jdbc:postgresql://citus:5432/weather_bi",
            "--dbtable", "curated.longest_episodes",
            "--dbuser", "admin",
            "--dbpassword", "admin",
            "--jdbc-mode", "overwrite"
        ],
        jars=POSTGRES_JDBC_JAR,
        verbose=False
    )

    citus_prepare_cooccurrence  = PostgresOperator(
        task_id="citus_prepare_cooccurrence_minutes_daily",
        postgres_conn_id=CITUS_CONN_ID,
        sql = cooccurrence_sql
    )

    publish_cooccurrence = SparkSubmitOperator(
        task_id="publish_cooccurrence_minutes_daily",
        application="/opt/airflow/dags/jobs/publish_cooccurrence_minutes_daily.py",
        name="publish_cooccurrence_minutes_daily",
        conn_id=SPARK_CONN_ID,
        application_args=[
            "--jdbc-url", "jdbc:postgresql://citus:5432/weather_bi",
            "--dbtable", "curated.cooccurrence_minutes_daily",
            "--dbuser", "admin",
            "--dbpassword", "admin",
            "--jdbc-mode", "overwrite"
        ],
        jars=POSTGRES_JDBC_JAR,
        verbose=False
    )

    citus_prepare_3h_peak  = PostgresOperator(
        task_id="citus_prepare_3h_peak_hours_daily",
        postgres_conn_id=CITUS_CONN_ID,
        sql = peak3h_sql
    )

    peak_3h_hours_daily = SparkSubmitOperator(
        task_id="publish_3h_peak_hours_daily",
        application="/opt/airflow/dags/jobs/publish_peak_three_h_daily.py",
        name="publish_3h_peak_hours_daily",
        conn_id=SPARK_CONN_ID,
        application_args=[
            "--jdbc-url", "jdbc:postgresql://citus:5432/weather_bi",
            "--dbtable", "curated.peak_exposure_3h_daily",
            "--dbuser", "admin",
            "--dbpassword", "admin",
            "--jdbc-mode", "overwrite"
        ],
        jars=POSTGRES_JDBC_JAR,
        verbose=False
    )


    #citus_prepare_exposure >> publish_exposure_hours
    #citus_prepare_longest >> publish_longest_episodes
    #citus_prepare_cooccurrence >> publish_cooccurrence
    citus_prepare_3h_peak >> peak_3h_hours_daily

dag = batch_publish_curated()

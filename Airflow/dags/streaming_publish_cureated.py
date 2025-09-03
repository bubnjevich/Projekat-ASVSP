from airflow.decorators import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime
from ddl_commands import *
from airflow.providers.postgres.operators.postgres import PostgresOperator

# konstante
SPARK_CONN_ID = "SPARK_CONNECTION"
POSTGRES_JDBC_JAR = "/shared/postgresql-42.7.3.jar"
CITUS_CONN_ID = "CITUS_DEFAULT"
@dag(
    dag_id="weather_streaming_dag",
    description="Join batch + stream weather data, windowing analytics, publish to Citus (gold zone)",
    start_date=datetime(2025, 9, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["streaming", "weather", "gold", "citus"],
)
def weather_streaming_dag():

    # citus_prepare_exposure = PostgresOperator(
    #     task_id="citus_prepare_weather_realtime",
    #     postgres_conn_id=CITUS_CONN_ID,
    #     sql=weather_events_realtime_sql
    # )
    #
    # weather_streaming_job = SparkSubmitOperator(
    #     task_id="weather_streaming_job",
    #     application="/opt/airflow/dags/jobs/streaming/weather_streaming.py",
    #     name="weather_streaming_job",
    #     conn_id=SPARK_CONN_ID,
    #     application_args=[
    #         "--jdbc-url", "jdbc:postgresql://citus:5432/weather_bi",
    #         "--dbtable", "curated.weather_events_realtime",
    #         "--dbuser", "admin",
    #         "--dbpassword", "admin",
    #         "--batch-path", "hdfs://namenode:9000/data/weather/transform/batch/events_clean",
    #     ],
    #     jars=POSTGRES_JDBC_JAR,
    #     packages="org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1",
    #     verbose=False
    # )

    # citus_prepare_weather_streaming2 = PostgresOperator(
    #     task_id="citus_prepare_weather_realtime_2",
    #     postgres_conn_id=CITUS_CONN_ID,
    #     sql=weather_realtime_sliding_sql
    # )
    #
    # weather_streaming_job2 = SparkSubmitOperator(
    #     task_id="weather_streaming_job",
    #     application="/opt/airflow/dags/jobs/streaming/weather_streaming_v2.py",
    #     name="weather_streaming_job_2",
    #     conn_id=SPARK_CONN_ID,
    #     application_args=[
    #         "--jdbc-url", "jdbc:postgresql://citus:5432/weather_bi",
    #         "--dbtable", "curated.weather_realtime_sliding",
    #         "--dbuser", "admin",
    #         "--dbpassword", "admin",
    #         "--batch-path", "hdfs://namenode:9000/data/weather/transform/batch/events_clean",
    #     ],
    #     jars=POSTGRES_JDBC_JAR,
    #     packages="org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1",
    #     verbose=False
    # )

    citus_prepare_weather_state_analytics = PostgresOperator(
        task_id="weather_state_analytics",
        postgres_conn_id=CITUS_CONN_ID,
        sql=weather_state_analytics_sql
    )

    weather_state_analytics_job = SparkSubmitOperator(
        task_id="weather_streaming_job",
        application="/opt/airflow/dags/jobs/streaming/weather_state_analytics.py",
        name="weather_state_analytics",
        conn_id=SPARK_CONN_ID,
        application_args=[
            "--jdbc-url", "jdbc:postgresql://citus:5432/weather_bi",
            "--dbtable", "curated.weather_state_analytics",
            "--dbuser", "admin",
            "--dbpassword", "admin",
            "--batch-path", "hdfs://namenode:9000/data/weather/transform/batch/events_clean",
        ],
        jars=POSTGRES_JDBC_JAR,
        packages="org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1",
        verbose=False
    )

    #citus_prepare_weather_streaming2 >> weather_streaming_job2
    citus_prepare_weather_state_analytics >> weather_state_analytics_job

dag = weather_streaming_dag()

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
        application="/opt/airflow/dags/jobs/batch/publish_exposure_hours.py",
        name="publish_exposure_hours",
        conn_id=SPARK_CONN_ID,
        application_args=[
            "--jdbc-url", "jdbc:postgresql://citus:5432/weather_bi",
            "--dbtable", "curated.exposure_hours_daily",
            "--dbuser", "admin",
            "--dbpassword", "admin",
            "--input-path", "hdfs://namenode:9000/data/weather/transform/batch/events_clean",
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
        application="/opt/airflow/dags/jobs/batch/publish_longest_episodes.py",
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
        application="/opt/airflow/dags/jobs/batch/publish_cooccurrence_minutes_daily.py",
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
        application="/opt/airflow/dags/jobs/batch/publish_peak_three_h_daily.py",
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

    citus_prepare_monthly_risk_trend_by_type  = PostgresOperator(
        task_id="citus_prepare_monthly_risk_trend_by_type",
        postgres_conn_id=CITUS_CONN_ID,
        sql = monthly_trend_by_type_sql
    )

    monthly_risk_trend_by_type = SparkSubmitOperator(
        task_id="monthly_risk_trend_by_type",
        application="/opt/airflow/dags/jobs/batch/publish_monthly_risk_trend_by_type.py",
        name="monthly_risk_trend_by_type",
        conn_id=SPARK_CONN_ID,
        application_args=[
            "--jdbc-url", "jdbc:postgresql://citus:5432/weather_bi",
            "--dbtable", "curated.monthly_risk_trend_by_type",
            "--dbuser", "admin",
            "--dbpassword", "admin",
            "--jdbc-mode", "overwrite"
        ],
        jars=POSTGRES_JDBC_JAR,
        verbose=False
    )

    citus_prepare_winter_workload_index = PostgresOperator(
        task_id="citus_prepare_winter_workload_index",
        postgres_conn_id=CITUS_CONN_ID,
        sql=winter_workload_index_sql
    )

    winter_workload_index = SparkSubmitOperator(
        task_id="winter_workload_index",
        application="/opt/airflow/dags/jobs/batch/publish_winter_workload_index.py",
        name="winter_workload_index",
        conn_id=SPARK_CONN_ID,
        application_args=[
            "--jdbc-url", "jdbc:postgresql://citus:5432/weather_bi",
            "--dbtable", "curated.winter_workload_index",
            "--dbuser", "admin",
            "--dbpassword", "admin",
            "--jdbc-mode", "overwrite"
        ],
        jars=POSTGRES_JDBC_JAR,
        verbose=False
    )

    citus_prepare_risky_sequences = PostgresOperator(
        task_id="citus_prepare_risky_sequences",
        postgres_conn_id=CITUS_CONN_ID,
        sql=risky_sequences_sql
    )

    publish_risky_sequences = SparkSubmitOperator(
        task_id="publish_risky_sequences",
        application="/opt/airflow/dags/jobs/batch/publish_risky_sequences.py",
        name="publish_risky_sequences",
        conn_id=SPARK_CONN_ID,
        application_args=[
            "--jdbc-url", "jdbc:postgresql://citus:5432/weather_bi",
            "--dbtable", "curated.risky_sequences",
            "--dbuser", "admin",
            "--dbpassword", "admin",
            "--jdbc-mode", "overwrite"
        ],
        jars=POSTGRES_JDBC_JAR,
        verbose=False
    )

    citus_prepare_winter_burstiness = PostgresOperator(
        task_id="citus_prepare_winter_burstiness",
        postgres_conn_id=CITUS_CONN_ID,
        sql=winter_burstiness_sql
    )

    publish_winter_burstiness = SparkSubmitOperator(
        task_id="publish_winter_burstiness",
        application="/opt/airflow/dags/jobs/batch/publish_winter_burstiness.py",
        name="publish_winter_burstiness",
        conn_id=SPARK_CONN_ID,
        application_args=[
            "--jdbc-url", "jdbc:postgresql://citus:5432/weather_bi",
            "--dbtable", "curated.winter_burstiness",
            "--dbuser", "admin",
            "--dbpassword", "admin",
            "--jdbc-mode", "overwrite",
            "--gap-threshold-hours", "6",
            "--min-episode-hours", "12"
        ],
        jars=POSTGRES_JDBC_JAR,
        verbose=False
    )

    citus_prepare_night_thunderstorm_risk = PostgresOperator(
        task_id="citus_prepare_night_thunderstorm_risk",
        postgres_conn_id=CITUS_CONN_ID,
        sql=night_thunderstorm_risk_sql
    )

    publish_night_thunderstorm_risk = SparkSubmitOperator(
        task_id="publish_night_thunderstorm_risk",
        application="/opt/airflow/dags/jobs/batch/publish_night_thunderstorm_risk.py",
        name="publish_night_thunderstorm_risk",
        conn_id=SPARK_CONN_ID,
        application_args=[
            "--jdbc-url", "jdbc:postgresql://citus:5432/weather_bi",
            "--dbtable", "curated.night_thunderstorm_risk",
            "--dbuser", "admin",
            "--dbpassword", "admin",
            "--jdbc-mode", "overwrite"
        ],
        jars=POSTGRES_JDBC_JAR,
        verbose=False
    )

    citus_prepare_winter_fenology = PostgresOperator(
        task_id="citus_prepare_winter_fenology",
        postgres_conn_id=CITUS_CONN_ID,
        sql=winter_fenology_sql
    )

    publish_winter_fenology = SparkSubmitOperator(
        task_id="publish_winter_fenology",
        application="/opt/airflow/dags/jobs/batch/publish_winter_fenology.py",
        name="publish_winter_fenology",
        conn_id=SPARK_CONN_ID,
        application_args=[
            "--jdbc-url", "jdbc:postgresql://citus:5432/weather_bi",
            "--dbtable", "curated.winter_fenology",
            "--dbuser", "admin",
            "--dbpassword", "admin",
            "--jdbc-mode", "overwrite",
            "--winter-types", "SNOW,ICE,COLD"
        ],
        jars=POSTGRES_JDBC_JAR,
        verbose=False
    )


    citus_prepare_exposure >> publish_exposure_hours
    citus_prepare_longest >> publish_longest_episodes
    citus_prepare_cooccurrence >> publish_cooccurrence
    citus_prepare_3h_peak >> peak_3h_hours_daily
    citus_prepare_monthly_risk_trend_by_type >> monthly_risk_trend_by_type
    citus_prepare_winter_workload_index >> winter_workload_index
    citus_prepare_risky_sequences >> publish_risky_sequences
    citus_prepare_winter_burstiness >> publish_winter_burstiness
    citus_prepare_night_thunderstorm_risk >> publish_night_thunderstorm_risk
    citus_prepare_winter_fenology >> publish_winter_fenology


dag = batch_publish_curated()

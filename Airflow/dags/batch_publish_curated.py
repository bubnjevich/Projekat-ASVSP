# dags/batch_publish_curated.py
from datetime import datetime
from airflow.decorators import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

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
    exposure_sql = """
    CREATE SCHEMA IF NOT EXISTS curated;
    CREATE EXTENSION IF NOT EXISTS citus;

    CREATE TABLE IF NOT EXISTS curated.exposure_hours_daily (
        airport_code text NOT NULL,
        event_date date NOT NULL,
        exposure_hours_day double precision NOT NULL,
        exposure_hours_30d double precision,
        exposure_hours_90d double precision,
        PRIMARY KEY (airport_code, event_date)
    );

    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM pg_dist_partition
            WHERE logicalrelid = 'curated.exposure_hours_daily'::regclass
        ) THEN
            PERFORM create_distributed_table('curated.exposure_hours_daily', 'airport_code');
        END IF;
    END $$;
    """

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

    citus_prepare_exposure >> publish_exposure_hours


dag = batch_publish_curated()

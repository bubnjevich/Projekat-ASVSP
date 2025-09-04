import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, count,
    min as _min, max as _max, stddev, when, lit, avg
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, IntegerType
import psycopg2
from psycopg2.extras import execute_values


def quiet_logs(spark):
    jvm = spark.sparkContext._jvm
    logger = jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)


def build_spark(app_name="weather_streaming_job_instability"):
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def upsert_to_pg(df, epoch_id, args):
    if df.rdd.isEmpty():
        return

    rows = [tuple(r) for r in df.collect()]

    conn = psycopg2.connect(
        dbname="weather_bi",
        user=args.dbuser,
        password=args.dbpassword,
        host="citus",
        port=5432
    )
    cur = conn.cursor()

    insert_sql = f"""
    INSERT INTO {args.dbtable} (
        airport_code, state, county, event_window_start, event_window_end,
        event_count, min_dewpoint_spread, stddev_pressure_mb,
        max_gust_factor, stddev_cloud, instability_score
    )
    VALUES %s
    ON CONFLICT (airport_code, event_window_start) DO UPDATE SET
        state = EXCLUDED.state,
        county = EXCLUDED.county,
        event_window_end = EXCLUDED.event_window_end,
        event_count = EXCLUDED.event_count,
        min_dewpoint_spread = EXCLUDED.min_dewpoint_spread,
        stddev_pressure_mb = EXCLUDED.stddev_pressure_mb,
        max_gust_factor = EXCLUDED.max_gust_factor,
        stddev_cloud = EXCLUDED.stddev_cloud,
        instability_score = EXCLUDED.instability_score;
    """

    execute_values(cur, insert_sql, rows)
    conn.commit()
    cur.close()
    conn.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--jdbc-url", required=True)
    ap.add_argument("--dbtable", required=True, help="Gold tabela za instability KPI")
    ap.add_argument("--dbuser", required=True)
    ap.add_argument("--dbpassword", required=True)
    ap.add_argument("--jdbc-mode", default="append", choices=["overwrite", "append"])
    ap.add_argument("--batch-path", required=True)
    args = ap.parse_args()

    spark = build_spark()
    quiet_logs(spark)

    # ========= 1) Kafka stream =========
    raw_stream = (spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "broker1:9092")
        .option("subscribe", "weather_raw")
        .load())

    schema = StructType([
        StructField("airport_code", StringType()),
        StructField("state", StringType()),
        StructField("county", StringType()),
        StructField("timezone", StringType()),
        StructField("start_time_utc", TimestampType()),

        StructField("temp_c", DoubleType()),
        StructField("humidity", DoubleType()),
        StructField("cloud", DoubleType()),       # oblacnost %
        StructField("wind_kph", DoubleType()),    # prosečan vetar
        StructField("pressure_mb", DoubleType()), # pritisak u milibarima
        StructField("dewpoint_c", DoubleType()),  # tačka rose
        StructField("gust_kph", DoubleType())     # udar vetra
    ])

    stream_df = (raw_stream
        .selectExpr("CAST(value AS STRING) AS json_str")
        .select(from_json(col("json_str"), schema).alias("data"))
        .select("data.*"))

    # ========= 2) Batch (static) =========
    batch_df = spark.read.parquet(args.batch_path)
    cols_to_drop = [c for c in ["state", "county", "timezone", "start_time_utc"] if c in batch_df.columns]
    batch_df = batch_df.drop(*cols_to_drop) if cols_to_drop else batch_df

    # ========= 3) Join stream + batch =========
    joined = stream_df.join(batch_df, on="airport_code", how="left")

    # Feature engineering
    joined = joined.withColumn("dewpoint_spread", col("temp_c") - col("dewpoint_c"))
    joined = joined.withColumn("gust_factor", when(col("wind_kph") > 0, col("gust_kph") / col("wind_kph")).otherwise(lit(1.0)))

    # ========= 4) Sliding window + agregacije =========
    windowed = (joined
        .withWatermark("start_time_utc", "15 minutes")
        .groupBy(
            col("airport_code"),
            col("state"),
            col("county"),
            window(col("start_time_utc"), "2 hours", "30 minutes")
        )
        .agg(
            count("*").alias("event_count"),
            _min("dewpoint_spread").alias("min_dewpoint_spread"),
            stddev("pressure_mb").alias("stddev_pressure_mb"),
            _max("gust_factor").alias("max_gust_factor"),
            stddev("cloud").alias("stddev_cloud")
        )
    )

    # ========= 5) Izračunavanje instability score =========
    result = (windowed
        .withColumn(
            "instability_score",
            (1.0 / (col("min_dewpoint_spread") + lit(0.1))) +
            (col("stddev_pressure_mb") / lit(10.0)) +
            col("max_gust_factor") +
            (col("stddev_cloud") / lit(20.0))
        )
        .select(
            col("airport_code"),
            col("state"),
            col("county"),
            col("window.start").alias("event_window_start"),
            col("window.end").alias("event_window_end"),
            col("event_count"),
            col("min_dewpoint_spread"),
            col("stddev_pressure_mb"),
            col("max_gust_factor"),
            col("stddev_cloud"),
            col("instability_score")
        )
    )

    # ========= 6) Console sink =========
    console_query = (
        result.writeStream
        .outputMode("update")
        .format("console")
        .option("truncate", False)
        .start()
    )

    # ========= 7) Citus sink =========
    db_query = (
        result.writeStream
        .outputMode("update")
        .foreachBatch(lambda df, epochId: upsert_to_pg(df, epochId, args))
        .start()
    )

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()

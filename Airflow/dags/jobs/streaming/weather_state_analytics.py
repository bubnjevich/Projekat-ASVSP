import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, avg, count, countDistinct, sum as _sum,
    max as _max, min as _min, when, stddev
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
import psycopg2
from psycopg2.extras import execute_values


def quiet_logs(spark):
    jvm = spark.sparkContext._jvm
    logger = jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)


def build_spark(app_name="weather_state_analytics_job"):
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def upsert_to_pg(df, epoch_id, args):
    # Ako batch nema redova, preskoči
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
        state, interval_start, interval_end, event_count,
        avg_temp_c, min_temp_c, max_temp_c, stddev_temp_c,
        avg_humidity, stddev_humidity,
        heat_stress_days, weather_volatility
    )
    VALUES %s
    ON CONFLICT (state, interval_start) DO UPDATE SET
        interval_end = EXCLUDED.interval_end,
        event_count = EXCLUDED.event_count,
        avg_temp_c = EXCLUDED.avg_temp_c,
        min_temp_c = EXCLUDED.min_temp_c,
        max_temp_c = EXCLUDED.max_temp_c,
        stddev_temp_c = EXCLUDED.stddev_temp_c,
        avg_humidity = EXCLUDED.avg_humidity,
        stddev_humidity = EXCLUDED.stddev_humidity,
        heat_stress_days = EXCLUDED.heat_stress_days,
        weather_volatility = EXCLUDED.weather_volatility;
    """

    execute_values(cur, insert_sql, rows)
    conn.commit()
    cur.close()
    conn.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--jdbc-url", required=True)
    ap.add_argument("--dbtable", required=True)
    ap.add_argument("--dbuser", required=True)
    ap.add_argument("--dbpassword", required=True)
    ap.add_argument("--jdbc-mode", default="append", choices=["overwrite", "append"])
    ap.add_argument("--batch-path", required=True)
    args = ap.parse_args()

    spark = build_spark()
    quiet_logs(spark)

    # ========= 1) Kafka stream =========
    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "broker1:9092")
        .option("subscribe", "weather_raw")
        .load()
    )

    schema = StructType([
        StructField("airport_code", StringType()),
        StructField("state", StringType()),
        StructField("county", StringType()),
        StructField("timezone", StringType()),
        StructField("start_time_utc", TimestampType()),
        StructField("precip_in", DoubleType()),
        StructField("cloud", DoubleType()),
        StructField("temp_c", DoubleType()),
        StructField("humidity", DoubleType()),
        StructField("wind_kph", DoubleType()),
        StructField("pressure_mb", DoubleType()),
        StructField("vis_km", DoubleType())
    ])

    from pyspark.sql.functions import to_timestamp
    stream_df = (
        raw_stream
        .selectExpr("CAST(value AS STRING) AS json_str")
        .select(from_json(col("json_str"), schema).alias("data"))
        .select("data.*")
        .withColumn("start_time_utc", to_timestamp("start_time_utc"))
    )

    # ========= 2) Batch (static) =========
    batch_df = spark.read.parquet(args.batch_path)
    cols_to_drop = [c for c in ["state", "county", "timezone", "start_time_utc", "precipitation_in"] if c in batch_df.columns]
    batch_df = batch_df.drop(*cols_to_drop) if cols_to_drop else batch_df

    # ========= 3) Join stream + batch =========
    joined = stream_df.join(batch_df, on="airport_code", how="left")

    # ========= 4) State-level 6h window agregacija =========
    state_windowed = (
        joined
        .withWatermark("start_time_utc", "1 day")
        .groupBy(
            col("state"),
            window(col("start_time_utc"), "6 hours", "6 hours")
        )
        .agg(
            count("*").alias("event_count"),
            avg("temp_c").alias("avg_temp_c"),
            _min("temp_c").alias("min_temp_c"),
            _max("temp_c").alias("max_temp_c"),
            stddev("temp_c").alias("stddev_temp_c"),
            avg("humidity").alias("avg_humidity"),
            stddev("humidity").alias("stddev_humidity"),
            _sum(when((col("temp_c") >= 30) & (col("humidity") >= 60), 1).otherwise(0)).alias("heat_stress_days")
        )
        .withColumn("weather_volatility", col("stddev_temp_c") + col("stddev_humidity"))
        .select(
            col("state"),
            col("window.start").alias("interval_start"),
            col("window.end").alias("interval_end"),
            "event_count",
            "avg_temp_c",
            "min_temp_c",
            "max_temp_c",
            "stddev_temp_c",
            "avg_humidity",
            "stddev_humidity",
            "heat_stress_days",
            "weather_volatility"
        )
    )

    # ========= 5) Console sink =========
    console_query = (
        state_windowed.writeStream
        .outputMode("update")
        .format("console")
        .option("truncate", False)
        .start()
    )

    # ========= 6) Citus sink =========
    db_query = (
        state_windowed.writeStream
        .outputMode("update")
        .foreachBatch(lambda df, epochId: upsert_to_pg(df, epochId, args))
        .start()
    )

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()

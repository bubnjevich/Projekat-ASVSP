import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, avg, count, min as _min, max as _max, when, lit
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, IntegerType
import psycopg2
from psycopg2.extras import execute_values


def quiet_logs(spark):
    jvm = spark.sparkContext._jvm
    logger = jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)


def build_spark(app_name="weather_streaming_job_extended"):
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
        event_count, avg_temp_c, avg_humidity,
        min_dewpoint_spread, avg_uv_index, max_gust_kph,
        daytime_rate, avg_gti
    )
    VALUES %s
    ON CONFLICT (airport_code, event_window_start) DO UPDATE SET
        state = EXCLUDED.state,
        county = EXCLUDED.county,
        event_window_end = EXCLUDED.event_window_end,
        event_count = EXCLUDED.event_count,
        avg_temp_c = EXCLUDED.avg_temp_c,
        avg_humidity = EXCLUDED.avg_humidity,
        min_dewpoint_spread = EXCLUDED.min_dewpoint_spread,
        avg_uv_index = EXCLUDED.avg_uv_index,
        max_gust_kph = EXCLUDED.max_gust_kph,
        daytime_rate = EXCLUDED.daytime_rate,
        avg_gti = EXCLUDED.avg_gti;
    """

    execute_values(cur, insert_sql, rows)
    conn.commit()
    cur.close()
    conn.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--jdbc-url", required=True)
    ap.add_argument("--dbtable", required=True, help="Gold table for radiation KPI")
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

        # NOVI parametri
        StructField("temp_c", DoubleType()),
        StructField("humidity", DoubleType()),
        StructField("dewpoint_c", DoubleType()),
        StructField("uv", DoubleType()),
        StructField("gust_kph", DoubleType()),
        StructField("is_day", IntegerType()),   # 1 = dan, 0 = noć
        StructField("gti", DoubleType())
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
    joined = joined.withColumn("is_day_flag", when(col("is_day") == lit(1), 1.0).otherwise(0.0))

    # ========= 4) Sliding window =========
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
            avg("temp_c").alias("avg_temp_c"),
            avg("humidity").alias("avg_humidity"),
            _min("dewpoint_spread").alias("min_dewpoint_spread"),
            avg("uv").alias("avg_uv_index"),
            _max("gust_kph").alias("max_gust_kph"),
            avg("is_day_flag").alias("daytime_rate"),
            avg("gti").alias("avg_gti")
        )
    )

    result = (windowed
        .select(
            col("airport_code"),
            col("state"),
            col("county"),
            col("window.start").alias("event_window_start"),
            col("window.end").alias("event_window_end"),
            col("event_count"),
            col("avg_temp_c"),
            col("avg_humidity"),
            col("min_dewpoint_spread"),
            col("avg_uv_index"),
            col("max_gust_kph"),
            col("daytime_rate"),
            col("avg_gti")
        )
    )

    # ========= 5) Console sink =========
    console_query = (
        result.writeStream
        .outputMode("update")
        .format("console")
        .option("truncate", False)
        .start()
    )

    # ========= 6) Citus sink =========
    db_query = (
        result.writeStream
        .outputMode("update")
        .foreachBatch(lambda df, epochId: upsert_to_pg(df, epochId, args))
        .start()
    )

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()

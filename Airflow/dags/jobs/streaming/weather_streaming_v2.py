import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, avg, count, sum as _sum,
    max as _max, min as _min, when
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
import psycopg2
from psycopg2.extras import execute_values


def quiet_logs(spark):
    jvm = spark.sparkContext._jvm
    logger = jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)


def build_spark(app_name="weather_streaming_job_v2"):
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
        airport_code, state, county, event_window_start, event_window_end,
        event_count, total_precip_in, avg_temp_c, avg_humidity,
        max_wind_kph, min_vis_km, rainy_rate, heat_stress_rate
    )
    VALUES %s
    ON CONFLICT (airport_code, event_window_start) DO UPDATE SET
        state = EXCLUDED.state,
        county = EXCLUDED.county,
        event_window_end = EXCLUDED.event_window_end,
        event_count = EXCLUDED.event_count,
        total_precip_in = EXCLUDED.total_precip_in,
        avg_temp_c = EXCLUDED.avg_temp_c,
        avg_humidity = EXCLUDED.avg_humidity,
        max_wind_kph = EXCLUDED.max_wind_kph,
        min_vis_km = EXCLUDED.min_vis_km,
        rainy_rate = EXCLUDED.rainy_rate,
        heat_stress_rate = EXCLUDED.heat_stress_rate;
    """

    execute_values(cur, insert_sql, rows)
    conn.commit()
    cur.close()
    conn.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--jdbc-url", required=True)
    ap.add_argument("--dbtable", required=True, help="Gold tabela za sliding window KPI")
    ap.add_argument("--dbuser", required=True)
    ap.add_argument("--dbpassword", required=True)
    ap.add_argument("--jdbc-mode", default="append", choices=["overwrite", "append"])
    ap.add_argument("--batch-path", required=True)
    args = ap.parse_args()

    spark = build_spark()
    quiet_logs(spark)

    # ========= 1) Kafka stream =========
    raw_stream = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", "broker1:9092") \
        .option("subscribe", "weather_raw") \
        .load()

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

    stream_df = (raw_stream
        .selectExpr("CAST(value AS STRING) AS json_str")
        .select(from_json(col("json_str"), schema).alias("data"))
        .select("data.*")
    )


    # ========= 2) Batch (static) =========
    batch_df = spark.read.parquet(args.batch_path)
    cols_to_drop = [c for c in ["state", "county", "timezone", "start_time_utc", "precipitation_in"] if c in batch_df.columns]
    batch_df = batch_df.drop(*cols_to_drop) if cols_to_drop else batch_df

    # ========= 3) Join stream + batch =========
    joined = stream_df.join(batch_df, on="airport_code", how="left")

    # Feature engineering
    joined = (joined
        .withColumn("is_rainy", when(col("precip_in") > 0.0, 1.0).otherwise(0.0))
        .withColumn("is_heat_stress", when((col("temp_c") >= 30.0) & (col("humidity") >= 60.0), 1.0).otherwise(0.0))
    )

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
            _sum("precip_in").alias("total_precip_in"),
            avg("temp_c").alias("avg_temp_c"),
            avg("humidity").alias("avg_humidity"),
            _max("wind_kph").alias("max_wind_kph"),
            _min("vis_km").alias("min_vis_km"),
            _sum("is_rainy").alias("rainy_hits"),
            _sum("is_heat_stress").alias("heat_stress_hits")
        )
    )

    result = (windowed
        .withColumn("rainy_rate", (col("rainy_hits") / col("event_count")).cast("double"))
        .withColumn("heat_stress_rate", (col("heat_stress_hits") / col("event_count")).cast("double"))
        .select(
            col("airport_code"),
            col("state"),
            col("county"),
            col("window.start").alias("event_window_start"),
            col("window.end").alias("event_window_end"),
            col("event_count"),
            col("total_precip_in"),
            col("avg_temp_c"),
            col("avg_humidity"),
            col("max_wind_kph"),
            col("min_vis_km"),
            col("rainy_rate"),
            col("heat_stress_rate")
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

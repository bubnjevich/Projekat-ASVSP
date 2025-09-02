import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, count
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

def quiet_logs(spark):
    jvm = spark.sparkContext._jvm
    logger = jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

def build_spark(app_name="weather_streaming_job"):
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

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

    # ======================
    # 1. Kafka (stream raw)
    # ======================
    raw_stream = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", "broker1:9092") \
        .option("subscribe", "weather_raw") \
        .load()

    schema = StructType([
        StructField("airport_code", StringType()),
        StructField("state", StringType()),
        StructField("county", StringType()),
        StructField("start_time_utc", TimestampType()),  # batch join key
        StructField("precip_in", DoubleType()),
        StructField("cloud", DoubleType()),
        StructField("temp_c", DoubleType()),
        StructField("humidity", DoubleType()),
        StructField("wind_kph", DoubleType()),
        StructField("pressure_mb", DoubleType()),
        StructField("vis_km", DoubleType())
    ])

    stream_df = raw_stream.selectExpr("CAST(value AS STRING) as json_str") \
        .select(from_json(col("json_str"), schema).alias("data")) \
        .select("data.*")

    # ======================
    # 2. Batch zona
    # ======================
    batch_df = spark.read.parquet(args.batch_path)
    batch_df.printSchema()

    batch_df = batch_df \
        .drop("state") \
        .drop("county") \
        .drop("start_time_utc") \
        .drop("precipitation_in")

    # ======================
    # 3. Join stream + batch
    # ======================
    joined = stream_df.join(batch_df, "airport_code", "left")

    # ======================
    # 4. Windowing  (gold metrika)
    # ======================
    agg = joined \
        .withWatermark("start_time_utc", "10 minutes") \
        .groupBy(
            col("airport_code"),
            col("state"),
            col("county"),
            window(col("start_time_utc"), "5 hour")
        ) \
        .agg(
            count("*").alias("event_count"),
            avg("precip_in").alias("avg_precipitation"),
            avg("temp_c").alias("avg_temp_c"),
            avg("humidity").alias("avg_humidity"),
            avg("cloud").alias("avg_cloud"),
            avg("wind_kph").alias("avg_wind_kph"),
            avg("pressure_mb").alias("avg_pressure_mb"),
            avg("vis_km").alias("avg_vis_km")
        ) \
        .select(
            col("airport_code"),
            col("state"),
            col("county"),
            col("window.start").alias("event_window_start"),
            col("window.end").alias("event_window_end"),
            col("event_count"),
            col("avg_precipitation"),
            col("avg_temp_c"),
            col("avg_humidity"),
            col("avg_cloud"),
            col("avg_wind_kph"),
            col("avg_vis_km"),
            col("avg_pressure_mb")
        )

    # ======================
    # 5. Sink 1 – ispis u terminal
    # ======================
    console_query = (
        agg.writeStream
        .outputMode("update")
        .format("console")
        .option("truncate", False)
        .start()
    )

    # ======================
    # 6. Sink 2 – upis u Citus
    # ======================
    citus_query = (
        agg.writeStream
        .foreachBatch(lambda df, epochId:
                      df.write.format("jdbc")
                      .option("url", args.jdbc_url)
                      .option("dbtable", args.dbtable)
                      .option("user", args.dbuser)
                      .option("password", args.dbpassword)
                      .option("driver", "org.postgresql.Driver")
                      .mode("append")
                      .save()
                      )
        .outputMode("update")
        .start()
    )

    # ======================
    # 7. Čekaj oba
    # ======================
    console_query.awaitTermination()
    citus_query.awaitTermination()

    spark.stop()

if __name__ == "__main__":
    main()

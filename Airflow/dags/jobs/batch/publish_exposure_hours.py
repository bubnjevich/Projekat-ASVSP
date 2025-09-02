#!/usr/bin/env python3
"""
How many hours was the airport exposed to operationally risky weather conditions on a daily basis,
followed by rolling metrics (30 and 90 days).
30 days → shows the short-term trend (one month), useful for operational decisions
90 days → shows the seasonal trend, useful for strategic decisions
"""
import argparse
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window as W

RISK_TYPES = ["Fog", "Snow", "Thunderstorm"]
RISK_SEVERITY = ["Moderate", "Heavy"]

def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

def build_spark(app_name: str = "publish_exposure_hours"):
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

def exposure_hours_daily(spark, input_path: str):
    df = spark.read.parquet(input_path).select(
        "airport_code",
        "type",
        "severity",
        "local_start_ts",
        "local_end_ts"
    )

    # filtriramo samo rizične evente
    risky = (
        df.filter(
            (F.col("type").isin(RISK_TYPES)) |
            (F.col("severity").isin(RISK_SEVERITY))
        )
        .filter(F.col("local_end_ts").isNotNull() & F.col("local_start_ts").isNotNull())
        .filter(F.col("local_end_ts") > F.col("local_start_ts"))
    )

    # razbijamo događaje po danima (ako prelazi ponoć)
    exploded = (
        risky
        .withColumn("start_date", F.to_date("local_start_ts"))
        .withColumn("end_date", F.to_date("local_end_ts"))
        .withColumn("day", F.explode(F.sequence(F.col("start_date"), F.col("end_date"))))
    )

    day_start_ts = F.to_timestamp(F.concat_ws(" ", F.col("day"), F.lit("00:00:00")))
    day_end_ts = F.to_timestamp(F.concat_ws(" ", F.date_add(F.col("day"), 1), F.lit("00:00:00")))

    with_overlap = (
        exploded
        .withColumn("ov_start", F.greatest(F.col("local_start_ts"), day_start_ts))
        .withColumn("ov_end", F.least(F.col("local_end_ts"), day_end_ts))
        .withColumn("ov_secs", (F.unix_timestamp("ov_end") - F.unix_timestamp("ov_start")).cast("double"))
        .withColumn("exposure_hours", F.when(F.col("ov_secs") > 0, F.col("ov_secs") / 3600.0).otherwise(0.0))
    )

    # ...
    daily = (
        with_overlap.groupBy("airport_code", "day")
        .agg(F.sum("exposure_hours").alias("exposure_hours_day"))
    )

    daily = daily.withColumn("event_date", F.col("day").cast("date")).drop("day")

    sec_per_day = 24 * 60 * 60
    order_col = F.col("event_date").cast("timestamp").cast("long")

    w30 = W.partitionBy("airport_code").orderBy(order_col).rangeBetween(-29 * sec_per_day, 0)
    w90 = W.partitionBy("airport_code").orderBy(order_col).rangeBetween(-89 * sec_per_day, 0)

    daily_roll = (
        daily
        .withColumn("exposure_hours_30d", F.sum("exposure_hours_day").over(w30))
        .withColumn("exposure_hours_90d", F.sum("exposure_hours_day").over(w90))
    )

    daily_roll = (
        daily_roll
        .withColumn("exposure_hours_day", F.round("exposure_hours_day", 2))
        .withColumn("exposure_hours_30d", F.round("exposure_hours_30d", 2))
        .withColumn("exposure_hours_90d", F.round("exposure_hours_90d", 2))
    )

    return daily_roll.select(
        "airport_code", "event_date",
        "exposure_hours_day", "exposure_hours_30d", "exposure_hours_90d"
    )


def write_jdbc(df, jdbc_url, dbtable, user, password):
    (
        df.repartition("airport_code")
        .write
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", dbtable)
        .option("user", user)
        .option("password", password)
        .option("driver", "org.postgresql.Driver")
        .option("batchsize", "10000")
        .mode("overwrite")          # truncate + insert fresh
        .option("truncate", "true")
        .save()
    )

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--jdbc-url", required=True)
    parser.add_argument("--dbtable", required=True)
    parser.add_argument("--dbuser", required=True)
    parser.add_argument("--dbpassword", required=True)
    parser.add_argument(
        "--input-path",
        default="hdfs://namenode:9000/data/weather/transform/batch/events_clean",
        help="Path to transformed parquet data"
    )
    args = parser.parse_args()

    spark = build_spark()
    quiet_logs(spark)
    try:
        df = exposure_hours_daily(spark, args.input_path)
        write_jdbc(df, args.jdbc_url, args.dbtable, args.dbuser, args.dbpassword)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()

#!/usr/bin/env python3
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window as window
import argparse

INPUT_PATH = "hdfs://namenode:9000/data/weather/transform/events_clean"

def quiet_logs(spark):
    jvm = spark.sparkContext._jvm
    logger = jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

def build_spark(app_name="publish_monthly_risk_trend_by_type"):
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--jdbc-url", required=True)
    ap.add_argument("--dbtable", required=True)   # npr. curated.monthly_risk_trend_by_type
    ap.add_argument("--dbuser", required=True)
    ap.add_argument("--dbpassword", required=True)
    ap.add_argument("--jdbc-mode", default="overwrite", choices=["overwrite","append"])
    args = ap.parse_args()

    spark = build_spark()
    quiet_logs(spark)

    # 1) Učitaj i pripremi trajanje (min); koristi lokalne timestamp-e iz ulaza
    df = (
        spark.read.parquet(INPUT_PATH)
            .select("airport_code", "type", "local_start_ts", "local_end_ts")
            .withColumn("duration_min",
                        (F.unix_timestamp("local_end_ts") - F.unix_timestamp("local_start_ts")) / 60.0)
            .withColumn("year",  F.year("local_start_ts"))
            .withColumn("month", F.month("local_start_ts"))
    )

    # 2) Mesečna agregacija po airport+type
    monthly = (
        df.groupBy("airport_code", "year", "month", "type")
          .agg(
              F.round(F.avg("duration_min"), 2).alias("avg_duration_min"),
              F.count("*").alias("total_events")
          )
    )

    # 3) MOM promena po airport+type (lag po hronologiji meseci)
    w = window.partitionBy("airport_code", "type").orderBy("year", "month")
    monthly = monthly.withColumn("prev_avg", F.lag("avg_duration_min").over(w))
    monthly = monthly.withColumn(
        "mom_change_pct",
        F.when(F.col("prev_avg").isNull() | (F.col("prev_avg") < 5), None)
         .otherwise(F.round(((F.col("avg_duration_min") - F.col("prev_avg")) / F.col("prev_avg")) * 100.0, 2))
    ).drop("prev_avg")

    # 4) Upis u Citus
    (monthly.write
        .format("jdbc")
        .option("url", args.jdbc_url)
        .option("dbtable", args.dbtable)
        .option("user", args.dbuser)
        .option("password", args.dbpassword)
        .option("driver", "org.postgresql.Driver")
        .mode(args.jdbc_mode)
        .save())

    spark.stop()

if __name__ == "__main__":
    main()

#!/usr/bin/env python3
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window as window
import argparse

RISK_TYPES = ["Fog", "Snow", "Thunderstorm"]
RISK_SEVERITY = ["Moderate", "Heavy"]
GAP_MINUTES = 60
INPUT_PATH = "hdfs://namenode:9000/data/weather/transform/batch/events_clean"

def quiet_logs(spark):
    jvm = spark.sparkContext._jvm
    logger = jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

def build_spark(app_name="publish_longest_episodes"):
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
    ap.add_argument("--jdbc-mode", default="overwrite", choices=["overwrite","append"])
    args = ap.parse_args()

    spark = build_spark()
    quiet_logs(spark)

    df = spark.read.parquet(INPUT_PATH).select(
        "airport_code", "type", "severity", "local_start_ts", "local_end_ts"
    )

    # 2) filtar rizičnih događaja (Fog, Snow, Thunderstorm, Moderate, Heavy)
    df = (
        df.filter(F.upper(F.col("type")).isin([t.upper() for t in RISK_TYPES]))
          .filter(F.initcap(F.col("severity")).isin(RISK_SEVERITY))
    )

    # 3) sessionization po aerodromu (gap <= 60 min)
    w_sort = window.partitionBy("airport_code", "type").orderBy("local_start_ts")
    gap_sec = F.unix_timestamp("local_start_ts") - F.unix_timestamp(F.lag("local_end_ts").over(w_sort))
    df = df.withColumn("gap_sec", gap_sec)

    threshold = GAP_MINUTES * 60
    # Ako je prvi dogadjaj u grupi (gap_sec IS NULL) ili je pauza > 60 min  tada pocinje nova epizoda.
    # Inace, dogadjaj pripada istoj epizodi.
    df = df.withColumn(
        "is_new_session",
        F.when(F.col("gap_sec").isNull() | (F.col("gap_sec") > threshold), 1).otherwise(0),
    )

    w_cum = w_sort.rowsBetween(window.unboundedPreceding, window.currentRow)
    df = df.withColumn("session_seq", F.sum("is_new_session").over(w_cum))

    sessions = (
        df.groupBy("airport_code", "type", "session_seq")
          .agg(
              F.min("local_start_ts").alias("session_start"),
              F.max("local_end_ts").alias("session_end"),
              F.count("*").alias("events_in_session")
          )
          .withColumn(
              "session_duration_hours",
              (F.unix_timestamp("session_end") - F.unix_timestamp("session_start")) / 3600.0
          )
    )

    # 4) najduza epizoda po aerodromu
    w_rank = window.partitionBy("airport_code", "type").orderBy(F.col("session_duration_hours").desc(), F.col("session_start").asc())
    longest = (
        sessions.withColumn("rn", F.row_number().over(w_rank))
                .where("rn = 1")
                .drop("rn")
                .withColumn("session_duration_hours", F.round("session_duration_hours", 2))
                .select("airport_code", "type", "session_start", "session_end", "session_duration_hours", "events_in_session")
    )

    # 5) upis u Citus
    (longest.write
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

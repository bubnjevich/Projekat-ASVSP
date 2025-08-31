#!/usr/bin/env python3
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window as W
import argparse

INPUT_PATH = "hdfs://namenode:9000/data/weather/transform/events_clean"

def quiet_logs(spark):
    jvm = spark.sparkContext._jvm
    logger = jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

def build_spark(app_name="publish_winter_burstiness"):
    return (SparkSession.builder
            .appName(app_name)
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate())

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--jdbc-url", required=True)
    ap.add_argument("--dbtable", required=True)     # curated.winter_burstiness
    ap.add_argument("--dbuser", required=True)
    ap.add_argument("--dbpassword", required=True)
    ap.add_argument("--jdbc-mode", default="overwrite", choices=["overwrite","append"])
    ap.add_argument("--gap-threshold-hours", type=float, default=6.0)   # prekid koji *započinje novu* epizodu
    ap.add_argument("--min-episode-hours", type=float, default=12.0)    # prag za "duge" epizode
    args = ap.parse_args()

    spark = build_spark()
    quiet_logs(spark)

    # 1) Učitaj i filtriraj zimske evente
    df = (
        spark.read.parquet(INPUT_PATH)
        .select("state","county","type","local_start_ts","local_end_ts","season")
        .withColumn("type_u", F.upper("type"))
        .filter(F.col("type_u").isin("SNOW","ICE","COLD"))

    )

    # 2) Sezonsko grupisanje: DJF → decembar ide u narednu godinu
    df = df.withColumn("year",  F.year("local_start_ts")) \
           .withColumn("month", F.month("local_start_ts")) \
           .withColumn("season_year", F.when(F.col("month")==12, F.col("year")+1).otherwise(F.col("year"))) \
           .filter(F.col("season") == "DJF")

    # 3) Sortiraj po county/season_year i pronađi prekide (gap)
    w_time = W.partitionBy("state","county","season_year").orderBy("local_start_ts")
    df = df.withColumn("prev_end", F.lag("local_end_ts").over(w_time))
    df = df.withColumn(
        "gap_h",
        (F.unix_timestamp("local_start_ts") - F.unix_timestamp("prev_end"))/3600.0
    )

    # Nova epizoda ako prvo pojavljivanje ili gap > threshold
    df = df.withColumn(
        "new_episode",
        F.when(F.col("gap_h").isNull() | (F.col("gap_h") > F.lit(args.gap_threshold_hours)), 1).otherwise(0)
    )

    # 4) Kumulativni ID epizode putem windowinga (burst segmentation)
    w_cum = w_time.rowsBetween(W.unboundedPreceding, 0)
    df = df.withColumn("episode_id", F.sum("new_episode").over(w_cum)) 

    # 5) Sažmi po epizodi: start, end, trajanje, inter-episode gap (lag end-a)
    episodes = (
        df.groupBy("state","county","season_year","episode_id")
          .agg(
              F.min("local_start_ts").alias("episode_start"),
              F.max("local_end_ts").alias("episode_end")
          )
          .withColumn("duration_h",
              (F.unix_timestamp("episode_end") - F.unix_timestamp("episode_start"))/3600.0
          )
    )

    # inter-episode gap: razmak između *početka* tekuće i *kraja* prethodne epizode
    w_ep = W.partitionBy("state","county","season_year").orderBy("episode_start")
    episodes = episodes.withColumn("prev_episode_end", F.lag("episode_end").over(w_ep))
    episodes = episodes.withColumn(
        "interepisode_gap_h",
        (F.unix_timestamp("episode_start") - F.unix_timestamp("prev_episode_end"))/3600.0
    )

    # 6) Agregacija na county/state + season_year
    burstiness = (
        episodes.groupBy("state","county","season_year")
        .agg(
            F.count("*").alias("total_episodes"),
            F.sum(F.when(F.col("duration_h") > F.lit(args.min_episode_hours), 1).otherwise(0)).alias("episodes_over_threshold"),
            F.round(F.avg("interepisode_gap_h"), 2).alias("avg_interepisode_gap_h"),
            F.round(F.avg("duration_h"), 2).alias("avg_episode_duration_h"),
            F.round(F.max("duration_h"), 2).alias("max_episode_duration_h")
        )
    )

    # 7) Upis u Citus
    (burstiness.write
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

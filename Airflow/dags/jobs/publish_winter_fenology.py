#!/usr/bin/env python3
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window as W
import argparse

INPUT_PATH = "hdfs://namenode:9000/data/weather/transform/batch/events_clean"

def quiet_logs(spark):
    jvm = spark.sparkContext._jvm
    logger = jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

def build_spark(app_name="publish_winter_fenology"):
    return (SparkSession.builder
            .appName(app_name)
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate())

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--jdbc-url", required=True)
    ap.add_argument("--dbtable", required=True)   # curated.winter_fenology
    ap.add_argument("--dbuser", required=True)
    ap.add_argument("--dbpassword", required=True)
    ap.add_argument("--jdbc-mode", default="overwrite", choices=["overwrite","append"])
    ap.add_argument("--winter-types", default="SNOW,ICE,COLD",
                    help="Comma-separated list of types treated as winter events")
    args = ap.parse_args()

    spark = build_spark()
    quiet_logs(spark)

    winter_types = [t.strip().upper() for t in args.winter_types.split(",") if t.strip()]

    # 1) Učitaj i filtriraj zimske događaje (DJF; validni timestamp-i)
    df = (
        spark.read.parquet(INPUT_PATH)
        .select("state","county","type","season","local_start_ts","local_end_ts")
        .withColumn("type_u", F.upper("type"))
        .filter(F.col("type_u").isin(winter_types))
        .filter(F.col("season") == F.lit("DJF"))
        .withColumn("year",  F.year("local_start_ts"))
        .withColumn("month", F.month("local_start_ts"))
        # DJF: decembar pripada narednoj godini
        .withColumn("season_year", F.when(F.col("month")==12, F.col("year")+1).otherwise(F.col("year")))
    )

    # 2) Agregacija: prvi i poslednji događaj po season_year
    fen = (
        df.groupBy("state","county","season_year")
          .agg(
              F.min("local_start_ts").alias("first_event_ts"),
              F.max("local_end_ts").alias("last_event_ts")
          )
          .withColumn(
              "season_length_days",
              F.round((F.unix_timestamp("last_event_ts") - F.unix_timestamp("first_event_ts"))/86400.0, 2)
          )
    )

    # 3) Day-of-year vrednosti za poređenja
    fen = fen \
        .withColumn("first_doy", F.dayofyear("first_event_ts")) \
        .withColumn("last_doy",  F.dayofyear("last_event_ts"))

    # 4) Windowing: poređenja sa prethodnom sezonom + rolling 3y prosek
    w = W.partitionBy("state","county").orderBy("season_year")

    fen = (
        fen
        .withColumn("prev_first_doy",  F.lag("first_doy").over(w))
        .withColumn("prev_last_doy",   F.lag("last_doy").over(w))
        .withColumn("prev_length",     F.lag("season_length_days").over(w))
        # start_shift_days sa cikličkom korekcijom
        .withColumn(
            "start_shift_days",
            F.when(F.col("prev_first_doy").isNotNull(),
                F.when((F.col("first_doy") - F.col("prev_first_doy")) > 180,
                       (F.col("first_doy") - F.col("prev_first_doy")) - 365
                ).when((F.col("first_doy") - F.col("prev_first_doy")) < -180,
                       (F.col("first_doy") - F.col("prev_first_doy")) + 365
                ).otherwise(F.col("first_doy") - F.col("prev_first_doy"))
            )
        )
        # end_shift_days sa cikličkom korekcijom
        .withColumn(
            "end_shift_days",
            F.when(F.col("prev_last_doy").isNotNull(),
                F.when((F.col("last_doy") - F.col("prev_last_doy")) > 180,
                       (F.col("last_doy") - F.col("prev_last_doy")) - 365
                ).when((F.col("last_doy") - F.col("prev_last_doy")) < -180,
                       (F.col("last_doy") - F.col("prev_last_doy")) + 365
                ).otherwise(F.col("last_doy") - F.col("prev_last_doy"))
            )
        )
        .withColumn(
            "yoy_length_change_days",
            F.when(F.col("prev_length").isNotNull(),
                   F.round(F.col("season_length_days") - F.col("prev_length"), 2))
        )
    )

    # rolling 3y prosek trajanja
    w3 = w.rowsBetween(-2, 0)
    fen = fen.withColumn("rolling_3y_length_avg_days",
                         F.round(F.avg("season_length_days").over(w3), 2)) \
             .drop("prev_first_doy","prev_last_doy","prev_length","first_doy","last_doy")

    # 5) Upis u Citus
    (fen.write
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

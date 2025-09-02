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

def build_spark(app_name="publish_night_thunderstorm_risk"):
    return (SparkSession.builder
            .appName(app_name)
            .config("spark.sql.session.timeZone","UTC")
            .getOrCreate())

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--jdbc-url", required=True)
    ap.add_argument("--dbtable", required=True)   # curated.night_thunderstorm_risk
    ap.add_argument("--dbuser", required=True)
    ap.add_argument("--dbpassword", required=True)
    ap.add_argument("--jdbc-mode", default="overwrite", choices=["overwrite","append"])
    args = ap.parse_args()

    spark = build_spark()
    quiet_logs(spark)

    # 1) Učitaj i filtriraj samo THUNDERSTORM događaje
    df = (
        spark.read.parquet(INPUT_PATH)
        .select("state","county","type","severity","precipitation_in","is_night",
                "local_start_ts","local_end_ts")
        .filter(F.col("local_end_ts").isNotNull() & F.col("local_start_ts").isNotNull())
        .filter(F.col("local_end_ts") > F.col("local_start_ts"))
        .withColumn("type_u", F.upper("type"))
        .filter(F.col("type_u") == "STORM")   # samo oluje
        .withColumn("duration_h",
            (F.unix_timestamp("local_end_ts") - F.unix_timestamp("local_start_ts"))/3600.0
        )
        .withColumn("year", F.year("local_start_ts"))
        .withColumn("month", F.month("local_start_ts"))
        .withColumn("sev_score",
            F.when(F.upper("severity")=="LIGHT",1)
             .when(F.upper("severity")=="MODERATE",2)
             .when(F.upper("severity")=="HEAVY",3)
             .otherwise(1))
    )

    # 2) Agregacija po state+county+year+month
    result = (
        df.groupBy("state","county","year","month")
          .agg(
              F.count("*").alias("count_events"),
              F.sum(F.when(F.col("is_night")==True,1).otherwise(0)).alias("count_night_events"),
              F.round(F.avg(F.when(F.col("is_night")==True, F.col("duration_h"))),2).alias("avg_duration_h"),
              F.round(F.avg(F.when(F.col("is_night")==True, F.col("sev_score"))),2).alias("avg_severity_score")
          )
          .withColumn("night_event_ratio",
              (F.round(F.col("count_night_events")/F.col("count_events"), 2)).cast("double"))
    )

    # 3) rolling 3-month average za avg_duration_h
    w3 = W.partitionBy("state","county").orderBy("year","month").rowsBetween(-2, 0)
    result = result.withColumn("rolling_3m_avg_duration",
                               F.round(F.avg("avg_duration_h").over(w3),2))

    # 4) Upis u Citus
    (result.write
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

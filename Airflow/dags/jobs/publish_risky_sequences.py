#!/usr/bin/env python3
from pyspark.sql import SparkSession, functions as F, Window as W
import argparse

INPUT_PATH = "hdfs://namenode:9000/data/weather/transform/events_clean"

def quiet_logs(spark):
    jvm = spark.sparkContext._jvm
    logger = jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

def build_spark(app_name="publish_risky_sequences"):
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

    df = (
        spark.read.parquet(INPUT_PATH)
        .select("state", "county", "type", "local_start_ts", "local_end_ts")
        .withColumn("type_upper", F.upper("type"))
    )

    w = W.partitionBy("state","county").orderBy("local_start_ts")

    df = (df
          .withColumn("next_type1", F.lead("type_upper", 1).over(w))
          .withColumn("next_start1", F.lead("local_start_ts", 1).over(w))
          .withColumn("next_end1", F.lead("local_end_ts", 1).over(w))
          .withColumn("next_type2", F.lead("type_upper", 2).over(w))
          .withColumn("next_start2", F.lead("local_start_ts", 2).over(w))
          .withColumn("next_end2", F.lead("local_end_ts", 2).over(w))
    )

    # 3) Pravilo: RAIN → FREEZE → SNOW u 48h
    risky = (
        df.filter(
            (F.col("type_upper") == "RAIN") &
            (F.col("next_type1").isin("COLD")) &
            (F.col("next_type2") == "SNOW") &
            (
                F.unix_timestamp("next_end2") - F.unix_timestamp("local_start_ts") <= 48*3600
            )
        )
        .withColumn("seq_start_ts", F.col("local_start_ts"))
        .withColumn("seq_end_ts", F.col("next_end2"))
        .withColumn("duration_h",
                    F.round((F.unix_timestamp("next_end2") - F.unix_timestamp("local_start_ts"))/3600.0, 2)
                    )
        .withColumn("sequence", F.lit("RAIN→FREEZE→SNOW"))
        .select("state","county","seq_start_ts","seq_end_ts","duration_h","sequence")
    )

    (risky.write
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

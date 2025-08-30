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

def build_spark(app_name="publish_winter_workload_index"):
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
        .withColumn("duration_h", F.col("duration_min") / 60.0)
        .withColumn("year", F.year("local_start_ts"))
        .withColumn("month", F.month("local_start_ts"))
    )

    severity_w = (
        F.when(F.upper(F.col("severity")) == "LIGHT", 1)
        .when(F.upper(F.col("severity")) == "MODERATE", 2)
        .when(F.upper(F.col("severity")) == "HEAVY", 3)
        .otherwise(1)
    )

    type_w = (
        F.when(F.upper(F.col("type")) == "SNOW", 3)
        .when(F.upper(F.col("type")) == "ICE", 2)
        .when(F.upper(F.col("type")) == "COLD", 1)
        .otherwise(0)
    )

    df = df.withColumn("score", F.col("duration_h") * severity_w * type_w)
    df.show(20, truncate=False)

    # 3) Agregacija po okrugu i mesecu
    monthly = (
        df.groupBy("state", "county", "year", "month")
        .agg(F.round(F.sum("score"), 2).alias("workload_index")) # svakom okrugu u mesecu dodeljuje workload index
    )

    w = W.partitionBy("state", "county").orderBy("year", "month")

    monthly = (
        monthly
        .withColumn("prev_index", F.lag("workload_index").over(w))
        .withColumn(
            "mom_change_pct",
            F.when(
                F.col("prev_index").isNotNull(),
                F.round(((F.col("workload_index") - F.col("prev_index")) / F.col("prev_index")) * 100.0, 2)
            )
        )
        .withColumn("rolling_3m_avg", F.avg("workload_index").over(w.rowsBetween(-2, 0)))
        .withColumn("rank_in_state",
            F.dense_rank().over(W.partitionBy("state","year","month").orderBy(F.col("workload_index").desc()))
        )
        .drop("prev_index")
    )

    # 5) Upis u Citus
    (
        monthly.write
        .format("jdbc")
        .option("url", args.jdbc_url)
        .option("dbtable", args.dbtable)
        .option("user", args.dbuser)
        .option("password", args.dbpassword)
        .option("driver", "org.postgresql.Driver")
        .mode(args.jdbc_mode)
        .save()
    )

    spark.stop()

if __name__ == "__main__":
    main()

#!/usr/bin/env python3
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window as window
import argparse

INPUT_PATH = "hdfs://namenode:9000/data/weather/transform/batch/events_clean"

def quiet_logs(spark):
    jvm = spark.sparkContext._jvm
    logger = jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

def build_spark(app_name="publish_cooccurrence_minutes_daily"):
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

    events = (
        spark.read.parquet(INPUT_PATH)
             .select("airport_code", "type", "local_start_ts", "local_end_ts")
    )

    # 2) Markeri (+1 start, -1 end)
    starts = (events
        .select("airport_code", "type", F.col("local_start_ts").alias("ts"))
        .withColumn("delta", F.lit(1)))
    ends = (events
        .select("airport_code", "type", F.col("local_end_ts").alias("ts"))
        .withColumn("delta", F.lit(-1)))
    markers = starts.unionByName(ends)

    # 3) Kumulativno stanje po (airport, type)
    w_type = window.partitionBy("airport_code", "type").orderBy("ts") \
                   .rowsBetween(window.unboundedPreceding, window.currentRow)
    active_at_ts = (markers
        .withColumn("active_cnt", F.sum("delta").over(w_type)) # da znamo da li je u tom trenutku aktivno ili ne
        .select("airport_code", "type", "ts", "active_cnt"))

    # 4) Vremenska linija (segmenti [ts, next_ts))
    timeline = (markers
            .select("airport_code", "ts").distinct()
        .withColumn("next_ts", F.lead("ts").over(window.partitionBy("airport_code").orderBy("ts")))
        .where(F.col("next_ts").isNotNull()))

    # 5) Grid = (airport, ts, type)
    airport_types = events.select("airport_code", "type").distinct()
    grid = timeline.join(airport_types, on="airport_code", how="left")

    # 6) Stanje po tipu na grid + carry-forward (last value)
    state_on_grid = (grid
        .join(active_at_ts, ["airport_code", "type", "ts"], "left")
        .withColumn(
            "active_cnt_ffill",
            F.last("active_cnt", ignorenulls=True).over(
                window.partitionBy("airport_code", "type").orderBy("ts")
                      .rowsBetween(window.unboundedPreceding, window.currentRow)
            )
        )
        .fillna({"active_cnt_ffill": 0})
    )

    # 7) Aktivni tipovi po segmentu
    active_types_per_segment = (state_on_grid
        .where(F.col("active_cnt_ffill") > 0)
        .groupBy("airport_code", "ts", "next_ts")
        .agg(F.collect_set("type").alias("active_types"))
        .withColumn("segment_minutes",
                    (F.unix_timestamp("next_ts") - F.unix_timestamp("ts")) / 60.0)
        .where(F.size("active_types") >= 2))

    # 8) Svi parovi tipova i njihovo trajanje
    left = (active_types_per_segment
        .select("airport_code", "ts", "next_ts", "segment_minutes",
                F.posexplode("active_types").alias("i", "type_i")))
    right = (active_types_per_segment
        .select("airport_code", "ts", "next_ts", "segment_minutes",
                F.posexplode("active_types").alias("j", "type_j")))

    pairs = (left.join(right, ["airport_code", "ts", "next_ts", "segment_minutes"])
                  .where(F.col("i") < F.col("j"))
                  .select("airport_code", "ts", "segment_minutes", "type_i", "type_j"))

    # 9) Normalizacija parova (uvek type_i <= type_j) i dodavanje event_date
    pair_i = F.when(F.col("type_i") <= F.col("type_j"), F.col("type_i")).otherwise(F.col("type_j"))
    pair_j = F.when(F.col("type_i") <= F.col("type_j"), F.col("type_j")).otherwise(F.col("type_i"))

    cooccur_daily = (pairs
        .withColumn("event_date", F.to_date("ts"))
        .withColumn("type_i_norm", pair_i)
        .withColumn("type_j_norm", pair_j)
        .groupBy("airport_code", "event_date", "type_i_norm", "type_j_norm")
        .agg(F.sum("segment_minutes").alias("overlap_minutes"))
        .withColumnRenamed("type_i_norm", "type_i")
        .withColumnRenamed("type_j_norm", "type_j")
    )

    # 10) Upis u Citus
    (cooccur_daily.write
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

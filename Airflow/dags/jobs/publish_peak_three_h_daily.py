#!/usr/bin/env python3
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window as window
import argparse

INPUT_PATH = "hdfs://namenode:9000/data/weather/transform/events_clean"
RISK_TYPES = ["Fog", "Snow", "Thunderstorm"]
RISK_SEVERITY = ["Moderate", "Heavy"]
def quiet_logs(spark):
    jvm = spark.sparkContext._jvm
    logger = jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

def build_spark(app_name="publish_peak_exposure_3h_daily"):
    return (SparkSession.builder
            .appName(app_name)
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate())

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

    # 0) Load minimal fields
    events = (spark.read.parquet(INPUT_PATH)
              .select("airport_code", "type", "severity", "local_start_ts", "local_end_ts")
              .filter(F.upper(F.col("type")).isin([t.upper() for t in RISK_TYPES]))
              .filter(F.initcap(F.col("severity")).isin(RISK_SEVERITY))
    )


    # 1) Markers (+1 start, -1 end)
    starts = (events.select("airport_code", F.col("local_start_ts").alias("ts"))
                    .withColumn("delta", F.lit(1)))
    ends   = (events.select("airport_code", F.col("local_end_ts").alias("ts"))
                    .withColumn("delta", F.lit(-1)))
    markers = starts.unionByName(ends)

    # 2) Active-any-risk per segment (carry-forward on timeline)
    # 2a) timeline segments per airport
    timeline = (markers.select("airport_code","ts").distinct()
                .withColumn("next_ts", F.lead("ts").over(
                    window.partitionBy("airport_code").orderBy("ts")))
                .where(F.col("next_ts").isNotNull()))

    # 2b) compute active count of events (any type) per airport
    w_air = window.partitionBy("airport_code").orderBy("ts") \
                  .rowsBetween(window.unboundedPreceding, window.currentRow)
    active_any = (markers
        .withColumn("active_cnt", F.sum("delta").over(w_air))
        .select("airport_code","ts","active_cnt"))

    # 2c) join to timeline and ffill
    state = (timeline.join(active_any, ["airport_code","ts"], "left")
             .withColumn("active_ffill",
                 F.last("active_cnt", ignorenulls=True).over(
                     window.partitionBy("airport_code").orderBy("ts")
                           .rowsBetween(window.unboundedPreceding, window.currentRow)))
             .fillna({"active_ffill": 0}))

    # 3) segment_minutes_risk = minutes if active>0 else 0
    seg = (state
        .withColumn("segment_minutes",
                    (F.unix_timestamp("next_ts") - F.unix_timestamp("ts"))/60.0)
        .withColumn("segment_minutes_risk",
                    F.when(F.col("active_ffill") > 0, F.col("segment_minutes")).otherwise(F.lit(0.0)))
        .withColumn("event_date", F.to_date("ts"))
        .withColumn("ts_long", F.unix_timestamp("ts")))

    # 4) Rolling 3h sum with rangeBetween on ts_long
    w_3h = window.partitionBy("airport_code","event_date") \
                 .orderBy("ts_long").rangeBetween(-3*3600, 0)

    seg = seg.withColumn("rolling_3h_minutes",
                         F.sum("segment_minutes_risk").over(w_3h))

    # 5) For each (airport, day) pick the max and its window start
    w_day = window.partitionBy("airport_code","event_date") \
                  .orderBy(F.col("rolling_3h_minutes").desc(), F.col("ts").asc())

    out = (seg.withColumn("rn", F.row_number().over(w_day))
              .where(F.col("rn")==1)
              .select(
                  "airport_code",
                  "event_date",
                  F.col("rolling_3h_minutes").alias("p3h_minutes"),
                  F.col("ts").alias("window_start"),
                  F.expr("ts + INTERVAL 3 HOURS").alias("window_end")
              ))

    # 6) Write to Citus
    (out.write
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

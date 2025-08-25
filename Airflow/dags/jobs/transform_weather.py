# dags/jobs/transform_weather.py
import argparse
from pyspark.sql import SparkSession, functions as F, types as T

def quiet_logs(sc):
  logger = sc._jvm.org.apache.log4j
  logger.LogManager.getLogger("org"). setLevel(logger.Level.ERROR)
  logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

def season_expr(month_col):
    return (F.when(month_col.isin(12,1,2), F.lit("DJF"))
              .when(month_col.isin(3,4,5), F.lit("MAM"))
              .when(month_col.isin(6,7,8), F.lit("JJA"))
              .otherwise(F.lit("SON")))

def main(run_date, raw_path, out_base):
    spark = (SparkSession.builder
             .appName(f"transform_weather_{run_date}")
             .config("spark.pyspark.python", "python3")
             .config("spark.pyspark.driver.python", "python3")
             .getOrCreate())
    quiet_logs(spark)

    # 1) Učitavanje RAW CSV
    df_raw = (spark.read
              .option("header", True)
              .option("mode", "PERMISSIVE")
              .csv(raw_path))

    # 2) Normalizacija imena kolona
    for c in df_raw.columns:
        df_raw = df_raw.withColumnRenamed(c, c.strip().lower().replace(" ", "_").replace("(", "_").replace(")", "_"))

    # 3) Cast tipova
    def to_ts(col):
        return F.to_timestamp(F.col(col))

    df = (df_raw
          .withColumn("start_time_utc", to_ts("starttime_utc_"))
          .withColumn("end_time_utc", to_ts("endtime_utc_"))
          .withColumn("location_lat", F.col("locationlat").cast("double"))
          .withColumn("location_lng", F.col("locationlng").cast("double"))
          .withColumn("precipitation_in", F.col("precipitation_in_").cast("double"))
          .withColumn("zip_code", F.col("zipcode").cast("string"))
          .withColumn("type", F.upper(F.trim(F.col("type"))))
          .withColumn("severity", F.initcap(F.trim(F.col("severity"))))
          .withColumn("timezone", F.col("timezone"))
          .withColumn("airport_code", F.col("airportcode"))
          .withColumn("state", F.col("state"))
          .withColumn("county", F.col("county"))
          .withColumn("city", F.col("city"))
          .withColumn("event_id", F.col("eventid"))
          )

    # 4) Validacija & quarantine
    cond_valid = (
        F.col("start_time_utc").isNotNull() &
        F.col("end_time_utc").isNotNull() &
        (F.col("end_time_utc") >= F.col("start_time_utc")) &
        F.col("type").isNotNull() &
        (F.col("location_lat").between(-90, 90)) &
        (F.col("location_lng").between(-180, 180))
    )
    df_bad = (df.where(~cond_valid)
                .withColumn("reject_reason", F.lit("basic_validation_failed")))

    # 5) Derivacije
    df_good = (df.where(cond_valid)
                 .withColumn("duration_min", (F.col("end_time_utc").cast("long") - F.col("start_time_utc").cast("long"))/60.0)
                 .where(F.col("duration_min") > 0)
                 .withColumn("event_date", F.to_date("start_time_utc"))
                 .withColumn("event_year", F.year("start_time_utc"))
                 .withColumn("event_month", F.month("start_time_utc"))
                 .withColumn("event_day", F.dayofmonth("start_time_utc"))
                 .withColumn("hour_of_day", F.hour("start_time_utc"))
                 .withColumn("dow", ((F.dayofweek("start_time_utc") + 5) % 7) + 1)
                 .withColumn("season", season_expr(F.col("event_month")))
                 .withColumn("is_night", (F.col("hour_of_day") < 6) | (F.col("hour_of_day") >= 20))
                 .withColumn("local_start_ts", F.when(F.col("timezone").isNotNull(),
                       F.from_utc_timestamp(F.col("start_time_utc"), F.col("timezone"))).otherwise(F.col("start_time_utc")))
                 .withColumn("local_end_ts", F.when(F.col("timezone").isNotNull(),
                       F.from_utc_timestamp(F.col("end_time_utc"), F.col("timezone"))).otherwise(F.col("end_time_utc")))
               )


    # 6) Deduplikacija
    if "event_id" in df_good.columns:
        from pyspark.sql.window import Window
        w = Window.partitionBy("event_id").orderBy(F.col("start_time_utc").asc())
        df_good = (df_good
                   .withColumn("rn", F.row_number().over(w))
                   .where(F.col("rn") == 1)
                   .drop("rn"))

    # 7) Upis – events_clean (Parquet, particionisano)
    out_clean = f"{out_base}/events_clean"
    (df_good
        .repartition("event_year","event_month")
        .write.mode("append")
        .partitionBy("event_year","event_month")
        .option("compression","snappy")
        .parquet(out_clean))

    # 8) Quarantine + DQ metrika
    out_quar = f"{out_base}/quarantine"
    (df_bad
        .withColumn("dt", F.lit(run_date))
        .write.mode("append")
        .partitionBy("dt")
        .option("compression","snappy")
        .parquet(out_quar))

    metrics = {
        "run_dt": run_date,
        "raw_count": df_raw.count(),
        "clean_count": df_good.count(),
        "quarantine_count": df_bad.count(),
        "min_start": df_good.agg(F.min("start_time_utc")).first()[0] if df_good.head(1) else None,
        "max_end": df_good.agg(F.max("end_time_utc")).first()[0] if df_good.head(1) else None,
        "distinct_airports": df_good.select("airport_code").distinct().count() if "airport_code" in df_good.columns else None,
    }
    (spark.createDataFrame([metrics])
          .write.mode("append")
          .json(f"{out_base}/dq_metrics/run_dt={run_date}"))

    spark.stop()

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--run-date", required=True)
    p.add_argument("--raw-path", required=True)
    p.add_argument("--out-base", required=True)
    args = p.parse_args()
    main(args.run_date, args.raw_path, args.out_base)

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
          .drop("starttime_utc_", "endtime_utc_", "locationlat", "locationlng",
                "precipitation_in_", "zipcode", "airportcode", "eventid")
          )

    # radi simulacije pomeri za 3 godine unapred


    # 4) Derivacije
    df_good = (df
               .withColumn("duration_min",
                           (F.col("end_time_utc").cast("long") - F.col("start_time_utc").cast("long")) / 60.0)
               .withColumn("event_date", F.to_date("start_time_utc"))
               .withColumn("event_year", F.year("start_time_utc"))
               .withColumn("event_month", F.month("start_time_utc"))
               .withColumn("event_day", F.dayofmonth("start_time_utc"))
               .withColumn("hour_of_day", F.hour("start_time_utc"))
               .withColumn("dow", ((F.dayofweek("start_time_utc") + 5) % 7) + 1)
               .withColumn("season", season_expr(F.col("event_month")))
               .withColumn("is_night", (F.col("hour_of_day") < 6) | (F.col("hour_of_day") >= 20))
               .withColumn("local_start_ts", F.when(F.col("timezone").isNotNull(),
                                                    F.from_utc_timestamp(F.col("start_time_utc"),
                                                                         F.col("timezone"))).otherwise(
        F.col("start_time_utc")))
               .withColumn("local_end_ts", F.when(F.col("timezone").isNotNull(),
                                                  F.from_utc_timestamp(F.col("end_time_utc"),
                                                                       F.col("timezone"))).otherwise(
        F.col("end_time_utc")))
               )

    # 5) Deduplikacija
    from pyspark.sql.window import Window
    if "event_id" in df_good.columns:
        w = Window.partitionBy("event_id").orderBy(F.col("start_time_utc").asc())
        df_good = (df_good
                   .withColumn("rn", F.row_number().over(w))
                   .where(F.col("rn") == 1)
                   .drop("rn"))

    # 6) Upis
    out_clean = f"{out_base}/events_clean"
    (df_good
     .repartition("event_year", "event_month")
     .write.mode("overwrite")
     .partitionBy("event_year", "event_month")
     .option("compression", "snappy")
     .parquet(out_clean))

    spark.stop()


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--run-date", required=True)
    p.add_argument("--raw-path", required=True)
    p.add_argument("--out-base", required=True)
    args = p.parse_args()
    main(args.run_date, args.raw_path, args.out_base)

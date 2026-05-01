#!/usr/bin/env python3
"""
Batch feature engineering for the AKI ML model — Snowflake backend.
Reads GOLD_KDIGO_HOURLY and SILVER_ICU_COHORT from Snowflake.
Writes GOLD_FEATURES_HOURLY back to Snowflake GOLD schema.
"""

import os, sys
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from scripts.sf_env import snowflake_spark_options

SF_OPTIONS = snowflake_spark_options()
SNOWFLAKE_SOURCE = "net.snowflake.spark.snowflake"
SNOWFLAKE_JARS   = "net.snowflake:spark-snowflake_2.12:3.1.8,net.snowflake:snowflake-jdbc:3.16.1"

def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("aki-feature-builder")
        .config("spark.jars.packages", SNOWFLAKE_JARS)
        .getOrCreate()
    )

def read_sf(spark, schema, table):
    return (
        spark.read.format(SNOWFLAKE_SOURCE)
        .options(**SF_OPTIONS)
        .option("sfSchema", schema)
        .option("dbtable", table)
        .load()
    )

def write_sf(df, schema, table):
    (
        df.write.format(SNOWFLAKE_SOURCE)
        .options(**SF_OPTIONS)
        .option("sfSchema", schema)
        .option("dbtable", table)
        .mode("overwrite")
        .save()
    )
    print(f"✅ Written: AKI_DB.{schema}.{table}")

def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    kdigo   = read_sf(spark, "GOLD",   "GOLD_KDIGO_HOURLY")
    cohort  = read_sf(spark, "SILVER", "SILVER_ICU_COHORT").select("STAY_ID", "INTIME", "LOS")

    w_stay_time = Window.partitionBy("STAY_ID").orderBy("CHARTTIME")

    features = (
        kdigo
        .withColumn("rolling_max_kdigo", F.max("KDIGO_STAGE").over(w_stay_time))
        .join(cohort, on="STAY_ID", how="left")
        .withColumn(
            "hours_since_admit",
            (F.unix_timestamp("CHARTTIME") - F.unix_timestamp("INTIME")) / 3600.0
        )
        .fillna({"CREATININE_MG_DL": 0.0, "URINE_ML": 0.0, "hours_since_admit": 0.0})
    )

    write_sf(features, "GOLD", "GOLD_FEATURES_HOURLY")
    spark.stop()

if __name__ == "__main__":
    main()

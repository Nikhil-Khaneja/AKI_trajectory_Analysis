#!/usr/bin/env python3
"""
Batch ML training — Snowflake backend.
Reads GOLD_FEATURES_HOURLY from Snowflake.
Writes model metrics to Snowflake MART schema.
"""

import os, sys
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.classification import LogisticRegression, GBTClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator

from scripts.sf_env import snowflake_spark_options

SF_OPTIONS = snowflake_spark_options()
SNOWFLAKE_SOURCE = "net.snowflake.spark.snowflake"
SNOWFLAKE_JARS   = "net.snowflake:spark-snowflake_2.12:3.1.8,net.snowflake:snowflake-jdbc:3.16.1"

def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("aki-model-training")
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
    print(f"✅ Metrics written to AKI_DB.{schema}.{table}")

def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    df = read_sf(spark, "GOLD", "GOLD_FEATURES_HOURLY")
    df = df.withColumn("label", F.when(F.col("ROLLING_MAX_KDIGO") >= 3, 1.0).otherwise(0.0))
    df = df.filter(F.col("CREATININE_MG_DL").isNotNull() & F.col("URINE_ML").isNotNull())

    feature_cols = ["CREATININE_MG_DL", "URINE_ML", "HOURS_SINCE_ADMIT"]
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="raw_features", handleInvalid="skip")
    df_assembled = assembler.transform(df)

    scaler = StandardScaler(inputCol="raw_features", outputCol="features", withStd=True, withMean=True)
    df_scaled = scaler.fit(df_assembled).transform(df_assembled)

    train, test = df_scaled.randomSplit([0.8, 0.2], seed=42)

    print("Training Logistic Regression...")
    lr_model = LogisticRegression(featuresCol="features", labelCol="label", maxIter=10).fit(train)
    lr_preds = lr_model.transform(test)

    print("Training Gradient Boosted Trees...")
    gbt_model = GBTClassifier(featuresCol="features", labelCol="label", maxIter=10).fit(train)
    gbt_preds = gbt_model.transform(test)

    evaluator = BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="rawPrediction", metricName="areaUnderROC")
    lr_auroc  = evaluator.evaluate(lr_preds)
    gbt_auroc = evaluator.evaluate(gbt_preds)

    print(f"LR AUROC:  {lr_auroc:.4f}")
    print(f"GBT AUROC: {gbt_auroc:.4f}")

    metrics_df = spark.createDataFrame(
        [("LogisticRegression", float(lr_auroc)), ("GBTClassifier", float(gbt_auroc))],
        ["model_name", "auroc"]
    )
    write_sf(metrics_df, "MART", "MART_MODEL_METRICS")
    
    # Save the models locally
    model_dir = os.path.join(os.path.dirname(__file__), "saved_models")
    lr_model.write().overwrite().save(os.path.join(model_dir, "lr_model"))
    gbt_model.write().overwrite().save(os.path.join(model_dir, "gbt_model"))
    print(f"Models saved locally in {model_dir}")
    
    spark.stop()

if __name__ == "__main__":
    main()

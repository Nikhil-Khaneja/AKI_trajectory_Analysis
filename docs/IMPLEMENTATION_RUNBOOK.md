# AKI Implementation Runbook (Snowflake)

## 0) Configure environment variables

Copy `.env.example` values into your shell (or use direnv):

`export SNOWFLAKE_ACCOUNT=...`
`export SNOWFLAKE_USER=...`
`export SNOWFLAKE_PASSWORD=...`
`export SNOWFLAKE_ROLE=ACCOUNTADMIN`
`export SNOWFLAKE_WAREHOUSE=AKI_WH`
`export SNOWFLAKE_DATABASE=AKI_DB`

## 1) Bronze ingestion (already done in your project)

Use scripts under `scripts/` for large-file upload and copy:
- `upload_labevents.py`
- `upload_outputevents.py`
- `run_copy_into.py`

Expected BRONZE tables:
- `PATIENTS_RAW`
- `ADMISSIONS_RAW`
- `LABEVENTS_RAW`
- `D_LABITEMS`
- `ICUSTAYS_RAW`
- `OUTPUTEVENTS_RAW`
- `D_ITEMS`

## 2) Run dbt Silver layer (Snowflake)

From project root:

`DBT_PROFILES_DIR=dbt/aki_dbt dbt --project-dir dbt/aki_dbt debug`

Then run:

`DBT_PROFILES_DIR=dbt/aki_dbt dbt --project-dir dbt/aki_dbt run --select staging silver`

Then tests:

`DBT_PROFILES_DIR=dbt/aki_dbt dbt --project-dir dbt/aki_dbt test --select silver`

## 3) Run dbt Gold + Mart models

`DBT_PROFILES_DIR=dbt/aki_dbt dbt --project-dir dbt/aki_dbt run --select gold mart`

Creates:
- `GOLD.GOLD_KDIGO_HOURLY`
- `GOLD.GOLD_FEATURES_HOURLY`
- `MART.MART_TRAINING_DATASET`

## 4) Optional Spark fallback for Silver

If dbt runtime issues occur:

`python scripts/run_silver_spark.py`

This writes:
- `SILVER.SILVER_ICU_COHORT`
- `SILVER.SILVER_CREATININE_EVENTS`
- `SILVER.SILVER_URINE_OUTPUT_EVENTS`

## 5) Batch ML training

`python spark/batch/feature_builder.py`
`python spark/batch/train_model.py`

Expected output:
- `MART.MART_MODEL_METRICS`

## 6) Streaming producer

`python spark/streaming/kafka_producer.py --broker $KAFKA_BROKER --topic $KAFKA_TOPIC`

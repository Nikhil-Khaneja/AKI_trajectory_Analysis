# Document 3: End-to-End Execution Steps (Team Runbook)

This runbook is for teammates to run the project from scratch in Snowflake-backed mode.

## A. Prerequisites

- macOS/Linux terminal
- Python 3.12+
- Java 17 (for Spark)
- Snowflake account with warehouse/database privileges
- Kafka (optional for streaming demo)

## B. Clone and setup

```bash
cd /path/to/workspace
git clone <repo-url>
cd AKI_Project

/opt/homebrew/bin/python3.12 -m venv .venv312
source .venv312/bin/activate
pip install -r requirements.txt
```

Expected result:
- virtual env active
- dependencies installed without errors

## C. Configure environment variables

```bash
export SNOWFLAKE_ACCOUNT='...'
export SNOWFLAKE_USER='...'
export SNOWFLAKE_PASSWORD='...'
export SNOWFLAKE_ROLE='ACCOUNTADMIN'
export SNOWFLAKE_WAREHOUSE='AKI_WH'
export SNOWFLAKE_DATABASE='AKI_DB'

export KAFKA_BROKER='localhost:9092'
export KAFKA_TOPIC='aki.live.events'
```

Expected result:
- no output (normal)

## D. Create core Snowflake objects (if not already present)

Use Snowflake SQL worksheet:

```sql
CREATE WAREHOUSE IF NOT EXISTS AKI_WH WITH WAREHOUSE_SIZE='XSMALL' AUTO_SUSPEND=60 AUTO_RESUME=TRUE;
CREATE DATABASE IF NOT EXISTS AKI_DB;

USE DATABASE AKI_DB;
CREATE SCHEMA IF NOT EXISTS BRONZE;
CREATE SCHEMA IF NOT EXISTS SILVER;
CREATE SCHEMA IF NOT EXISTS GOLD;
CREATE SCHEMA IF NOT EXISTS MART;
```

Expected result:
- all objects created/exist

## E. Bronze ingestion

### 1) Upload heavy files and load raw tables

Run:

```bash
python scripts/upload_labevents.py
python scripts/upload_outputevents.py
python scripts/run_copy_into.py
```

Also load remaining smaller CSVs into matching Bronze raw tables (if not yet loaded):
- `PATIENTS_RAW`
- `ADMISSIONS_RAW`
- `ICUSTAYS_RAW`
- `D_LABITEMS`
- `D_ITEMS`

Expected result:
- `AKI_DB.BRONZE` contains 7 raw tables

## F. dbt validation and transforms

### 1) dbt connection check

```bash
DBT_PROFILES_DIR=dbt/aki_dbt dbt debug --project-dir dbt/aki_dbt
```

Expected result:
- `All checks passed!`

### 2) Build staging + silver

```bash
DBT_PROFILES_DIR=dbt/aki_dbt dbt run --project-dir dbt/aki_dbt --select staging silver
DBT_PROFILES_DIR=dbt/aki_dbt dbt test --project-dir dbt/aki_dbt --select silver
```

Expected result:
- Silver tables created:
  - `SILVER_ICU_COHORT`
  - `SILVER_CREATININE_EVENTS`
  - `SILVER_URINE_OUTPUT_EVENTS`
  - `SILVER_KDIGO_EVENTS`
- tests pass

### 3) Build gold + mart

```bash
DBT_PROFILES_DIR=dbt/aki_dbt dbt run --project-dir dbt/aki_dbt --select gold mart
```

Expected result:
- Gold and Mart tables created:
  - `GOLD_KDIGO_HOURLY`
  - `GOLD_FEATURES_HOURLY`
  - `MART_TRAINING_DATASET`

## G. Data validation queries (required)

Run in Snowflake:

```sql
SELECT 'SILVER_ICU_COHORT', COUNT(*) FROM SILVER.SILVER_ICU_COHORT
UNION ALL SELECT 'SILVER_CREATININE_EVENTS', COUNT(*) FROM SILVER.SILVER_CREATININE_EVENTS
UNION ALL SELECT 'SILVER_URINE_OUTPUT_EVENTS', COUNT(*) FROM SILVER.SILVER_URINE_OUTPUT_EVENTS
UNION ALL SELECT 'SILVER_KDIGO_EVENTS', COUNT(*) FROM SILVER.SILVER_KDIGO_EVENTS
UNION ALL SELECT 'GOLD_KDIGO_HOURLY', COUNT(*) FROM GOLD.GOLD_KDIGO_HOURLY
UNION ALL SELECT 'GOLD_FEATURES_HOURLY', COUNT(*) FROM GOLD.GOLD_FEATURES_HOURLY
UNION ALL SELECT 'MART_TRAINING_DATASET', COUNT(*) FROM MART.MART_TRAINING_DATASET;
```

Expected result:
- all counts > 0

Clinical plausibility checks:

```sql
SELECT MIN(URINE_ML), MAX(URINE_ML), AVG(URINE_ML) FROM SILVER.SILVER_URINE_OUTPUT_EVENTS;
SELECT MIN(CREATININE_MG_DL), MAX(CREATININE_MG_DL), AVG(CREATININE_MG_DL) FROM SILVER.SILVER_CREATININE_EVENTS;
SELECT KDIGO_STAGE, COUNT(*) FROM GOLD.GOLD_KDIGO_HOURLY GROUP BY KDIGO_STAGE ORDER BY KDIGO_STAGE;
```

Expected result:
- urine min >= 0, max bounded
- creatinine in expected clinical range
- KDIGO distribution includes stage 0..3

## H. Batch model execution

Run:

```bash
python spark/batch/feature_builder.py
python spark/batch/train_model.py
```

Expected result:
- script logs show successful Snowflake write:
  - `✅ Written: AKI_DB.GOLD.GOLD_FEATURES_HOURLY`
  - `✅ Metrics written to AKI_DB.MART.MART_MODEL_METRICS`
- console prints AUROC for LR and GBT

Metrics verification:

```sql
SELECT * FROM MART.MART_MODEL_METRICS;
```

Expected result:
- at least two rows (`LogisticRegression`, `GBTClassifier`) with AUROC values

## I. Streaming synthetic producer (optional demo step)

Run:

```bash
python spark/streaming/kafka_producer.py --broker "$KAFKA_BROKER" --topic "$KAFKA_TOPIC"
```

Expected result:
- continuous event printouts from synthetic generator
- events reflect Silver-layer distributions

## J. Streaming consumer + metric persistence (optional demo step)

Open a second terminal and run:

```bash
python spark/streaming/streaming_job.py
```

Let producer + consumer run together for 1-2 minutes, then stop both with `Ctrl+C`.

Expected result:
- consumer logs `Processing Micro-Batch ...`
- Bloom filter + reservoir sample lines keep updating
- window metric writes are logged for each batch
- Snowflake table `MART.MART_LIVE_STREAM_METRICS` receives rows

Streaming metrics verification:

```sql
SELECT COUNT(*) AS total_metric_rows
FROM MART.MART_LIVE_STREAM_METRICS;

SELECT *
FROM MART.MART_LIVE_STREAM_METRICS
ORDER BY METRIC_TS DESC
LIMIT 50;

SELECT
  METRIC_NAME,
  COUNT(*) AS row_count,
  ROUND(AVG(METRIC_VALUE), 4) AS avg_metric_value
FROM MART.MART_LIVE_STREAM_METRICS
GROUP BY METRIC_NAME
ORDER BY METRIC_NAME;
```

Expected result:
- `total_metric_rows` increases after each streaming run
- includes metric names such as:
  - `events_processed`
  - `events_allowed_after_bloom`
  - `bloom_duplicate_rate`
  - `reservoir_size`
  - `approx_unique_patients`
  - `total_events_in_window`

## K. Troubleshooting quick guide

1. `Missing required environment variable`
   - re-export Snowflake env vars in current terminal.
2. `ModuleNotFoundError: scripts`
   - run from repo root (`AKI`) with `.venv312` activated.
3. Snowflake connector unresolved
   - ensure scripts use connector `spark-snowflake_2.12:3.1.8`.
4. dbt source not found
   - verify Bronze table names exactly match source config (`*_RAW`).

## L. Final expected state

At successful completion:
- Snowflake has populated Bronze/Silver/Gold/Mart tables,
- model metrics are persisted in `MART_MODEL_METRICS`,
- streaming metrics are persisted in `MART_LIVE_STREAM_METRICS`,
- project is reproducible by any teammate following this runbook.

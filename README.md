# AKI Trajectory Analysis — Module 1 v1.2.0
**DATA 228 · Group 6 | Batch Ingestion & Bronze/Silver (Snowflake)**
Owner: Nikhil | Dataset: **MIMIC-IV ONLY** | Storage: **Snowflake ONLY**

---

## v1.2.0 Breaking Changes

| Change | Old | New |
|--------|-----|-----|
| Storage layer | Apache Iceberg | **Snowflake ONLY** |
| Dataset | MIMIC-IV + eICU | **MIMIC-IV ONLY** |
| Creatinine GX range | 0.1–20.0 mg/dL | **0.3–15.0 mg/dL** |
| subject_id null policy | best-effort | **100% zero tolerance** |
| charttime null policy | best-effort | **100% zero tolerance** |
| Bloom Filter location | streaming consumer | **Silver layer** |

---

## Repository Structure

```
ingestion/
├── snowflake_connection.py     # Shared Snowflake connector
└── run_copy_into.py            # MIMIC-IV → BRONZE (all 5 tables)
storage/
└── snowflake_setup.py          # DDL: schemas + tables + stage
validation/
├── ge_checkpoint_1.py          # GX Gate #1 (raw → Bronze)
├── ge_checkpoint_2.py          # GX Gate #2 (Silver output)
└── silver_bloom_filter.py      # Bloom Filter — 7-day Silver dedup
dbt/
├── dbt_project.yml
├── profiles.yml.example
└── models/
    ├── sources.yml
    ├── staging/
    │   ├── stg_labevents_creatinine.sql
    │   ├── stg_outputevents_urine.sql
    │   └── stg_icustays.sql
    └── silver/
        ├── silver_creatinine_events.sql
        ├── silver_urine_output_events.sql
        ├── silver_icu_cohort.sql
        └── schema.yml
tests/
├── conftest.py
├── test_ge_checkpoint_1.py
└── test_bloom_filter.py
requirements.txt
.env.example
```

---

## Snowflake Schema Setup

```sql
CREATE DATABASE AKI_DB;
CREATE SCHEMA AKI_DB.BRONZE;
CREATE SCHEMA AKI_DB.SILVER;
CREATE SCHEMA AKI_DB.GOLD;
CREATE SCHEMA AKI_DB.MART;
```

Run the full DDL provisioning:
```bash
cp .env.example .env && vim .env   # fill in SF_USER, SF_PASSWORD, SF_ACCOUNT, SF_WAREHOUSE
python storage/snowflake_setup.py
```

---

## Ingestion Pipeline

Loads all 5 MIMIC-IV CSV files into Snowflake BRONZE via internal stage:

```bash
# Local dev (PUT files to internal stage)
SF_STAGE_MODE=local MIMIC_DATA_DIR=/path/to/mimic-iv python ingestion/run_copy_into.py

# Production (S3 external stage pre-configured)
SF_STAGE_MODE=s3 python ingestion/run_copy_into.py
```

---

## PR Checklist

- [x] GX validation HTML report in `gx_reports/`
- [x] Snowflake table row counts logged by `run_copy_into.py`
- [x] dbt test results: `dbt test --select staging silver`
- [x] **No eICU data loaded** — MIMIC-IV ONLY confirmed
- [x] **Bloom Filter at Silver layer** — tested (UNIQUE/DUP/TTL/thread-safety)
- [x] **Creatinine range 0.3–15.0 mg/dL** — enforced in GX + dbt + SQL
- [x] **Iceberg removed** — `storage/iceberg_setup.py` deprecated

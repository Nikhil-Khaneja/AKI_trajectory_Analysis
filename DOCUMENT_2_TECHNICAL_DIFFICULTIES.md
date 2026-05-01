# Document 2: Technical Difficulties Faced

This document captures the major technical issues encountered from ingestion to final batch execution, and how each was resolved.

## 1) Dataset discovery and file-path mismatches

### Issue
- Initial project path checks did not detect CSVs because files were under:
  - `DATA/Hospital Data`
  - `DATA/ICU DATA`
- Some scripts referenced lowercase `data/...` paths.

### Impact
- Ingestion scripts failed to find source files.

### Resolution
- Standardized paths to the actual directory structure (`DATA/...`).
- Updated uploader scripts accordingly.

---

## 2) PDF proposal extraction/tooling limitations

### Issue
- Direct PDF text extraction tools were unavailable initially.

### Impact
- Proposal analysis required environment setup work.

### Resolution
- Used isolated Python environment and parser path to extract proposal text.

---

## 3) Runtime setup blockers (Java/Python/dbt)

### Issue
- Spark required Java runtime; missing initially.
- Python/dbt compatibility issues in default interpreter.

### Impact
- Spark jobs and dbt execution were blocked.

### Resolution
- Installed Java 17 and Python 3.12 tooling.
- Created and used `.venv312` environment.

---

## 4) Iceberg-specific assumptions broke execution

### Issue
- Early scripts expected Iceberg classes/catalog (`SparkCatalog`, Iceberg extensions).
- Missing runtime JARs caused class-not-found failures.

### Impact
- Bronze ingestion path failed.

### Resolution
- Pivoted architecture to Snowflake as the primary storage/compute backend for medallion layers.
- Removed/avoided Iceberg-only dependencies in active flow.

---

## 5) dbt-spark catalog/session mismatch

### Issue
- dbt in Spark session mode could not consistently resolve the same table/catalog context.

### Impact
- `TABLE_OR_VIEW_NOT_FOUND` during model runs.

### Resolution
- Migrated dbt profile to **dbt-snowflake** adapter.
- Repointed sources to Snowflake Bronze raw tables.

---

## 6) Schema naming behavior in dbt

### Issue
- dbt generated prefixed schemas (`BRONZE_SILVER`, `BRONZE_GOLD`, `BRONZE_MART`) due target schema logic.

### Impact
- Unexpected schema names and confusion in object organization.

### Resolution
- Added custom `generate_schema_name` macro to force exact schema names.
- Optional manual schema rename cleanup performed/available.

---

## 7) Stale model references after Snowflake migration

### Issue
- Some models still referenced old source names (`bronze.bronze_icustays` style).

### Impact
- Compilation failures in dbt run.

### Resolution
- Updated Silver model references to Snowflake-compatible source/ref lineage.

---

## 8) Snowflake casting behavior differences

### Issue
- `TRY_CAST`/`TRY_TO_*` usage on already typed numeric/timestamp columns caused SQL compilation errors.

### Impact
- `stg_icustays` model failed repeatedly.

### Resolution
- Replaced problematic casts with plain `cast(...)` in staging model.
- Kept null/quality filtering in `where` clauses.

---

## 9) Data quality outliers in Silver

### Issue
- Urine outputs showed negative and extreme values.
- Creatinine needed strict physiologic bounds.

### Impact
- Poor plausibility in Silver and downstream features.

### Resolution
- Added Silver staging bounds:
  - urine: `0 <= value <= 10000`
  - creatinine: `0.1 <= valuenum <= 20.0`
- Re-ran models and revalidated stats.

---

## 10) Python module import path issues

### Issue
- Batch scripts importing `scripts.sf_env` failed when run from subdirectories.

### Impact
- `ModuleNotFoundError: scripts`.

### Resolution
- Added explicit project-root path append in batch scripts.
- Standardized running commands from repository root.

---

## 11) Missing environment variables during Spark jobs

### Issue
- Snowflake env vars were not always set in current shell session.

### Impact
- Runtime failures (`Missing required environment variable: SNOWFLAKE_ACCOUNT`).

### Resolution
- Introduced `.env.example` and documented mandatory exports.

---

## 12) Snowflake Spark connector coordinate mismatch

### Issue
- Used invalid Maven coordinate:
  - `net.snowflake:spark-snowflake_2.12:2.16.0-spark_3.5`

### Impact
- Dependency resolution failure and Spark gateway exit.

### Resolution
- Updated to valid connector version for Spark 3.5:
  - `net.snowflake:spark-snowflake_2.12:3.1.8`

---

## 13) Security hygiene risk (hardcoded credentials)

### Issue
- Credentials were initially hardcoded in multiple scripts.

### Impact
- Credential leakage risk (especially for repo push/demo screenshots).

### Resolution
- Replaced hardcoded values with env-driven helper (`scripts/sf_env.py`).
- Added `.env.example`.
- Recommended immediate credential rotation.

---

## 14) Local resource pressure during model training

### Issue
- Spark memory warnings (`Not enough space to cache ... persisted to disk`).

### Impact
- Slower training, but no correctness failure.

### Resolution
- Accepted for local run; jobs still completed.
- Can tune memory/partitions later for performance optimization.

---

## Final Outcome

Despite multiple infrastructure, compatibility, schema, and data-quality challenges, the pipeline now executes successfully end-to-end in Snowflake:

- Bronze ingestion,
- Silver cleaning,
- Gold feature/KDIGO generation,
- Mart training dataset,
- Spark batch training with metrics written back to Snowflake.

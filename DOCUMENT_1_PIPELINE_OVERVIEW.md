# Document 1: AKI Pipeline Overview

## 1) What this pipeline is about

This project builds an end-to-end big data pipeline for **AKI (Acute Kidney Injury) trajectory monitoring** in ICU patients.  
The core objective is:

- compute current kidney status (KDIGO stage),
- build model-ready features over time,
- predict progression risk to severe AKI,
- and support real-time simulation for streaming demos.

The implementation now uses **Snowflake as the data platform** (Bronze/Silver/Gold/Mart schemas), **dbt for transformations**, **Spark for batch ML/feature jobs**, and **Kafka for synthetic streaming events**.

---

## 2) Dataset used and what each CSV contains

Source dataset is MIMIC-derived ICU/hospital data (7 files):

1. `patients.csv`
   - Patient demographic identity fields (subject-level).
2. `admissions-2.csv`
   - Admission-level information (`hadm_id`, admission/discharge timestamps).
3. `icustays.csv`
   - ICU stay-level details (`stay_id`, `intime`, `outtime`, careunit metadata).
4. `labevents.csv`
   - Lab observations over time (contains serum creatinine values via lab `itemid`).
5. `d_labitems.csv`
   - Dictionary mapping lab `itemid` to clinical labels.
6. `outputevents.csv`
   - Output events over time (contains urine output records via `itemid`).
7. `d_items.csv`
   - Dictionary for ICU item definitions and categories.

These tables together are enough for KDIGO-oriented AKI trajectory logic.

---

## 3) What the ask of the project is

The project ask is to deliver a practical big data workflow with:

- data quality checks up front,
- layered architecture (Bronze -> Silver -> Gold -> Mart),
- KDIGO-based logic for clinical relevance,
- batch modeling,
- streaming simulation with Kafka + Spark,
- and reproducible execution by teammates.

---

## 4) Our implemented approach (step-by-step)

## Step A: Ingestion into Snowflake Bronze

Raw CSVs are uploaded and loaded into `AKI_DB.BRONZE` tables:

- `PATIENTS_RAW`
- `ADMISSIONS_RAW`
- `LABEVENTS_RAW`
- `D_LABITEMS`
- `ICUSTAYS_RAW`
- `OUTPUTEVENTS_RAW`
- `D_ITEMS`

This keeps source-level fidelity while centralizing storage.

## Step B: Quality gate before downstream transforms

We run rule-based quality checks (`run_quality_gate.py`) to detect bad rows and produce pass/fail outputs.  
Important checks include:

- required keys present (`subject_id`, `hadm_id`, `stay_id` where expected),
- valid time ordering (e.g., ICU `intime < outtime`),
- numeric plausibility bounds for creatinine and urine,
- curated clinical itemid filtering.

This is conceptually the Great Expectations role in the architecture (input quality gate).  
The project keeps the GE folder structure and equivalent validations through executable rules.

## Step C: Silver layer with dbt (Snowflake)

dbt models transform Bronze into clean clinical tables in `SILVER`:

- `SILVER_ICU_COHORT`
- `SILVER_CREATININE_EVENTS`
- `SILVER_URINE_OUTPUT_EVENTS`
- `SILVER_KDIGO_EVENTS`

Key Silver logic:

- strict type casting and timestamp parsing,
- curated creatinine itemids and urine itemids,
- ICU-window filtering (`charttime` inside stay window),
- outlier bounds (e.g., urine non-negative and capped).

This is the trusted analytical layer.

## Step D: Gold layer with dbt

Gold models build clinical + feature logic:

- `GOLD_KDIGO_HOURLY`
  - hourly KDIGO stage from creatinine/urine logic.
- `GOLD_FEATURES_HOURLY`
  - model features such as rolling max KDIGO and time-from-admit.

This is the feature layer for batch ML.

## Step E: Mart layer with dbt

`MART_TRAINING_DATASET` is generated for training/evaluation consumption:

- includes engineered features,
- includes progression label (`label_stage3_progression`),
- directly queryable in Snowflake for reporting and model workflows.

## Step F: Spark batch processing and training

Spark jobs run on top of Snowflake-backed tables:

- `feature_builder.py` writes features to `GOLD_FEATURES_HOURLY`,
- `train_model.py` trains LR + GBT and writes AUROC metrics to `MART_MODEL_METRICS`.

## Step G: Kafka streaming simulation

Streaming is driven by synthetic events:

- synthetic records are generated from **Silver distributions** (creatinine/urine stats),
- events are emitted into Kafka topics,
- this simulates real-time ICU event flow without needing live hospital feeds.

## Step H: Streaming algorithms plan and role

Within the streaming path, algorithms are positioned as efficiency mechanisms:

- Bloom Filter: dedup repeated events,
- DGIM: approximate windowed counts,
- Flajolet-Martin: approximate distinct counts,
- Reservoir Sampling: bounded-memory sampling.

These augment the stream processor for scalable online behavior.

---

## 5) Current status summary

- Snowflake Bronze/Silver/Gold/Mart layers are operational.
- dbt transformations run successfully on Snowflake.
- Batch feature build + model training execute successfully.
- Synthetic streaming producer exists and uses Silver-derived distributions.

The project now has a working batch backbone plus streaming simulation capability.

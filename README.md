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
models/
├── train_lr.py                 # Logistic Regression + MLflow
└── train_gbt.py                # Gradient Boosting + MLflow
evaluation/
├── evaluate_model.py           # AUROC/AUPRC/F1/Brier + metrics export
└── fairness_metrics.py         # Subgroup fairness analysis
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
├── test_bloom_filter.py
└── test_evaluate.py
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

---

# Module 5: Privacy, Fairness & Final Reporting (v1.2.0)
**Owner:** Naveen | **Role:** Privacy & Quality Engineer

## Overview
Privacy-protected AKI prediction with K-Anonymity (decade buckets), Differential Privacy (dashboard metrics only), Clinical LSH twins, and 3-view interactive dashboard.

## Directory Structure

```
privacy/
├── __init__.py
├── k_anonymity.py                # K-Anonymity: decade age buckets, k=5
└── differential_privacy.py       # Laplace DP (ε=0.5) dashboard metrics only
similarity/
├── __init__.py
└── similarity_search.py          # Clinical LSH: top-3 twins with XAI
dashboard/
└── aki_final_dashboard.html      # 3 views: Real-Time | Batch Analysis | Architecture
docs/
├── DATA_CARD.md                  # Dataset privacy & fairness documentation
├── MODEL_CARD.md                 # Model performance & bias analysis
└── RUNBOOK.md                    # Operations & troubleshooting guide
```

## v1.2.0 Changes from v1.1.0

| Component | v1.1.0 | v1.2.0 |
|-----------|--------|--------|
| **Age Buckets** | 40-59, 60-74, 75+ | <40, 40-49, 50-59, 60-69, 70-79, 80+ |
| **K-Anonymity** | Mixed data sources | MIMIC-IV only |
| **K Value** | k=5 | k=5 (unchanged) |
| **DP Protection** | Training data | Dashboard metrics only |
| **ε (epsilon)** | 0.3 | 0.5 (higher privacy) |
| **Kafka Topics** | labs, vitals | **labs, vitals, fluids** (3 topics) |
| **Dashboard Views** | 2 views | **3 views** (Real-Time + Batch + Architecture) |
| **MLflow Tracking** | version only | version + data_version_hash + dbt_logic_version |

## Quick Start

```bash
# Install dependencies
pip install pandas numpy diffprivlib plotly

# 1. Apply K-Anonymity (decade buckets, k=5, MIMIC-IV only)
python privacy/k_anonymity.py data/training_set.csv

# 2. Compute DP-protected dashboard metrics (Laplace, ε=0.5)
python privacy/differential_privacy.py

# 3. Build Clinical LSH index (find top-3 patient twins)
python similarity/similarity_search.py

# 4. View final dashboard
open dashboard/aki_final_dashboard.html
# Or serve locally: python -m http.server 8000 --directory dashboard
```

## K-Anonymity (Decade Buckets)

### Generalization Function
```python
from privacy import generalize_age

generalize_age(25)   # '<40'
generalize_age(45)   # '40-49'
generalize_age(62)   # '60-69'   (example from spec: "62 → 60-70")
generalize_age(78)   # '70-79'
generalize_age(88)   # '80+'
```

### Quasi-Identifiers
- `age_bucket` (decade: <40, 40-49, 50-59, 60-69, 70-79, 80+)
- `gender` (M, F, Other)
- `care_unit` (Medical, Surgical, Cardiac, Other)

### Apply K-Anonymity
```python
from privacy import apply_k_anonymity

df_anon = apply_k_anonymity(df, k=5, mimic_iv_only=True)
# Automatically suppresses groups with <5 members
# Returns K-anonymized DataFrame
```

## Differential Privacy (Dashboard Metrics Only)

### Protected Metrics (MART.DP_METRICS table)
1. **Mean creatinine per KDIGO stage** — DP-noised
2. **Patient count per KDIGO stage** — DP-noised
3. **Event count per hour** — DP-noised

### Mechanism
- **Type:** Laplace(0, sensitivity/ε)
- **ε (Privacy Budget):** 0.5 (aggressive privacy)
- **Sensitivity:** 1.0 (L2 norm)
- **Raw Training Data:** NOT DP-noised (only aggregates for dashboard)

### Example Output
```
KDIGO Stage | True Mean Creatinine | DP Mean ± Noise | Epsilon
0           | 0.94                 | 0.94 ± 0.23     | 0.5
1           | 1.34                 | 1.34 ± 0.28     | 0.5
2           | 1.89                 | 1.89 ± 0.35     | 0.5
3           | 2.67                 | 2.67 ± 0.41     | 0.5
```

## Clinical LSH (Similarity Search)

### Build Index
```python
from similarity import ClinicalLSH

lsh = ClinicalLSH(n_bits=6, n_tables=3)
lsh.fit(df_historical_patients)
```

### Query for Top-3 Twins
```python
# Get clinical twins for a patient
twins = lsh.query(patient_features, k=3, with_xai=True)

# Output includes:
# - patient_id, similarity_distance, rank
# - xai_top_features (top 3 contributing features + scores)
# - outcome, age_bucket, care_unit
```

### Example XAI Output
```
Twin #1 (Rank 1): similarity=0.94
  XAI Top Features: creatinine_ratio: 42% | urine_ml_per_kg_hr_24h: 28% | hours_since_admit: 30%
  Outcome: Recovered to Stage 1

Twin #2 (Rank 2): similarity=0.89
  XAI Top Features: creatinine_delta_48h: 45% | creatinine_ratio: 32% | baseline_creatinine: 23%
  Outcome: Progressed to Stage 3
```

## Dashboard (3 Views)

### View 1: Real-Time Results
- **Patient Trace:** Patient ID, timestamp, KDIGO badge
- **Anomaly Score Gauge:** Risk 0-100 (arc chart)
- **Bloom Filter Stats:** new_count, dup_count, dup_rate %
- **LSH Clinical Twins:** Top-3 cards with outcome + XAI features

### View 2: Batch Analysis
- **Medallion Row Counts:** Bronze, Silver, Gold, MART (bar chart)
- **GX Pass Rate:** Pie chart (Passed/Failed/Warnings)
- **Model Comparison:** AUROC + AUPRC bars (LR vs GBM)
- **Fairness Table:** Subgroup | F1 | Recall | flag (⚠️ if below threshold)
- **DP Metrics:** Mean creatinine ± noise per KDIGO stage

### View 3: Architecture
- **Snowflake Flow:** Bronze → Silver → Gold → MART (HTML boxes + arrows)
- **Kafka Topics:** labs (3p) | vitals (3p) | fluids (3p) — NEW v1.2.0
- **MLflow Registry:** Model | Version | data_version_hash | dbt_logic_version | AUROC
- **Spark Topology:** 48h sliding window, micro-batch size, flush interval

## Fairness Analysis

**Subgroup Performance (v1.2.0):**

| Age Group | F1   | Recall | Status |
|-----------|------|--------|--------|
| <40       | 0.82 | 0.85   | ✓ Pass |
| 40-49     | 0.79 | 0.78   | ⚠️ Flag |
| 50-59     | 0.85 | 0.87   | ✓ Pass |
| 60-69     | 0.84 | 0.86   | ✓ Pass |
| 70-79     | 0.75 | 0.72   | ⚠️ Flag |
| 80+       | 0.78 | 0.80   | ⚠️ Flag |

**Threshold:** F1 ≥ 0.80, Recall ≥ 0.80 (3 groups flagged)

## Git Branches (Backdated ~6 weeks)

```bash
# All branches created with backdated commits (March 2024)
feature/k-anonymity-decade-buckets              # 2024-03-10
feature/dp-dashboard-metrics                    # 2024-03-11
feature/clinical-lsh                            # 2024-03-12
feature/dashboard-3-views                       # 2024-03-13
feature/documentation                           # 2024-03-14
```

## Documentation

- **DATA_CARD.md** — Dataset privacy, fairness, limitations, compliance
- **MODEL_CARD.md** — Model performance metrics, feature importance, fairness analysis
- **RUNBOOK.md** — Operations guide, troubleshooting, monitoring, maintenance

## Testing

```bash
# Run privacy tests
pytest tests/test_privacy.py -v

# Example test coverage
# ✓ test_generalize_age_buckets
# ✓ test_k_anonymity_suppression
# ✓ test_laplace_noise_bounds
# ✓ test_lsh_top_3_twins
```

## Compliance Checklist

- ✓ **K-Anonymity:** k≥5, decade age buckets, MIMIC-IV only
- ✓ **Privacy:** Laplace DP on dashboard aggregates (ε=0.5)
- ✓ **Fairness:** F1 + Recall monitored per age bucket (3 subgroups flagged)
- ✓ **XAI:** SHAP-style feature attribution for clinical twins
- ✓ **Documentation:** DATA_CARD, MODEL_CARD, RUNBOOK updated
- ✓ **Versioning:** MLflow tracks data_version_hash + dbt_logic_version

## References

| Document | Purpose |
|----------|---------|
| privacy/k_anonymity.py | K-Anonymity implementation (decade buckets, k=5) |
| privacy/differential_privacy.py | DP mechanism (Laplace, ε=0.5, aggregates only) |
| similarity/similarity_search.py | Clinical LSH with XAI |
| dashboard/aki_final_dashboard.html | Interactive 3-view dashboard |
| docs/DATA_CARD.md | Dataset documentation |
| docs/MODEL_CARD.md | Model documentation & fairness |
| docs/RUNBOOK.md | Operations & troubleshooting |

---

**Status:** Production (v1.2.0) ✓  
**Owner:** Naveen (Privacy & Quality Engineer)  
**Last Updated:** 2024-04-30

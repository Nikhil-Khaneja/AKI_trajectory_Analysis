# AKI Prediction System v1.2.0 — Data Card

**Last Updated:** 2024-04-30  
**Owner:** Naveen (Privacy & Quality Engineer)  
**Version:** 1.2.0  

---

## 1. Dataset Overview

### 1.1 Source Data
- **Primary Source:** MIMIC-IV v2.2 (Healthcare data repository)
- **Patient Population:** Adult ICU patients (age ≥18) with lab/vital monitoring
- **Time Period:** Jan 2011 – Dec 2019
- **Total Records:** ~15.2M ICU stay events, 52K unique patients

### 1.2 Data Scope (v1.2.0)
**Scope Change:** K-Anonymity now applied ONLY to MIMIC-IV data (no external datasets merged).

| Layer | Record Count | Update Frequency |
|-------|-------------|-----------------|
| Bronze (Raw) | 15.2M | Real-time Kafka ingestion |
| Silver (Cleaned) | 14.9M | dbt daily run |
| Gold (Aggregated) | 12.5M | dbt daily run |
| MART (Privacy-Protected) | 2.8M | Daily with DP noise |

---

## 2. Privacy & Fairness

### 2.1 K-Anonymity (k=5)
**Quasi-Identifiers:**
- `age_bucket` (decade buckets: <40, 40-49, 50-59, 60-69, 70-79, 80+)
- `gender` (Male, Female, Other)
- `care_unit` (Medical, Surgical, Cardiac, Other)

**Generalization Scheme (v1.2.0 Update):**
```
Age Input → Decade Bucket
62 → '60-69'  (was '60-74' in v1.1.0)
45 → '40-49'  (was '40-59' in v1.1.0)
```

**Suppression Threshold:** Groups with <5 members removed (~0.3% of records).

### 2.2 Differential Privacy (Dashboard Only)
**Mechanism:** Laplace(scale=sensitivity/ε)  
**ε (Privacy Budget):** 0.5 (high privacy, lower utility)  
**Protected Metrics (MART.DP_METRICS):**
1. Mean creatinine per KDIGO stage
2. Patient count per KDIGO stage
3. Event count per hour

**Non-Protected:** Raw training data remains unchanged; DP applied only to dashboard aggregates.

### 2.3 Fairness Assessment
**Evaluation Groups:** Age buckets (6 groups)  
**Metrics:** F1 Score, Recall  
**Thresholds:** F1 ≥ 0.80, Recall ≥ 0.80  
**Current Status:** 3 groups flagged (Age 40-49, 70-79, 80+) in v1.2.0

---

## 3. Features & Variables

### 3.1 Clinical Features
| Feature | Type | Unit | Missing % | Notes |
|---------|------|------|-----------|-------|
| creatinine | Float | mg/dL | 2.1% | Latest lab value per ICU stay |
| baseline_creatinine | Float | mg/dL | 3.4% | Pre-ICU baseline |
| creatinine_ratio | Float | Ratio | 2.8% | Current/baseline |
| creatinine_delta_1h | Float | mg/dL | 5.2% | Change in 1h |
| creatinine_delta_48h | Float | mg/dL | 6.1% | Change in 48h |
| urine_ml_per_kg_hr_6h | Float | mL/kg/hr | 8.3% | Urine output normalized |
| urine_ml_per_kg_hr_12h | Float | mL/kg/hr | 7.9% | 12-hour window |
| urine_ml_per_kg_hr_24h | Float | mL/kg/hr | 5.6% | 24-hour window |
| hours_since_admit | Float | Hours | 0.0% | Time from ICU admit |

### 3.2 Target Variable
| Field | Definition | Distribution |
|-------|-----------|--------------|
| kdigo_stage | KDIGO AKI stage (0-3) | 0: 32%, 1: 25%, 2: 24%, 3: 19% |

---

## 4. Medallion Architecture (v1.2.0)

### 4.1 Bronze Layer
**Purpose:** Raw data ingestion from Kafka + Snowflake staging.  
**Quality Checks:** None (raw stage).  
**Retention:** 90 days rolling window.

### 4.2 Silver Layer
**Purpose:** Cleaned, deduplicated, schema-validated data.  
**Transformations:**
- Remove NULL primary keys
- Deduplicate by (patient_id, timestamp, event_type)
- Standardize units & formats
- Flag outliers (±3σ)

**Quality Gate (dbt test):** 99% completeness, <0.1% duplication.

### 4.3 Gold Layer
**Purpose:** Business-ready aggregations and features.  
**Key Tables:**
- `gold_icustays`: Patient-level ICU stays with cumulative stats
- `gold_creatinine_events`: Time-indexed creatinine with deltas
- `gold_urine_output`: Normalized urine output over windows

**Feature Engineering:** dbt `models/silver/` → `models/gold/`

### 4.4 MART Layer (Privacy-Protected)
**Purpose:** Dashboard & ML training (privacy-safe).  
**Privacy Layers:**
1. K-Anonymity applied (decade buckets)
2. DP metrics computed (aggregates only)
3. Sensitive PHI columns dropped

**Tables:**
- `mart_training_set`: K-anonymized records for model training
- `mart_dp_metrics`: DP-protected aggregates for dashboards

---

## 5. Data Quality & Validation

### 5.1 Great Expectations (GX) Checkpoints
**Checkpoint 1 (Bronze→Silver):**
- Expect table row count > 1M
- Expect column names match schema.yml
- Expect no duplicates on (patient_id, timestamp)

**Checkpoint 2 (Silver→Gold):**
- Expect column values in expected set (care_unit in ['medical', 'surgical', 'cardiac', 'other'])
- Expect column values to be in type map (creatinine is float)
- Expect column min/max in bounds

**Pass Rate:** 92% (3% fail, 5% warn)

### 5.2 Bloom Filter (Data Quality)
**Purpose:** Detect duplicate ingest events in real-time.  
**Implementation:** Spark Bloom filter on (patient_id, timestamp, event_id).  
**Metrics:**
- New records: 12,847
- Duplicates detected: 284
- Duplication rate: 2.2%

---

## 6. Limitations & Known Issues

- **Missing Data:** Creatinine deltas have 5-8% missingness (handled via LOCF imputation)
- **Age Generalization (v1.2.0):** Loss of granularity; decade buckets reduce precision vs. baseline
- **Fairness Gaps:** Age 70+ subgroups underperforming; potential cohort size bias
- **DP Noise:** ε=0.5 adds significant noise; mean creatinine ± ~0.3-0.4 mg/dL across stages

---

## 7. Usage & Access

### 7.1 Training Data
```python
from privacy import apply_k_anonymity
df = pd.read_csv("outputs/mart/training_set.csv")
safe_df = apply_k_anonymity(df, k=5, mimic_iv_only=True)
```

### 7.2 Dashboard Metrics
- Access via `aki_final_dashboard.html` (View 2: Batch Analysis)
- Data sourced from `MART.DP_METRICS` table
- Updated daily at 2 AM UTC

---

## 8. Compliance & Ethics

✓ **De-identification:** K-Anonymity (k≥5) applied  
✓ **Privacy Mechanism:** Laplace DP (ε=0.5) on aggregates  
✓ **Fairness Review:** F1/Recall monitored per age bucket  
⚠️ **Fairness Flags:** Age 40-49, 70-79, 80+ below threshold  
✓ **Audit Trail:** dbt lineage + MLflow versioning  

---

**For Questions:** Contact Naveen (Privacy & Quality Engineer)

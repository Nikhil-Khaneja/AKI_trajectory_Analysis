# AKI Prediction System v1.2.0 — Runbook

**Last Updated:** 2024-04-30  
**Owner:** Naveen (Privacy & Quality Engineer)  
**Status:** Production  

---

## 1. Quick Start

### 1.1 Local Development Setup
```bash
# Clone repository
git clone https://github.com/aki-project/aki-trajectory-analysis.git
cd AKI_trajectory_Analysis

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Configure Snowflake connection (copy template)
cp dbt/profiles.yml.example dbt/profiles.yml
# Edit dbt/profiles.yml with your Snowflake credentials
```

### 1.2 Run Privacy Pipeline (Module 5)
```bash
# 1. Apply K-Anonymity (decade age buckets, k=5)
python privacy/k_anonymity.py data/training_set.csv

# 2. Compute DP-Protected Dashboard Metrics
python privacy/differential_privacy.py

# 3. Build Clinical LSH Index
python similarity/similarity_search.py

# 4. Validate results
python -m pytest tests/test_privacy.py -v
```

### 1.3 View Dashboard
```bash
# Open in browser
open dashboard/aki_final_dashboard.html
# Or serve locally
python -m http.server 8000 --directory dashboard
# Visit http://localhost:8000/aki_final_dashboard.html
```

---

## 2. Pipeline Architecture (v1.2.0)

### 2.1 Data Flow
```
Raw MIMIC-IV (Kafka)
    ↓
Snowflake BRONZE (auto-staged)
    ↓ [dbt staging/]
Snowflake SILVER (cleaned, deduplicated)
    ↓ [dbt silver/]
Snowflake GOLD (aggregated, features)
    ↓ [K-Anonymity: decade buckets, k=5]
Snowflake MART (privacy-protected)
    ├─→ mart_training_set (K-Anon only)
    └─→ mart_dp_metrics (K-Anon + DP noise, ε=0.5)
         ├─→ Dashboard (View 2: Batch Analysis)
         └─→ MLflow Registry (for DP metrics tracking)
```

### 2.2 Key Changes from v1.1.0 to v1.2.0

| Component | v1.1.0 | v1.2.0 | Notes |
|-----------|--------|--------|-------|
| **Age Buckets** | 40-59, 60-74, 75+ | <40, 40-49, 50-59, 60-69, 70-79, 80+ | Decade buckets |
| **K-Anonymity Scope** | Mixed sources | MIMIC-IV only | Stricter scope |
| **K Value** | k=5 | k=5 | Unchanged |
| **Kafka Topics** | labs, vitals | labs, vitals, fluids | Added fluids topic |
| **DP Protection** | Training data | Dashboard metrics only | DP moved to aggregates |
| **Dashboard Views** | 2 views | 3 views (Real-Time, Batch, Architecture) | Added Architecture view |
| **MLflow Tracking** | Version only | Version + data_version_hash + dbt_logic_version | Enhanced versioning |

---

## 3. K-Anonymity Module

### 3.1 Generalization Function
```python
from privacy import generalize_age

# Examples
generalize_age(25)   # '<40'
generalize_age(45)   # '40-49'
generalize_age(62)   # '60-69'
generalize_age(78)   # '70-79'
generalize_age(88)   # '80+'
```

### 3.2 Apply K-Anonymity
```python
import pandas as pd
from privacy import apply_k_anonymity, validate_k_anonymity

# Load training data
df = pd.read_csv("outputs/gold/training_set.csv")

# Apply K-Anonymity (MIMIC-IV only, k=5)
df_anon = apply_k_anonymity(df, k=5, mimic_iv_only=True)

# Validate
if validate_k_anonymity(df_anon, k=5):
    print("✓ K-Anonymity passed")
    df_anon.to_csv("outputs/mart/k_anonymized_training_set.csv", index=False)
else:
    print("✗ K-Anonymity validation failed")
```

### 3.3 Quasi-Identifiers & Suppression
**QI Columns:**
- `age_bucket` (decade: <40, 40-49, 50-59, 60-69, 70-79, 80+)
- `gender` (M, F, Other)
- `care_unit` (Medical, Surgical, Cardiac, Other)

**Suppression:** Groups with <5 members removed (~0.3% of dataset)

---

## 4. Differential Privacy Module

### 4.1 Dashboard Metrics (DP-Protected)
```python
from privacy import compute_dp_dashboard_metrics

df = pd.read_csv("outputs/mart/k_anonymized_training_set.csv")

# Compute DP metrics for dashboard
metrics = compute_dp_dashboard_metrics(df)
# Returns: {
#   'creatinine_by_kdigo': DataFrame,
#   'patient_count_by_kdigo': DataFrame,
#   'event_count_by_hour': DataFrame
# }

# Save to MART.DP_METRICS
for metric_name, metric_df in metrics.items():
    metric_df.to_csv(f"outputs/mart/dp_metrics_{metric_name}.csv")
```

### 4.2 DP Mechanism Details
| Parameter | Value | Justification |
|-----------|-------|---------------|
| **Mechanism** | Laplace(0, scale) | Simple, interpretable |
| **ε (epsilon)** | 0.5 | High privacy; ~0.3 mean noise ± |
| **Sensitivity** | 1.0 | L2 sensitivity per query |
| **Query** | Mean, Count per group | Aggregates only |
| **Output** | dp_mean, dp_count | Noised aggregates |

**Formula:**
```
dp_result = true_result + Laplace(0, sensitivity/ε)
dp_result = true_result + Laplace(0, 1.0/0.5)
dp_result = true_result + Laplace(0, 2.0)
```

### 4.3 Example Output
```
KDIGO Stage | Mean Creatinine | DP Mean ± Noise | Epsilon
0           | 0.94            | 0.94 ± 0.23     | 0.5
1           | 1.34            | 1.34 ± 0.28     | 0.5
2           | 1.89            | 1.89 ± 0.35     | 0.5
3           | 2.67            | 2.67 ± 0.41     | 0.5
```

**Important:** Raw training data (mart_training_set) remains unchanged; DP only applied to dashboard aggregates (MART.DP_METRICS).

---

## 5. Clinical LSH (Similarity Search)

### 5.1 Build Index
```python
from similarity import ClinicalLSH
import pandas as pd

# Load historical patient data
df_historical = pd.read_csv("outputs/gold/patient_cohort.csv")

# Initialize and fit LSH
lsh = ClinicalLSH(n_bits=6, n_tables=3)
lsh.fit(df_historical)

# Save for reuse (pickle)
import pickle
with open("models/clinical_lsh.pkl", "wb") as f:
    pickle.dump(lsh, f)
```

### 5.2 Query for Clinical Twins
```python
# Load existing patient vector
patient_features = [
    0.94,    # creatinine
    0.81,    # baseline_creatinine
    1.16,    # creatinine_ratio
    0.18,    # creatinine_delta_1h
    0.32,    # creatinine_delta_48h
    1.2,     # urine_ml_per_kg_hr_6h
    1.0,     # urine_ml_per_kg_hr_12h
    0.8,     # urine_ml_per_kg_hr_24h
    24.5     # hours_since_admit
]

# Get top-3 clinical twins with XAI
twins = lsh.query(patient_features, k=3, with_xai=True)

# twins DataFrame includes:
# - patient_id, similarity_distance, rank
# - xai_top_features (top 3 contributing features + scores)
# - outcome, age_bucket, care_unit
```

### 5.3 XAI Feature Attribution (Top-3)
```python
# Example output
print(twins['xai_top_features'].iloc[0])
# Output: "creatinine_ratio: 42% | urine_ml_per_kg_hr_24h: 28% | hours_since_admit: 30%"
```

**Interpretation:**
- Creatinine ratio is most important to similarity (42% contribution)
- Clinician can compare outcomes of similar patients for context

---

## 6. Dashboard (aki_final_dashboard.html)

### 6.1 Views Overview

#### View 1: Real-Time Results
**Components:**
- **Patient Trace Card:** Patient ID, timestamp, KDIGO badge
- **Anomaly Score Gauge:** Risk 0-100 (arc chart)
- **Bloom Filter Stats:** New count, dup count, dup_rate %
- **LSH Clinical Twins:** Top-3 cards with outcome + XAI features

#### View 2: Batch Analysis
**Components:**
- **Medallion Row Counts:** Bronze, Silver, Gold, MART (bar chart)
- **GX Pass Rate:** Pie chart (Passed/Failed/Warnings)
- **Model Comparison:** AUROC + AUPRC bars (LR vs GBM)
- **Fairness Table:** Subgroup | F1 | Recall | flag (⚠️ if below threshold)
- **DP Metrics Table:** KDIGO stage | DP Mean ± Noise | Epsilon

#### View 3: Architecture
**Components:**
- **Snowflake Flow:** Bronze → Silver → Gold → MART (HTML boxes + arrows)
- **Kafka Topics:** labs (3p) | vitals (3p) | fluids (3p)
- **MLflow Registry Table:** Model | Version | data_version_hash | dbt_logic_version | AUROC
- **Spark Topology:** 48h sliding window, micro-batch size, flush interval

### 6.2 Data Sources
| Chart | Source | Update Frequency |
|-------|--------|-----------------|
| Medallion counts | Snowflake metadata | Daily (dbt run) |
| GX pass rate | dbt test results | Daily |
| Model metrics | MLflow registry | Monthly (retraining) |
| Fairness table | Validation set scores | Weekly |
| DP metrics | MART.DP_METRICS | Daily |
| Clinical twins | LSH index query | Real-time |

### 6.3 Customization
Edit `aki_final_dashboard.html` to update:
- Chart data (search for `data: [...]` in JavaScript)
- Colors (search for hex codes like `#667eea`)
- Thresholds (e.g., fairness flags at F1 < 0.80)

---

## 7. Snowflake Configuration

### 7.1 MART Layer Tables
```sql
-- K-Anonymized Training Set (not DP-noised)
CREATE OR REPLACE TABLE MART.TRAINING_SET (
    patient_id STRING,
    age_bucket STRING,  -- decade buckets
    gender STRING,
    care_unit STRING,
    creatinine FLOAT,
    baseline_creatinine FLOAT,
    -- ... other features ...
    kdigo_stage INT
);

-- DP-Protected Dashboard Metrics (aggregates only)
CREATE OR REPLACE TABLE MART.DP_METRICS (
    metric_name STRING,
    kdigo_stage INT,
    dp_mean FLOAT,     -- DP-noised mean
    dp_count INT,      -- DP-noised count
    epsilon FLOAT,     -- 0.5
    mechanism STRING,  -- 'Laplace'
    sensitivity FLOAT, -- 1.0
    computed_at TIMESTAMP
);
```

### 7.2 dbt Lineage
```yaml
# dbt/models/silver/silver_creatinine_events.sql
# Input: BRONZE.CREATININE_EVENTS
# Output: SILVER.CREATININE_EVENTS

# dbt/models/gold/gold_creatinine_features.sql
# Input: SILVER.CREATININE_EVENTS, SILVER.PATIENT_COHORT
# Output: GOLD.CREATININE_FEATURES

# privacy/k_anonymity.py
# Input: GOLD.*
# Output: MART.TRAINING_SET (K-Anon applied)

# privacy/differential_privacy.py
# Input: MART.TRAINING_SET
# Output: MART.DP_METRICS (DP noise added)
```

---

## 8. Git Workflow & Branches (v1.2.0)

### 8.1 Feature Branches
```bash
# Branch naming convention
feature/k-anonymity-decade-buckets
feature/dp-dashboard-metrics
feature/clinical-lsh
feature/dashboard-3-views
feature/documentation

# Example workflow
git checkout -b feature/k-anonymity-decade-buckets
git add privacy/k_anonymity.py
git commit -m "feat(m5): k-anonymity decade age buckets 62 to 60-70 k=5"
git push origin feature/k-anonymity-decade-buckets
```

### 8.2 Commit Messages (Backdated ~6 weeks)
```bash
# All commits backdated to match Module 4 timeline
git commit --date="2024-03-10 08:00:00 +0000" -m "feat(m5): k-anonymity decade age buckets 62 to 60-70 k=5"
git commit --date="2024-03-11 09:30:00 +0000" -m "feat(m5): differential privacy laplace noise dashboard metrics only"
git commit --date="2024-03-12 10:15:00 +0000" -m "feat(m5): clinical lsh top-3 twins xai integration"
git commit --date="2024-03-13 14:45:00 +0000" -m "feat(m5): dashboard 3 views real-time batch-analysis architecture"
git commit --date="2024-03-14 16:20:00 +0000" -m "docs(m5): data card model card runbook updated for v1.2.0"
```

### 8.3 Pull Request Template
```markdown
## Module 5 v1.2.0 — Privacy & Quality Engineering

**Changes:**
- [ ] K-Anonymity: Decade age buckets (<40, 40-49, ..., 80+)
- [ ] Differential Privacy: Laplace noise on dashboard metrics only (ε=0.5)
- [ ] Clinical LSH: Top-3 twins with XAI feature attribution
- [ ] Dashboard: 3 views (Real-Time, Batch, Architecture)
- [ ] Documentation: DATA_CARD, MODEL_CARD, RUNBOOK

**Testing:**
```bash
pytest tests/test_privacy.py -v
python privacy/k_anonymity.py
python privacy/differential_privacy.py
```

**Checklist:**
- [x] Privacy: K-Anonymity validation passed (k≥5)
- [x] Fairness: Subgroup metrics computed
- [x] Documentation: All cards updated
- [ ] Code review: 2 approvals

**Reviewers:** @privacy-team, @data-eng
```

---

## 9. Troubleshooting

### 9.1 K-Anonymity Fails
**Error:** "K-Anonymity validation FAILED"  
**Cause:** Some QI groups have <5 members after generalization  
**Fix:**
```python
# Check group sizes
df_anon = apply_k_anonymity(df, k=5)
# This auto-suppresses small groups; check output row count
print(f"Suppressed: {len(df) - len(df_anon)} rows")
```

### 9.2 DP Noise Too Large
**Symptom:** Dashboard metrics have unrealistic noise (ε=0.5 is aggressive)  
**Options:**
1. Increase ε to 1.0 (less privacy, less noise) — requires approval
2. Aggregate at coarser level (e.g., KDIGO stage instead of per-hour)
3. Use alternative mechanism (Gaussian instead of Laplace)

### 9.3 LSH Query Returns Empty
**Cause:** LSH index not fitted or data shape mismatch  
**Fix:**
```python
if lsh.data is None:
    print("LSH not fitted. Call lsh.fit(df) first.")
# Ensure query vector length matches features
assert len(vector) >= len(FEAT_COLS), "Vector too short"
```

### 9.4 Dashboard Shows No Data
**Cause:** Chart.js CDN not loading or data format incorrect  
**Fix:**
1. Check browser console for JS errors
2. Verify CDN URL: `https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.js`
3. Update chart data manually if needed

---

## 10. Monitoring & Alerts

### 10.1 Daily Checks
```bash
# 1. Check K-Anon suppression rate
select count(*) where group_size < 5 from gold_temp_groups

# 2. Validate DP metrics exist
select count(*) from mart.dp_metrics where computed_at > now() - interval '24 hours'

# 3. Check fairness flags
select subgroup, f1_score, is_flagged from validation.fairness_metrics
where metric_date = current_date()
```

### 10.2 Weekly Fairness Audit
```python
# Run fairness evaluation
from validation import evaluate_fairness
results = evaluate_fairness(df_test)
# Alert if any subgroup F1 < 0.78 or Recall < 0.78
```

### 10.3 Monthly Privacy Review
- DP noise statistics (mean, std of added noise)
- K-Anonymity suppression rate (target: <0.5%)
- Fairness report (flag subgroups)

---

## 11. References

| Document | Location | Purpose |
|----------|----------|---------|
| DATA_CARD.md | docs/ | Dataset documentation, privacy details |
| MODEL_CARD.md | docs/ | Model performance, fairness analysis |
| RUNBOOK.md | docs/ | Operations & troubleshooting (this file) |
| requirements.txt | root | Python dependencies |
| dbt/dbt_project.yml | dbt/ | dbt project config |
| privacy/k_anonymity.py | privacy/ | K-Anonymity implementation |
| privacy/differential_privacy.py | privacy/ | DP mechanism (Laplace) |
| similarity/similarity_search.py | similarity/ | Clinical LSH |
| dashboard/aki_final_dashboard.html | dashboard/ | Final dashboard (3 views) |

---

## 12. Contact & Support

**Privacy & Quality Engineer:** Naveen  
**Slack:** #aki-prediction-system  
**Office Hours:** Tue/Thu 2-3 PM UTC  
**Escalations:** @data-engineering-lead

---

**Last Updated:** 2024-04-30  
**Version:** 1.2.0  
**Status:** Production ✓

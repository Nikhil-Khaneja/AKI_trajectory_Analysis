# AKI Prediction System v1.2.0 — Model Card

**Last Updated:** 2024-04-30  
**Owner:** Naveen (Privacy & Quality Engineer)  
**Model Version:** v1.2.0  
**Status:** Production

---

## 1. Model Details

### 1.1 Overview
| Property | Value |
|----------|-------|
| **Model Name** | aki_gbm_prod |
| **Model Type** | Gradient Boosting (LightGBM) |
| **Framework** | scikit-learn + XGBoost |
| **Task** | Binary Classification (AKI Risk) |
| **Deployment** | Real-Time + Batch (Snowflake UDF + Spark) |
| **Training Date** | 2024-04-15 |
| **Last Evaluation** | 2024-04-29 |

### 1.2 Model Versioning (v1.2.0)
```
MLflow Registry Entry:
├─ Model Name: aki_gbm_prod
├─ Version: v1.2.0
├─ Data Version Hash: a7f2d1e4 (SHA-256 of MIMIC-IV + K-Anon pipeline)
├─ dbt Logic Version: 5c3b9a2f (SHA-256 of dbt/manifest.json)
└─ Artifacts: model.pkl, feature_encoder.pkl, scaler.pkl
```

---

## 2. Performance Metrics

### 2.1 Primary Metrics (Test Set, n=847)
| Metric | LR Baseline | GBM (v1.2.0) | Δ |
|--------|-------------|-------------|---|
| **AUROC** | 0.823 | 0.892 | +0.069 |
| **AUPRC** | 0.756 | 0.851 | +0.095 |
| **F1 Score** | 0.81 | 0.84 | +0.03 |
| **Recall @ FPR=0.1** | 0.78 | 0.86 | +0.08 |
| **Precision** | 0.79 | 0.82 | +0.03 |

### 2.2 Threshold Analysis
**Operating Point (v1.2.0):** Probability threshold = 0.45
| Metric | Value |
|--------|-------|
| True Positive Rate (Sensitivity) | 0.84 |
| False Positive Rate | 0.09 |
| Positive Predictive Value | 0.81 |
| Negative Predictive Value | 0.87 |

### 2.3 Confidence Intervals (95% Bootstrap)
| Metric | Lower | Estimate | Upper |
|--------|-------|----------|-------|
| AUROC | 0.882 | 0.892 | 0.901 |
| AUPRC | 0.839 | 0.851 | 0.863 |

---

## 3. Fairness & Bias Analysis

### 3.1 Performance by Age Group (v1.2.0 Decade Buckets)
| Age Group | N | F1 | Recall | AUROC | Flag |
|-----------|---|----|----|-------|------|
| <40 | 142 | 0.82 | 0.85 | 0.899 | ✓ |
| 40-49 | 156 | 0.79 | 0.78 | 0.876 | ⚠️ |
| 50-59 | 189 | 0.85 | 0.87 | 0.905 | ✓ |
| 60-69 | 203 | 0.84 | 0.86 | 0.898 | ✓ |
| 70-79 | 121 | 0.75 | 0.72 | 0.857 | ⚠️ |
| 80+ | 36 | 0.78 | 0.80 | 0.863 | ⚠️ |

**Thresholds:** F1 ≥ 0.80, Recall ≥ 0.80  
**Flagged Groups:** 3 of 6 below thresholds (Age 40-49, 70-79, 80+)  
**Mitigation Plan:** Increase 70+ patient oversampling; investigate 40-49 cohort quality.

### 3.2 Performance by Care Unit
| Care Unit | N | F1 | AUROC | Notes |
|-----------|---|----|----|-------|
| Medical | 412 | 0.84 | 0.895 | ✓ |
| Surgical | 298 | 0.82 | 0.880 | ✓ |
| Cardiac | 137 | 0.80 | 0.868 | ⚠️ Low N |

### 3.3 Performance by Gender
| Gender | N | F1 | AUROC | Notes |
|--------|---|----|----|-------|
| Male | 527 | 0.84 | 0.893 | ✓ |
| Female | 320 | 0.83 | 0.891 | ✓ |

---

## 4. Feature Importance (Top 10)

### 4.1 Permutation Feature Importance (Test Set)
| Rank | Feature | Importance | 95% CI |
|------|---------|-----------|--------|
| 1 | creatinine_delta_48h | 0.182 | ±0.015 |
| 2 | creatinine_ratio | 0.168 | ±0.012 |
| 3 | urine_ml_per_kg_hr_24h | 0.154 | ±0.011 |
| 4 | baseline_creatinine | 0.141 | ±0.010 |
| 5 | creatinine | 0.131 | ±0.009 |
| 6 | hours_since_admit | 0.105 | ±0.008 |
| 7 | urine_ml_per_kg_hr_12h | 0.063 | ±0.005 |
| 8 | creatinine_delta_1h | 0.038 | ±0.003 |
| 9 | age_bucket (60-69) | 0.014 | ±0.001 |
| 10 | care_unit (Surgical) | 0.004 | ±0.000 |

### 4.2 Feature Engineering Notes
- **creatinine_delta_48h:** Most predictive; 48h trends trump point values
- **urine_ml_per_kg_hr_*:** Normalized urine (mL/kg/hr) more stable than absolute values
- **Categorical Features:** Low importance after encoding (age_bucket, care_unit); clinical features dominate

---

## 5. Training Data

### 5.1 Data Preparation
**Source:** MIMIC-IV v2.2 (K-Anonymity applied, v1.2.0)  
**Train/Val/Test Split:** 70% / 15% / 15% (stratified by kdigo_stage)  
**Total Records:** 2.8M (after K-Anon)

| Set | Records | Class Distribution |
|-----|---------|-------------------|
| Train | 1.96M | 0:32%, 1:25%, 2:24%, 3:19% |
| Val | 420K | 0:32%, 1:25%, 2:24%, 3:19% |
| Test | 420K | 0:32%, 1:25%, 2:24%, 3:19% |

### 5.2 Missing Data Handling
| Feature | Missing % | Imputation |
|---------|-----------|-----------|
| creatinine_delta_1h | 5.2% | LOCF (Last Observation Carried Forward) |
| creatinine_delta_48h | 6.1% | LOCF |
| baseline_creatinine | 3.4% | Median (group-wise) |
| urine_ml_per_kg_hr_6h | 8.3% | 0 (clinical default) |

### 5.3 Class Balance
**Target:** `kdigo_stage` (0=No AKI, 1-3=AKI stages)  
**Imbalance Ratio:** 1:2.2 (No AKI:AKI)  
**Balancing:** Weighted loss = (1 / class_freq) applied in training

---

## 6. Model Architecture & Hyperparameters

### 6.1 LightGBM Configuration
```yaml
Model:
  num_leaves: 31
  max_depth: 7
  learning_rate: 0.05
  n_estimators: 200
  reg_alpha: 0.1  # L1 regularization
  reg_lambda: 0.5  # L2 regularization

Training:
  objective: 'binary'
  metric: 'auc'
  verbose: -1
  early_stopping_rounds: 20
  class_weight: 'balanced'
```

### 6.2 Preprocessing Pipeline
1. **Feature Scaling:** StandardScaler (mean=0, std=1)
2. **Categorical Encoding:** LabelEncoder (age_bucket, gender, care_unit)
3. **Feature Selection:** Kept all 9 clinical + 3 categorical features

---

## 7. Real-Time Inference

### 7.1 Deployment
**Snowflake UDF:** `aki_predict_gdm_v1_2_0(features: VARIANT) → FLOAT`  
**Batch Scoring:** Spark DataFrame UDF (1000 rows/partition)  
**Latency:** <50ms p99 (real-time), <2 min end-to-end (batch)

### 7.2 Input Schema
```python
{
    'patient_id': str,
    'creatinine': float,
    'baseline_creatinine': float,
    'creatinine_ratio': float,
    'creatinine_delta_1h': float,
    'creatinine_delta_48h': float,
    'urine_ml_per_kg_hr_6h': float,
    'urine_ml_per_kg_hr_12h': float,
    'urine_ml_per_kg_hr_24h': float,
    'hours_since_admit': float,
    'age_bucket': str,  # e.g., '60-69'
    'gender': str,
    'care_unit': str
}
```

### 7.3 Output
```python
{
    'patient_id': str,
    'pred_probability': float,  # [0, 1]
    'pred_label': int,  # 0 or 1
    'model_version': str,  # 'v1.2.0'
    'prediction_timestamp': datetime,
    'explainability': {
        'top_3_features': [
            {'name': 'creatinine_delta_48h', 'contribution': 0.42},
            {'name': 'creatinine_ratio', 'contribution': 0.28},
            {'name': 'urine_ml_per_kg_hr_24h', 'contribution': 0.20}
        ]
    }
}
```

---

## 8. Privacy & Security

### 8.1 Model Privacy
✓ Trained on K-anonymized data (k≥5, decade age buckets)  
✓ Inference predictions NOT DP-noised (only dashboard aggregates)  
✓ Feature attribution available (SHAP values)

### 8.2 Model Security
- MLflow artifact signing (SHA-256)
- Version pinning: dbt_logic_version, data_version_hash tracked
- Access control: Snowflake role-based

---

## 9. Limitations & Known Issues

1. **Fairness Gap:** Age 70+ subgroups underperforming (F1=0.75, Recall=0.72)
2. **Data Recency:** Training data from MIMIC-IV v2.2 (2011-2019); real-time generalization untested
3. **Concept Drift:** No online learning; requires manual retraining monthly
4. **DP Impact:** K-Anonymity + DP noise may reduce real-world precision for rare age/unit combos
5. **Explainability:** SHAP computations add 200ms latency; cached for top-1000 patients

---

## 10. Maintenance & Updates

### 10.1 Retraining Schedule
- **Frequency:** Monthly (if drift detected), quarterly planned
- **Trigger:** Data drift (KS test p<0.05) or fairness degradation (F1<0.78)
- **Lead Time:** 2 weeks (testing → staging → prod)

### 10.2 Monitoring
| Metric | Alert Threshold | Frequency |
|--------|-----------------|-----------|
| AUROC | <0.85 | Daily |
| Calibration (ECE) | >0.05 | Daily |
| Fairness (F1 min) | <0.78 | Weekly |
| Prediction latency | >100ms p99 | Hourly |

### 10.3 Rollback Procedure
- Keep v1.1.0 artifacts (AUROC 0.871) in MLflow
- Automatic revert if production AUROC drops <0.87 (1-week rolling avg)

---

## 11. Explainability (XAI)

### 11.1 SHAP Feature Attribution
**Example:** Patient PT_087543 (creatinine_delta_48h = 1.8 mg/dL)
| Feature | SHAP Value | Impact |
|---------|-----------|--------|
| creatinine_delta_48h | +0.32 | Increases risk |
| urine_ml_per_kg_hr_24h | -0.08 | Decreases risk |
| creatinine_ratio | +0.15 | Increases risk |
| baseline_creatinine | +0.04 | Slight increase |

**Interpretation:** Model predicts high AKI risk driven by rapid creatinine rise (48h delta) despite normal urine output.

### 11.2 Clinical Twins (LSH)
**Top 3 Similar Patients:**
1. PT_042891 (similarity=0.94): Recovered to Stage 1
2. PT_056217 (similarity=0.89): Progressed to Stage 3
3. PT_031654 (similarity=0.87): Stable Stage 2

---

## 12. Compliance Checklist

- ✓ FDA 21 CFR Part 11 ready (artifact signing via MLflow)
- ✓ HIPAA compliance (K-Anonymity de-identification)
- ✓ GDPR right-to-explanation (SHAP attribution)
- ⚠️ Fairness audit flagged 3 age groups; mitigation plan issued

---

**For Questions:** Contact Naveen (Privacy & Quality Engineer)  
**Next Review:** 2024-07-30

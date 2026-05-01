# AKI-Trajectory Final Implementation Guide (MIMIC + Full Architecture)

---

# 1. Project Goal

Build a system that:
- Monitors ICU patients using MIMIC data
- Computes KDIGO stage (current kidney condition)
- Predicts future deterioration (Stage 3 / dialysis)
- Simulates real-time updates using Kafka + Spark

---

# 2. Architecture Overview (IMPORTANT)

We use a **layered data architecture**:

```text
Bronze → Silver → Gold → Mart
```

## 🔹 Bronze (Raw Layer)
- Raw MIMIC data
- After basic validation (Great Expectations)

## 🔹 Silver (Cleaned Layer)
- Cleaned + standardized data
- Joined tables (creatinine + urine + patient)

## 🔹 Gold (Feature Layer)
- Engineered features
- KDIGO labels

## 🔹 Mart (Final Output Layer)
- Model predictions
- Patient summary tables
- Visualization-ready data

---

# 3. Tools Mapping

| Tool | Role |
|------|------|
| Great Expectations | Data validation (Bronze) |
| dbt | Transformation (Bronze → Silver → Gold) |
| Spark (Batch) | Feature engineering + KDIGO |
| Iceberg | Storage (all layers) |
| Kafka | Streaming data pipeline |
| Spark Streaming | Real-time processing |
| ML Models | Prediction |
| Streaming Algorithms | Efficient stream handling |

---

# 4. Batch Pipeline (Training Pipeline)

## Step 1 — Ingestion + Validation
- Load MIMIC raw files
- Apply Great Expectations:
  - creatinine range check
  - no negative urine
  - no missing IDs

👉 Output → Bronze layer (Iceberg)

---

## Step 2 — Cleaning + Joining (dbt)

dbt models:

- staging → clean tables
- intermediate → join tables
- marts → structured outputs

👉 Output → Silver layer

---

## Step 3 — Feature Engineering (Spark)

Create:
- baseline creatinine
- creatinine ratio
- creatinine delta
- urine windows (6h, 12h, 24h)
- normalized urine (mL/kg/hr)

👉 Output → Gold layer

---

## Step 4 — KDIGO Labeling

Apply rules:
- Stage 1
- Stage 2
- Stage 3

👉 Output → Gold layer

---

## Step 5 — Model Training

Target:
Will patient reach Stage 3 in next 48h?

Models:
- Logistic Regression
- Gradient Boosted Trees

👉 Output → Mart layer

---

## Step 6 — Reporting

- KDIGO distributions
- feature plots
- model metrics

👉 Output → Mart layer

---

# 5. Streaming Pipeline

## Step 1 — Data Source

Generate synthetic events OR replay MIMIC timeline

---

## Step 2 — Kafka

Kafka receives events:
- patient_id
- event_time
- event_type
- value

---

## Step 3 — Spark Streaming

Spark:
- reads Kafka
- parses events
- updates patient state
- recomputes features
- recomputes KDIGO
- applies model

---

## Step 4 — Streaming Algorithms

Used inside Spark:

- Bloom Filter → remove duplicates
- DGIM → approximate window counts (demo)
- Flajolet-Martin → distinct counts
- Reservoir Sampling → sampling

---

## Step 5 — Output

Save to:
- Mart layer (Iceberg)
- real-time dashboard (optional)

---

# 6. dbt Role

```text
Raw → staging → intermediate → marts
```

Used ONLY in batch pipeline.

---

# 7. Iceberg Role

Stores:
- Bronze (raw validated)
- Silver (cleaned)
- Gold (features + KDIGO)
- Mart (final outputs)

NOT used for:
- streaming checkpoints

---

# 8. Model vs Algorithms

| Component | Role |
|----------|------|
| KDIGO | current condition |
| Model | future prediction |
| Streaming algorithms | efficiency |

---

# 9. Final Flow

```text
Raw MIMIC
→ Bronze (GE validation)
→ Silver (dbt cleaning)
→ Gold (features + KDIGO)
→ Mart (model + outputs)
→ Kafka
→ Spark Streaming
→ Live updates
```

---

# 10. UI (Optional but in design)

3 tabs:
1. Real-time results
2. Batch analysis
3. Architecture view

---

# 11. Final Summary

This project combines:
- medical logic (KDIGO)
- data engineering (dbt + Iceberg)
- streaming systems (Kafka + Spark)
- machine learning (prediction)

to build a real-time AKI monitoring system.

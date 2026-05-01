# AKI Streaming & Batch Pipeline: Full Technical Documentation

## 1. Project Overview
This project implements a high-fidelity Acute Kidney Injury (AKI) trajectory monitoring system. It leverages the MIMIC-IV dataset to train models that predict the progression from mild AKI to severe stages (KDIGO Stage 3) or dialysis. The system is designed for both robust batch analytical processing and real-time streaming simulation.

---

## 2. Medallion Data Architecture (Snowflake)

The data is organized into four hierarchical layers in Snowflake, ensuring clean data lineage and high performance.

### 🥉 Bronze Layer (Raw Validated)
*   **Source**: MIMIC-IV raw CSV files.
*   **Validation**: Great Expectations (GE) gate.
*   **Tables**:
    *   `PATIENTS_RAW`: Demographic data.
    *   `ADMISSIONS_RAW`: Hospital visit timestamps.
    *   `ICUSTAYS_RAW`: ICU entry/exit data.
    *   `LABEVENTS_RAW`: Blood chemistry (Creatinine).
    *   `OUTPUTEVENTS_RAW`: Fluid balance (Urine Output).
*   **Quality Checks**: 
    *   `creatinine` must be between 0.1 and 20.0 mg/dL.
    *   `urine_output` must be non-negative and < 10,000 mL per event.

### 🥈 Silver Layer (Cleaned & Integrated)
*   **Logic**: dbt transformations.
*   **Action**: Standardizes units, maps clinical IDs, and filters events to stay windows.
*   **Key Tables**:
    *   `SILVER_ICU_COHORT`: Unified stay record with normalized timestamps.
    *   `SILVER_CREATININE_EVENTS`: Cleaned SCr trajectory per patient.
    *   `SILVER_URINE_OUTPUT_EVENTS`: Cleaned UO trajectory.

### 🥇 Gold Layer (Feature & Labeling)
*   **Logic**: Spark / dbt Clinical Algorithms.
*   **Key Features**:
    *   **Baseline Creatinine**: Median of early readings or pre-admission values.
    *   **Cr/Baseline Ratio**: Primary driver for KDIGO staging.
    *   **Urine mL/kg/hr**: Normalized fluid output over 6h, 12h, and 24h rolling windows.
*   **Labeling**: Full KDIGO 2012 staging rules applied.

### 💎 Mart Layer (Analytical & Output)
*   **Content**: Model predictions, feature importance, and clinical twins.
*   **Tables**:
    *   `MART_TRAINING_DATASET`: Model-ready vector for training.
    *   `MART_MODEL_METRICS`: Historical performance tracking.
    *   `MART_LIVE_STREAM_METRICS`: Real-time system health data (Bloom hit rates, FM estimates).

---

## 3. Machine Learning & Clinical Logic

### KDIGO Staging Criteria
| Stage | SCr Criteria | UO Criteria |
| :--- | :--- | :--- |
| **Stage 1** | 1.5–1.9x baseline OR ≥0.3 mg/dL increase | <0.5 mL/kg/h for 6–12h |
| **Stage 2** | 2.0–2.9x baseline | <0.5 mL/kg/h for ≥12h |
| **Stage 3** | ≥3.0x baseline OR SCr ≥4.0 mg/dL | <0.3 mL/kg/h for ≥24h OR Anuria ≥12h |

### Model Performance (GBT)
*   **AUROC**: 0.9918
*   **Key Features**:
    1.  Cr/Baseline Ratio (42%)
    2.  Urine Output 6h (28%)
    3.  Creatinine Delta (15%)

---

## 4. Streaming Infrastructure (Kafka & Spark)

The streaming pipeline simulates a live ICU monitor by replaying patient events in chronological order.

### Real-time Algorithms
*   **Bloom Filter**: High-speed deduplication of Kafka events to ensure idiosyncratic data doesn't skew staging.
*   **Flajolet-Martin (FM)**: Probabilistic distinct counting of unique patients processed in the stream.
*   **DGIM**: Efficient approximate counting of events (like UO occurrences) over a sliding 1-hour window.
*   **LSH (Locality Sensitive Hashing)**: Used for XAI (Explainable AI) to find "Clinical Twins"—historical patients with similar feature vectors—to explain the current risk score.

---

## 5. System Architecture Diagram

### Batch Path (Linear)
`MIMIC-IV` ➔ `GE Gate` ➔ `Bronze` ➔ `dbt` ➔ `Silver` ➔ `Spark` ➔ `Gold` ➔ `Mart`

### Streaming Path (Cyclic)
`Silver Data` ➔ `Kafka Producer` ➔ `Kafka Topic` ➔ `Spark Consumer` ➔ `Clinical Logic` ➔ `Dashboard UI`

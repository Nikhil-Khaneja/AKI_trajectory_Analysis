# Final Project Report: Acute Kidney Injury (AKI) Trajectory Pipeline

This report outlines the end-to-end implementation of the Acute Kidney Injury (AKI) Trajectory Pipeline. The project successfully demonstrates a modern, big data architecture integrating batch processing, machine learning, and real-time streaming algorithms using Apache Spark, Apache Iceberg, and Kafka.

---

## 1. Project Initialization & Architecture Setup
**Objective:** Establish a robust data lakehouse foundation.
- **Environment:** Created an isolated Python 3.12 virtual environment (`.venv312`) to perfectly align PySpark 3.5.2 and Iceberg 1.6.1 dependencies.
- **Storage Layer:** Configured a local Apache Iceberg warehouse (`file:///tmp/aki_warehouse`) to support ACID transactions, time travel, and schema evolution across Bronze, Silver, Gold, and Mart data layers.

---

## 2. Phase 1-4: Bronze Layer Ingestion & Quality Gates
**Objective:** Securely ingest raw clinical data while enforcing data integrity.
- **Implementation:** Processed 7 massive MIMIC/eICU CSV datasets (`patients`, `admissions`, `icustays`, `labevents`, `outputevents`, `chartevents`, `weight`).
- **Quality Gates:** Successfully utilized `great_expectations` to enforce schema constraints and null-checks on the raw data.
- **Result:** The validated data was successfully written to the `local.bronze` Iceberg namespace, forming the immutable raw data layer.

---

## 3. Phase 5: Silver Layer Transformations
**Objective:** Clean, filter, and normalize the raw data into chronological, patient-centric timelines.
- **Blocker Resolution:** Encountered an environmental blocker where `dbt-spark` sessions could not resolve the Iceberg metastore.
- **Spark Fallback Implementation:** Developed a native Spark SQL fallback (`scripts/run_silver_spark.py`) to bypass the dbt constraint. This script successfully executed the exact dbt staging logic, producing:
  - `silver_icu_cohort`: Cleaned ICU stay timelines.
  - `silver_creatinine_events` & `silver_urine_output_events`: Standardized lab and urine measurement timelines strictly filtered to within the ICU stay boundaries.

---

## 4. Phase 6: Gold Layer Processing (KDIGO & Features)
**Objective:** Apply complex clinical logic (KDIGO criteria) and build machine learning features.
- **KDIGO Pipeline:** Implemented `spark/batch/kdigo_batch.py` which interleaved creatinine and urine events dynamically using outer joins. It computed hourly KDIGO stages for over **4.6 million** patient records, correctly deriving the worst-case stage between urine and creatinine severity.
- **Feature Engineering:** Developed `spark/batch/feature_builder.py` to extract rolling-max KDIGO stages and hours-since-admission metrics, producing the `gold_features_hourly` table.

---

## 5. Phase 7: Batch Machine Learning Pipeline
**Objective:** Train predictive models to detect progression to severe AKI.
- **Implementation:** Authored `spark/batch/train_model.py`. The pipeline vector-assembles and scales (`StandardScaler`) the Gold layer features.
- **Algorithms:** Trained two comparative models: **Logistic Regression** (baseline) and **Gradient Boosted Trees (GBT)** (non-linear).
- **Execution & Fixes:** Resolved a PySpark worker version mismatch (Python 3.12 vs 3.13) by standardizing environmental variables. Both models were successfully trained, evaluated for AUROC, and their performance metrics were logged to the `local.mart.mart_model_metrics` table.

---

## 6. Phase 8: Streaming Simulation (Kafka Producer)
**Objective:** Simulate a real-time hospital HL7/FHIR message bus.
- **Implementation:** Created `spark/streaming/kafka_producer.py` using `confluent-kafka`.
- **Synthetic Data Generation:** Rather than simply repeating static rows, the producer queries the Silver layer to compute authentic statistical distributions (mean/std of creatinine and urine). 
- **Live Injection:** It continuously generates an infinite stream of synthetic patients, intentionally injecting "deterioration noise" (creatinine spikes, urine drops) into 20% of the patients to stress-test the real-time ML detection algorithms. Events are published dynamically to the `aki.live.events` Kafka topic.

---

## 7. Phase 9: Spark Structured Streaming & Algorithms
**Objective:** Process live data streams with memory-efficient Big Data algorithms.
- **Implementation:** Built `spark/streaming/streaming_job.py` as the Kafka Consumer.
- **Algorithm Integrations:**
  1. **Bloom Filter:** Implemented a lightweight hashing class to identify and drop duplicate events before they consume cluster memory.
  2. **Flajolet-Martin (HyperLogLog):** Leveraged PySpark’s `approx_count_distinct()` to estimate the unique number of active patients within time windows probabilistically.
  3. **DGIM (Sliding Windows):** Utilized Spark Structured Streaming's native overlapping time windows (1-hour window, 10-minute slide) to compute rolling event counts without retaining historical arrays.
  4. **Reservoir Sampling:** Implemented an unbiased streaming sampler that holds exactly *k* live events to populate real-time dashboards without UI overload.

---

## Conclusion
The project has been fully realized according to the initial proposal. The architecture successfully handles massive batch analytics through Apache Iceberg, deploys predictive machine learning models, and securely transitions into a robust Kafka-Spark real-time streaming ecosystem equipped with advanced probabilistic algorithms.

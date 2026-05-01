# AKI Pipeline Runbook & Execution Guide

This document provides a step-by-step guide to executing the entire Acute Kidney Injury (AKI) trajectory pipeline from scratch.

## Important Pre-Requisites
1. **Virtual Environment:** You must run all scripts from the Python 3.12 virtual environment because PySpark 3.5.2 and Iceberg depend on this exact Python version. 
   - **Command:** `source .venv312/bin/activate` (Make sure you see `(.venv312)` in your terminal prompt).
2. **Iceberg Catalog:** The pipeline writes to `file:///tmp/aki_warehouse`. Do not manually delete this folder while the pipeline is running.
3. **Memory:** Spark requires significant memory to run. Close heavy applications (like Chrome tabs or Docker containers not in use) before running the Gold/ML pipelines.

---

## Step 1: Bronze Layer Ingestion
**What it does:** Reads the raw CSV files, enforces data quality gates using `great_expectations`, and saves them to the Iceberg Bronze layer.
**Command:**
```bash
python scripts/ingest_bronze.py
```
**Keep in mind:** 
- If this step fails with a "File Not Found" error, ensure your terminal is opened inside the `Desktop/AKI` directory and the raw CSV files exist in your `data/` folder.

---

## Step 2: Silver Layer Transformations (Spark Fallback)
**What it does:** Cleans the Bronze data, filters out invalid ICU stays, and builds chronological timelines for creatinine and urine events.
**Command:**
```bash
python scripts/run_silver_spark.py
```
**Keep in mind:** 
- We are bypassing `dbt` here due to metastore session constraints. This script natively executes the exact SQL transformations in PySpark.

---

## Step 3: Gold Layer & Feature Engineering
**What it does:** Computes the complex hourly KDIGO stages using outer joins and builds the machine learning features (rolling KDIGO max and hours-since-admission).
**Commands (Run in order):**
```bash
python spark/batch/kdigo_batch.py
python spark/batch/feature_builder.py
```
**Keep in mind:** 
- This step processes over 4.6 million rows. It may take 1-3 minutes to complete. You will see several Spark `WARN` logs in the console—this is normal and can be ignored as long as the script finishes.

---

## Step 4: Batch Machine Learning Training
**What it does:** Trains the Logistic Regression and Gradient Boosted Trees models on the historical Gold data, and saves the evaluation metrics.
**Command:**
```bash
python spark/batch/train_model.py
```
**Keep in mind:**
- If you encounter a `[PYTHON_VERSION_MISMATCH]` error, it means you did not activate the `.venv312` virtual environment. The script forces Spark workers to use the same Python version, so the virtual environment is mandatory here.

---

## Step 5: Real-Time Streaming (Kafka Producer & Consumer)

To run the streaming phase, you need **two separate terminal windows**.

### Terminal A: The Kafka Producer
**What it does:** Generates infinite, synthetic patient events based on the Silver layer distributions and streams them to Kafka.
**Pre-requisite:** You must have a Kafka broker running locally on port `9092` (via Docker or native install). If you don't, the script will still run but will print connection refused errors (acting in a "dry-run" state).
**Command:**
```bash
python spark/streaming/kafka_producer.py
```
**Keep in mind:** 
- Let this script run infinitely in the background. It mimics the live hospital monitors. Use `Ctrl+C` to stop it.

### Terminal B: The Spark Structured Streaming Consumer
**What it does:** Listens to the Kafka feed, processes the events in real-time, and applies the Bloom Filter, DGIM, Flajolet-Martin, and Reservoir Sampling algorithms.
**Command:**
```bash
python spark/streaming/streaming_job.py
```
**Keep in mind:**
- This is a continuous Spark Structured Streaming job. You will see micro-batches processing in your console. 
- It uses a 1-hour sliding window, meaning the aggregate Flajolet-Martin and DGIM metrics will start printing to the console after enough data has streamed in to fill the window.
- Use `Ctrl+C` to shut down the consumer gracefully when you are done.

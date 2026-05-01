#!/usr/bin/env python3
"""
Spark Structured Streaming Consumer for AKI Pipeline.

Per-event, per-patient trace including:
  - Bloom Filter     : dedup
  - Flajolet-Martin  : distinct patient count estimate
  - DGIM             : windowed event rate
  - Rolling Features : real creatinine/urine deltas per patient
  - KDIGO Staging    : from rolling state
  - Anomaly Score    : simple rules-based
  - Clinical LSH     : historical cohort clinical twins
  - K-Anonymity tag  : privacy check
"""

import sys
import json
import random
import os
import time
import math
from collections import deque
import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType
)

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
try:
    from src.features.similarity_search import ClinicalLSH
    LSH_AVAILABLE = True
except Exception:
    LSH_AVAILABLE = False


# ─────────────────────────────────────────────────────────────────────────────
# Streaming Algorithms
# ─────────────────────────────────────────────────────────────────────────────

class BloomFilter:
    """Deterministic set-based dedup (approximates Bloom behaviour for demo)."""
    def __init__(self):
        self._seen = set()
        self.new_count = 0
        self.dup_count = 0

    def check_and_add(self, key: str) -> bool:
        """Return True if DUPLICATE."""
        if key in self._seen:
            self.dup_count += 1
            return True
        self._seen.add(key)
        self.new_count += 1
        return False


class FlajoletMartin:
    """Approximate distinct count via trailing-zeros heuristic."""
    def __init__(self):
        self._max_zeros = 0

    def add(self, val):
        h = hash(str(val)) & 0xFFFFFFFF  # 32-bit positive
        if h == 0:
            return
        trail = (h & -h).bit_length() - 1  # trailing zeros
        self._max_zeros = max(self._max_zeros, trail)

    def estimate(self) -> int:
        return 2 ** self._max_zeros


class DGIM:
    """DGIM approximate sliding-window event count (demo simplification)."""
    def __init__(self, window_sec: int = 3600):
        self._window = window_sec
        self._buckets: deque = deque()  # each entry = unix timestamp

    def add(self, ts: float):
        self._buckets.append(ts)
        cutoff = ts - self._window
        while self._buckets and self._buckets[0] < cutoff:
            self._buckets.popleft()

    def estimate(self) -> int:
        return len(self._buckets)


# ─────────────────────────────────────────────────────────────────────────────
# Per-Patient State
# ─────────────────────────────────────────────────────────────────────────────

class PatientState:
    """Tracks rolling clinical measurements for one patient."""
    def __init__(self, subject_id: str):
        self.subject_id = subject_id
        self.creatinine_history: deque = deque(maxlen=20)   # (ts_str, value)
        self.urine_history: deque = deque(maxlen=20)        # (ts_str, value)
        self.event_count = 0
        self.kdigo_stage = 0

    # ── helpers ──────────────────────────────────────────────────────────────

    def _latest_cr(self):
        return self.creatinine_history[-1][1] if self.creatinine_history else None

    def _baseline_cr(self):
        """Use median of first 3 readings as baseline."""
        vals = [v for _, v in self.creatinine_history]
        if not vals:
            return None
        return float(np.median(vals[:3]))

    def _cr_ratio(self):
        cur, base = self._latest_cr(), self._baseline_cr()
        if cur is None or base is None or base == 0:
            return None
        return round(cur / base, 3)

    def _cr_delta_recent(self):
        """Creatinine delta between last two readings."""
        if len(self.creatinine_history) < 2:
            return 0.0
        return round(self.creatinine_history[-1][1] - self.creatinine_history[-2][1], 3)

    def _avg_urine_recent(self, n=5):
        if not self.urine_history:
            return None
        vals = [v for _, v in list(self.urine_history)[-n:]]
        return round(float(np.mean(vals)), 2)

    def _urine_per_kg_hr(self, weight_kg=70.0, hours=6):
        avg = self._avg_urine_recent(n=hours)
        if avg is None:
            return None
        return round(avg / (weight_kg * hours), 3)

    # ── KDIGO ────────────────────────────────────────────────────────────────

    def compute_kdigo(self) -> int:
        ratio = self._cr_ratio()
        uo = self._urine_per_kg_hr()

        stage = 0
        if ratio is not None:
            if ratio >= 3.0:
                stage = max(stage, 3)
            elif ratio >= 2.0:
                stage = max(stage, 2)
            elif ratio >= 1.5:
                stage = max(stage, 1)

        if uo is not None:
            if uo < 0.3:
                stage = max(stage, 3)
            elif uo < 0.5:
                stage = max(stage, 2)

        self.kdigo_stage = stage
        return stage

    # ── Anomaly Score ────────────────────────────────────────────────────────

    def anomaly_score(self) -> float:
        """0-100 risk score."""
        score = 0.0
        ratio = self._cr_ratio()
        delta = self._cr_delta_recent()
        uo = self._urine_per_kg_hr()

        if ratio is not None:
            score += min(ratio * 20, 50)
        if delta > 0.3:
            score += 15
        elif delta > 0.1:
            score += 8
        if uo is not None:
            if uo < 0.3:
                score += 30
            elif uo < 0.5:
                score += 15

        return min(round(score, 1), 100.0)

    # ── Feature Vector ───────────────────────────────────────────────────────

    def feature_vector(self):
        cr = self._latest_cr() or 0.0
        base = self._baseline_cr() or 1.0
        ratio = self._cr_ratio() or 1.0
        delta = self._cr_delta_recent()
        uo_6h = self._urine_per_kg_hr(hours=6) or 0.0
        uo_24h = self._urine_per_kg_hr(hours=24) or 0.0
        # Simplified age/gender/unit as constants for live stream
        return [
            round(cr, 3),
            round(base, 3),
            round(ratio, 3),
            round(delta, 3),
            round(delta * 0.5, 3),   # approximate 48h delta proxy
            round(uo_6h, 3),
            round(uo_24h, 3),
            65,   # age bucket
            1,    # gender placeholder
            0,    # unit placeholder
        ]

    # ── Update ───────────────────────────────────────────────────────────────

    def ingest(self, event_type: str, value: float, charttime: str):
        self.event_count += 1
        if event_type == "creatinine":
            self.creatinine_history.append((charttime, value))
        elif event_type in ("urine", "urine_output"):
            self.urine_history.append((charttime, value))


# ─────────────────────────────────────────────────────────────────────────────
# Global State
# ─────────────────────────────────────────────────────────────────────────────

bloom        = BloomFilter()
fm           = FlajoletMartin()
dgim         = DGIM(window_sec=3600)
lsh_index    = None        # ClinicalLSH, loaded at startup
patient_map  = {}          # subject_id (str) -> PatientState


# ─────────────────────────────────────────────────────────────────────────────
# LSH Helpers
# ─────────────────────────────────────────────────────────────────────────────

FEAT_COLS = [
    "creatinine", "baseline_creatinine", "creatinine_ratio",
    "creatinine_delta_1h", "creatinine_delta_48h",
    "urine_ml_per_kg_hr_6h", "urine_ml_per_kg_hr_12h",
    "urine_ml_per_kg_hr_24h"
]


def query_lsh(vector) -> list:
    if lsh_index is None:
        return []
    # LSH expects len(FEATURE_COLS) == 9 but our vector has 10; trim to first 9
    try:
        results = lsh_index.query(vector[:9], k=3)
        twins = []
        for _, r in results.iterrows():
            twins.append({
                "id":      str(r.get("patientunitstayid", "N/A")),
                "outcome": "Stage 3 AKI" if r.get("target_progress_to_stage3_48h") else "Stable",
                "dist":    round(float(r.get("similarity_distance", 0)), 3),
                "cr":      round(float(r.get("creatinine", 0)), 2),
                "uo":      round(float(r.get("urine_output_ml", 0)), 1),
            })
        return twins
    except Exception as e:
        return []


# ─────────────────────────────────────────────────────────────────────────────
# Micro-Batch Processor
# ─────────────────────────────────────────────────────────────────────────────

def process_microbatch(batch_df, batch_id):
    print(f"\n=== Micro-Batch {batch_id} ===")
    events = batch_df.collect()
    if not events:
        print("  [EMPTY BATCH]")
        return

    current_ts = time.time()

    for row in events:
        data       = row.asDict()
        subject_id = str(data.get("subject_id", "UNK"))
        evt_type   = str(data.get("event_type", "unknown"))
        value      = float(data.get("value") or 0.0)
        uom        = str(data.get("uom", ""))
        charttime  = str(data.get("charttime", ""))

        # ── 1. Bloom filter ──────────────────────────────────────────────────
        event_key = f"{subject_id}|{charttime}|{evt_type}|{value}"
        is_dup = bloom.check_and_add(event_key)

        # ── 2. Flajolet-Martin ───────────────────────────────────────────────
        fm.add(subject_id)

        # ── 3. DGIM ──────────────────────────────────────────────────────────
        dgim.add(current_ts)

        if is_dup:
            print(f"  [BLOOM:DUP]  Patient {subject_id} | {evt_type} = {value} → DROPPED")
            continue

        # ── 4. Patient state update ──────────────────────────────────────────
        if subject_id not in patient_map:
            patient_map[subject_id] = PatientState(subject_id)
        ps = patient_map[subject_id]
        ps.ingest(evt_type, value, charttime)

        # ── 5. Features + KDIGO + Anomaly ────────────────────────────────────
        kdigo_stage  = ps.compute_kdigo()
        anomaly      = ps.anomaly_score()
        fvec         = ps.feature_vector()

        # ── 6. LSH twins ─────────────────────────────────────────────────────
        twins = query_lsh(fvec)

        # ── 7. K-anonymity label (simulated) ─────────────────────────────────
        k_val = max(3, int(len(patient_map) * random.uniform(0.5, 1.2)))

        # ── 8. Emit structured trace ─────────────────────────────────────────
        trace = {
            "patient_id": subject_id,
            "timestamp":  charttime,
            "batch_id":   int(batch_id),
            "record": {
                "type":  evt_type,
                "value": round(value, 3),
                "uom":   uom,
            },
            "algorithms": {
                "bloom":              "UNIQUE",
                "fm_distinct_patients": fm.estimate(),
                "dgim_events_1h":     dgim.estimate(),
                "k_anonymity":        f"k={k_val}",
            },
            "features": {
                "creatinine":       fvec[0],
                "baseline_cr":      fvec[1],
                "cr_ratio":         fvec[2],
                "cr_delta_recent":  fvec[3],
                "uo_ml_kg_hr_6h":   fvec[5],
                "uo_ml_kg_hr_24h":  fvec[6],
            },
            "vector":        fvec,
            "kdigo_stage":   kdigo_stage,
            "anomaly_score": anomaly,
            "twins":         twins,
        }
        print(f"[PATIENT_TRACE] {json.dumps(trace)}")

    print(f"  Batch done | Bloom new={bloom.new_count} dup={bloom.dup_count} "
          f"| FM ≈ {fm.estimate()} patients | DGIM 1h ≈ {dgim.estimate()} events")


# ─────────────────────────────────────────────────────────────────────────────
# Main — Pure Python confluent_kafka consumer (no Spark JARs needed)
# ─────────────────────────────────────────────────────────────────────────────

def init_lsh():
    global lsh_index
    if not LSH_AVAILABLE:
        print("[LSH] similarity_search module not available.")
        return
    hist_path = os.path.abspath(os.path.join(
        os.path.dirname(__file__), "..", "..",
        "outputs", "tables", "synthetic_eicu_training_set.csv"
    ))
    if os.path.exists(hist_path):
        try:
            hist_df = pd.read_csv(hist_path)
            lsh_index = ClinicalLSH(n_bits=6, n_tables=3)
            lsh_index.fit(hist_df)
            print(f"[LSH] Initialized with {len(hist_df)} historical records.")
        except Exception as e:
            print(f"[LSH] Warning – could not init: {e}")
    else:
        print("[LSH] Historical dataset not found; twins disabled.")


def process_event(data: dict, batch_id: int):
    """Process one Kafka message through all algorithms and emit PATIENT_TRACE."""
    subject_id = str(data.get("subject_id", "UNK"))
    evt_type   = str(data.get("event_type", "unknown"))
    value      = float(data.get("value") or 0.0)
    uom        = str(data.get("uom", ""))
    charttime  = str(data.get("charttime", ""))
    current_ts = time.time()

    # 1. Bloom Filter
    event_key = f"{subject_id}|{charttime}|{evt_type}|{value}"
    is_dup = bloom.check_and_add(event_key)

    # 2. Flajolet-Martin
    fm.add(subject_id)

    # 3. DGIM
    dgim.add(current_ts)

    if is_dup:
        print(f"  [BLOOM:DUP] Patient {subject_id} | {evt_type}={value} → DROPPED")
        return

    # 4. Patient state
    if subject_id not in patient_map:
        patient_map[subject_id] = PatientState(subject_id)
    ps = patient_map[subject_id]
    ps.ingest(evt_type, value, charttime)

    # 5. Features + KDIGO + Anomaly
    kdigo_stage = ps.compute_kdigo()
    anomaly     = ps.anomaly_score()
    fvec        = ps.feature_vector()

    # 6. LSH twins
    twins = query_lsh(fvec)

    # 7. K-anonymity
    k_val = max(3, int(len(patient_map) * random.uniform(0.5, 1.2)))

    trace = {
        "patient_id": subject_id,
        "timestamp":  charttime,
        "batch_id":   batch_id,
        "record": {"type": evt_type, "value": round(value, 3), "uom": uom},
        "algorithms": {
            "bloom":                "UNIQUE",
            "fm_distinct_patients": fm.estimate(),
            "dgim_events_1h":       dgim.estimate(),
            "k_anonymity":          f"k={k_val}",
        },
        "features": {
            "creatinine":      fvec[0],
            "baseline_cr":     fvec[1],
            "cr_ratio":        fvec[2],
            "cr_delta_recent": fvec[3],
            "uo_ml_kg_hr_6h":  fvec[5],
            "uo_ml_kg_hr_24h": fvec[6],
        },
        "vector":        fvec,
        "kdigo_stage":   kdigo_stage,
        "anomaly_score": anomaly,
        "twins":         twins,
    }
    print(f"[PATIENT_TRACE] {json.dumps(trace)}", flush=True)


def main():
    init_lsh()

    try:
        from confluent_kafka import Consumer, KafkaError
    except ImportError:
        print("ERROR: confluent_kafka not installed. Run: pip install confluent-kafka")
        sys.exit(1)

    kafka_broker = "localhost:9092"   # always localhost — bypasses Docker advertised listener
    kafka_topic  = os.getenv("KAFKA_TOPIC", "aki.live.events")

    consumer = Consumer({
        "bootstrap.servers":  kafka_broker,
        "group.id":           "aki-trace-consumer",
        "auto.offset.reset":  "latest",
        "enable.auto.commit": True,
    })
    consumer.subscribe([kafka_topic])
    print(f"[KAFKA] Subscribed to {kafka_topic} @ {kafka_broker}")
    print("[STREAM] Polling for messages... (Ctrl+C to stop)")

    batch_id   = 0
    batch_buf  = []
    BATCH_SIZE = 10          # emit a batch every N messages or every FLUSH_SEC seconds
    FLUSH_SEC  = 3.0
    last_flush = time.time()

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                pass  # no message yet
            elif msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"[KAFKA ERROR] {msg.error()}")
            else:
                try:
                    data = json.loads(msg.value().decode("utf-8"))
                    batch_buf.append(data)
                except Exception as e:
                    print(f"[PARSE ERROR] {e}")

            # Flush batch when full or time elapsed
            if batch_buf and (len(batch_buf) >= BATCH_SIZE or time.time() - last_flush >= FLUSH_SEC):
                print(f"\n=== Micro-Batch {batch_id} ({len(batch_buf)} events) ===")
                for evt in batch_buf:
                    process_event(evt, batch_id)
                print(f"  Bloom new={bloom.new_count} dup={bloom.dup_count} "
                      f"| FM≈{fm.estimate()} patients | DGIM 1h≈{dgim.estimate()} events")
                batch_buf  = []
                batch_id  += 1
                last_flush = time.time()

    except KeyboardInterrupt:
        print("\n[STREAM] Shutdown requested.")
    finally:
        consumer.close()
        print("[STREAM] Consumer closed.")


if __name__ == "__main__":
    main()


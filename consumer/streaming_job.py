"""
Module 2 v1.2.0: Spark Structured Streaming consumer.
Owner: Karthik

Reads from the three v1.2.0 Kafka topics (labs / vitals / fluids), maintains
per-patient 48h sliding-window state, computes KDIGO stage with the min-baseline
labeling engine, attaches DGIM/FM streaming-algorithm summaries, and writes
PATIENT_TRACE rows to AKI_DB.MART.LIVE_RISK_SCORES.

PATIENT_TRACE JSON schema (v1.2.0):
    {
        "patient_id": <subject_id>,
        "timestamp": <charttime ISO>,
        "kdigo_stage": 0-3,
        "anomaly_score": 0-100,
        "cr_current": <float>,
        "baseline_cr_48h": <float>,
        "cr_ratio": <float>,
        "cr_delta_48h": <float>,
        "urine_velocity_6h": <float>,
        "urine_velocity_12h": <float>,
        "urine_velocity_24h": <float>,
        "bloom": "UNIQUE",          # Silver layer (Module 1) is the dedup boundary
        "fm_distinct_patients": <int>,
        "dgim_events_1h": <int>,
        "triggered_by": <reason>
    }

Bloom dedup is intentionally NOT performed here (v1.2.0: Silver-layer Bloom in
Module 1's `validation/silver_bloom_filter.py`). Every event the consumer sees
has already been deduped, hence "bloom": "UNIQUE".
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime
from typing import Dict, Iterable, Optional

from algorithms import DGIM, FlajoletMartin, ReservoirSampling
from labels.kdigo import classify_kdigo
from state.patient_state import PatientState

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)


KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPICS_DEFAULT = "aki.labs.events,aki.vitals.events,aki.fluids.events"


# ── Per-process aggregates (used inside foreachBatch / Python loop) ─────────

class StreamingAggregates:
    """Holds the streaming-algorithm summaries that decorate every PATIENT_TRACE."""

    def __init__(self):
        self.fm = FlajoletMartin(num_registers=64)
        self.dgim = DGIM(window_sec=3600)
        self.reservoir = ReservoirSampling(k=200)

    def observe(self, subject_id: str, charttime_epoch: float) -> None:
        self.fm.add(subject_id)
        self.dgim.add(charttime_epoch)

    def snapshot(self) -> dict:
        return {
            "fm_distinct_patients": self.fm.estimate(),
            "dgim_events_1h": self.dgim.estimate(),
        }


# ── Per-event handler ────────────────────────────────────────────────────────

def _to_epoch(ct) -> float:
    if isinstance(ct, (int, float)):
        return float(ct)
    if isinstance(ct, datetime):
        return ct.timestamp()
    s = str(ct).replace(" ", "T")
    try:
        return datetime.fromisoformat(s).timestamp()
    except ValueError:
        return time.time()


def build_patient_trace(
    event: dict,
    state: PatientState,
    aggregates: StreamingAggregates,
) -> dict:
    """
    Update per-patient state with `event`, compute KDIGO + score, and produce a
    v1.2.0 PATIENT_TRACE dict ready to be persisted to MART.LIVE_RISK_SCORES.
    """
    subject_id = str(event.get("subject_id"))
    event_type = str(event.get("event_type", ""))
    value = event.get("value")
    charttime = event.get("charttime")

    # State + KDIGO ----------------------------------------------------------
    if value is not None:
        try:
            state.ingest(event_type, float(value), charttime)
        except (TypeError, ValueError):
            log.warning("Skipping non-numeric value: subject=%s value=%r", subject_id, value)
    aggregates.observe(subject_id, _to_epoch(charttime))

    kdigo = classify_kdigo(
        cr_current=state.current_cr(),
        cr_baseline_48h=state.baseline_cr(),
        uo_6h=state.urine_per_kg_hr(6),
        uo_12h=state.urine_per_kg_hr(12),
        uo_24h=state.urine_per_kg_hr(24),
    )

    snap = aggregates.snapshot()
    trace = {
        "patient_id": subject_id,
        "timestamp": str(charttime),
        "kdigo_stage": kdigo.stage,
        "anomaly_score": state.anomaly_score(),
        "cr_current": state.current_cr(),
        "baseline_cr_48h": state.baseline_cr(),
        "cr_ratio": state.cr_ratio(),
        "cr_delta_48h": state.cr_delta_48h(),
        "urine_velocity_6h": state.urine_per_kg_hr(6),
        "urine_velocity_12h": state.urine_per_kg_hr(12),
        "urine_velocity_24h": state.urine_per_kg_hr(24),
        "bloom": "UNIQUE",  # Silver-layer Bloom (Module 1) — see v1.2.0 README
        "fm_distinct_patients": snap["fm_distinct_patients"],
        "dgim_events_1h": snap["dgim_events_1h"],
        "triggered_by": kdigo.triggered_by,
    }
    aggregates.reservoir.add(trace)
    return trace


def process_events(
    events: Iterable[dict],
    states: Optional[Dict[str, PatientState]] = None,
    aggregates: Optional[StreamingAggregates] = None,
) -> Iterable[dict]:
    """
    Consume an iterable of Silver/Kafka events and yield PATIENT_TRACE dicts.

    Used by the Spark `foreachBatch` callback and by the unit tests.
    """
    if states is None:
        states = {}
    if aggregates is None:
        aggregates = StreamingAggregates()

    for event in events:
        sid = str(event.get("subject_id"))
        if sid not in states:
            states[sid] = PatientState(sid)
        yield build_patient_trace(event, states[sid], aggregates)


# ── Spark Structured Streaming entry point ──────────────────────────────────

def run_spark_consumer(
    bootstrap: str = KAFKA_BOOTSTRAP,
    topics: str = TOPICS_DEFAULT,
    checkpoint: str = "tmp/checkpoints/aki_m2_consumer",
    sink: str = "snowflake",
):
    """
    Start the Spark Structured Streaming job.

    Args:
        bootstrap : Kafka bootstrap.servers.
        topics    : Comma-separated topic list (defaults to all 3 v1.2.0 topics).
        checkpoint: Spark checkpoint dir.
        sink      : 'snowflake' (write-back to MART.LIVE_RISK_SCORES) or
                    'console'   (print PATIENT_TRACE rows for debugging).
    """
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, from_json
    from pyspark.sql.types import (
        DoubleType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    spark = (
        SparkSession.builder.appName("aki-m2-streaming-consumer-v1.2.0")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )

    event_schema = StructType(
        [
            StructField("subject_id", StringType(), False),
            StructField("charttime", TimestampType(), True),
            StructField("event_type", StringType(), True),
            StructField("value", DoubleType(), True),
            StructField("valueuom", StringType(), True),
        ]
    )

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", topics)
        .option("startingOffsets", "earliest")
        .load()
    )

    parsed = raw.select(
        from_json(col("value").cast("string"), event_schema).alias("e"),
        col("topic"),
        col("timestamp").alias("kafka_ts"),
    ).select("e.*", "topic", "kafka_ts")

    # ── foreachBatch sink ───────────────────────────────────────────────
    states: Dict[str, PatientState] = {}
    aggregates = StreamingAggregates()

    def _foreach_batch(batch_df, batch_id):
        events = [row.asDict(recursive=True) for row in batch_df.collect()]
        traces = list(process_events(events, states=states, aggregates=aggregates))
        if not traces:
            return
        if sink == "console":
            for t in traces:
                print(json.dumps(t, default=str))
            return
        # Snowflake sink
        from consumer.sink_snowflake import write_traces

        write_traces(traces)

    query = (
        parsed.writeStream.foreachBatch(_foreach_batch)
        .option("checkpointLocation", checkpoint)
        .trigger(processingTime="5 seconds")
        .start()
    )
    log.info("Spark Structured Streaming started (sink=%s, topics=%s)", sink, topics)
    query.awaitTermination()


# ── CLI ─────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Module 2 v1.2.0 — Spark Structured Streaming consumer "
        "(Kafka 3-topic → KDIGO → MART.LIVE_RISK_SCORES)"
    )
    p.add_argument("--bootstrap", default=KAFKA_BOOTSTRAP)
    p.add_argument("--topics", default=TOPICS_DEFAULT)
    p.add_argument(
        "--checkpoint",
        default=os.getenv("AKI_M2_CHECKPOINT", "tmp/checkpoints/aki_m2_consumer"),
    )
    p.add_argument("--sink", choices=["snowflake", "console"], default="snowflake")
    return p.parse_args()


def main():
    args = parse_args()
    run_spark_consumer(
        bootstrap=args.bootstrap,
        topics=args.topics,
        checkpoint=args.checkpoint,
        sink=args.sink,
    )


if __name__ == "__main__":
    main()

"""
Module 2 v1.2.0: End-to-end streaming pipeline tests (in-process, no Kafka).
Owner: Karthik

Drives the same `process_events` function the Spark `foreachBatch` callback
uses, so we exercise: PatientState ingestion → 48h sliding window →
min-baseline KDIGO → DGIM/FM aggregates → PATIENT_TRACE JSON.
"""
from datetime import datetime, timedelta

from consumer.sink_snowflake import build_score_row
from consumer.streaming_job import StreamingAggregates, process_events
from state.patient_state import PatientState


def _ev(subject_id, event_type, value, charttime):
    return {
        "subject_id": subject_id,
        "event_type": event_type,
        "value": value,
        "charttime": charttime.isoformat(),
    }


def test_end_to_end_emits_patient_trace_with_v120_fields():
    t0 = datetime(2150, 1, 1, 0, 0)
    events = [
        _ev("10001", "creatinine", 1.0, t0),
        _ev("10001", "creatinine", 2.5, t0 + timedelta(hours=12)),
        _ev("10002", "urine", 250.0, t0 + timedelta(hours=1)),
        _ev("10001", "urine", 250.0, t0 + timedelta(hours=14)),
    ]
    traces = list(process_events(events))

    # One trace per ingested event.
    assert len(traces) == 4

    # The high-creatinine event for patient 10001 should hit Stage 2 (ratio = 2.5).
    last_creatinine = traces[1]
    assert last_creatinine["patient_id"] == "10001"
    assert last_creatinine["kdigo_stage"] == 2
    assert last_creatinine["cr_ratio"] == 2.5
    assert last_creatinine["baseline_cr_48h"] == 1.0

    # Every trace carries the v1.2.0 fields the dashboard depends on.
    required = {
        "patient_id",
        "timestamp",
        "kdigo_stage",
        "anomaly_score",
        "cr_current",
        "baseline_cr_48h",
        "cr_ratio",
        "cr_delta_48h",
        "urine_velocity_6h",
        "urine_velocity_12h",
        "urine_velocity_24h",
        "bloom",
        "fm_distinct_patients",
        "dgim_events_1h",
        "triggered_by",
    }
    for trace in traces:
        assert required.issubset(trace.keys())
        # Bloom is at Silver — every streamed event is "UNIQUE" by definition.
        assert trace["bloom"] == "UNIQUE"


def test_streaming_aggregates_distinct_count_grows_with_new_subjects():
    aggs = StreamingAggregates()
    for i in range(50):
        aggs.observe(f"patient_{i}", float(i))
    snap = aggs.snapshot()
    # FM is approximate; just sanity-check it grew.
    assert snap["fm_distinct_patients"] > 5
    assert snap["dgim_events_1h"] == 50


def test_build_score_row_returns_correct_arity():
    trace = {
        "patient_id": 10001,
        "timestamp": "2150-01-01T00:00:00",
        "kdigo_stage": 2,
        "anomaly_score": 75.0,
        "cr_current": 2.5,
        "baseline_cr_48h": 1.0,
        "cr_ratio": 2.5,
        "cr_delta_48h": 1.5,
        "urine_velocity_6h": 0.4,
        "urine_velocity_12h": 0.45,
        "urine_velocity_24h": 0.5,
        "bloom": "UNIQUE",
        "fm_distinct_patients": 17,
        "dgim_events_1h": 200,
        "triggered_by": "2.0<=cr_ratio<3.0",
    }
    row = build_score_row(trace)
    assert len(row) == 15
    assert row[0] == 10001
    assert row[2] == 2
    assert row[11] == "UNIQUE"


def test_min_baseline_drives_kdigo_in_streaming_path():
    """If a NEW low creatinine arrives, the baseline drops and ratio rises accordingly."""
    t0 = datetime(2150, 1, 1, 0, 0)
    events = [
        _ev("20001", "creatinine", 1.5, t0),
        _ev("20001", "creatinine", 0.8, t0 + timedelta(hours=4)),   # baseline drops to 0.8
        _ev("20001", "creatinine", 1.7, t0 + timedelta(hours=10)),  # ratio = 1.7 / 0.8 ≈ 2.125
    ]
    traces = list(process_events(events))
    final = traces[-1]
    assert final["baseline_cr_48h"] == 0.8
    assert final["cr_ratio"] > 2.0
    # ratio in [2.0, 3.0) → stage 2
    assert final["kdigo_stage"] == 2

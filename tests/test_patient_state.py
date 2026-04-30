"""
Module 2 v1.2.0: Tests for the 48h sliding-window PatientState.
Owner: Karthik

These tests pin down the v1.2.0 semantics:
    * baseline_cr() is the 48h MIN, not median(first3).
    * cr_ratio = cr_current / baseline_48h_min.
    * Entries older than 48h relative to the latest charttime are expired.
"""
from datetime import datetime, timedelta

import pytest

from state.patient_state import WINDOW_HOURS, PatientState


def test_window_hours_is_48():
    assert WINDOW_HOURS == 48


def test_baseline_cr_is_min_of_window():
    p = PatientState("10001")
    base_t = datetime(2150, 1, 1, 8, 0)
    # 0.8 is the lowest in-window value -> baseline should be 0.8
    p.ingest("creatinine", 1.2, base_t)
    p.ingest("creatinine", 0.8, base_t + timedelta(hours=6))
    p.ingest("creatinine", 1.5, base_t + timedelta(hours=12))
    p.ingest("creatinine", 2.4, base_t + timedelta(hours=24))

    assert p.current_cr() == 2.4
    assert p.baseline_cr() == 0.8
    assert p.cr_ratio() == pytest.approx(2.4 / 0.8, rel=1e-3)
    assert p.cr_delta_48h() == pytest.approx(2.4 - 0.8, rel=1e-3)


def test_old_entries_outside_48h_window_are_expired():
    """An out-of-window low value must not anchor baseline."""
    p = PatientState("10002")
    t0 = datetime(2150, 1, 1, 0, 0)
    # Day 0: low creatinine that should expire once the window slides past 48h
    p.ingest("creatinine", 0.5, t0)
    # Day 3 (72h later) — t0 is now > 48h old and must be expired.
    p.ingest("creatinine", 1.6, t0 + timedelta(hours=72))
    p.ingest("creatinine", 2.0, t0 + timedelta(hours=80))

    # Only the in-window samples (1.6, 2.0) should remain.
    assert len(p.creatinine_history) == 2
    assert p.baseline_cr() == 1.6
    assert p.cr_ratio() == pytest.approx(2.0 / 1.6, rel=1e-3)


def test_urine_per_kg_hr_uses_trailing_window():
    p = PatientState("10003", weight_kg=70.0)
    t0 = datetime(2150, 1, 1, 0, 0)
    # Two 250mL voids in last 6 hours = 500 mL total.
    p.ingest("urine", 250.0, t0 + timedelta(hours=2))
    p.ingest("urine", 250.0, t0 + timedelta(hours=5))
    # Anchor a final event at t0+6h so the 6h trailing window covers both.
    p.ingest("creatinine", 1.0, t0 + timedelta(hours=6))

    # 500 mL / (70 kg * 6 h) ≈ 1.190 mL/kg/hr
    assert p.urine_per_kg_hr(6) == pytest.approx(500.0 / (70 * 6), rel=1e-3)


def test_urine_velocity_excludes_events_outside_window():
    p = PatientState("10004", weight_kg=70.0)
    t0 = datetime(2150, 1, 1, 0, 0)
    # An old void 12h before the anchor must NOT count toward the 6h rate.
    p.ingest("urine", 1000.0, t0)
    p.ingest("urine", 70.0, t0 + timedelta(hours=10))      # in 6h window
    p.ingest("creatinine", 1.0, t0 + timedelta(hours=12))  # anchor

    # 6h window: only the 70 mL void counts → 70 / (70 * 6) = 0.1667 mL/kg/hr
    assert p.urine_per_kg_hr(6) == pytest.approx(70.0 / (70 * 6), rel=1e-3)
    # 12h window: both voids count → 1070 mL / (70 * 12)
    assert p.urine_per_kg_hr(12) == pytest.approx(1070.0 / (70 * 12), rel=1e-3)


def test_to_features_emits_v120_keys():
    p = PatientState("10005")
    t0 = datetime(2150, 1, 1, 0, 0)
    p.ingest("creatinine", 1.0, t0)
    p.ingest("creatinine", 2.5, t0 + timedelta(hours=12))

    feats = p.to_features()
    for key in (
        "subject_id",
        "last_charttime",
        "current_cr",
        "baseline_cr_48h",
        "cr_ratio",
        "cr_delta_48h",
        "uo_6h",
        "uo_12h",
        "uo_24h",
        "window_hours",
    ):
        assert key in feats
    assert feats["window_hours"] == 48
    assert feats["baseline_cr_48h"] == 1.0
    assert feats["cr_ratio"] == pytest.approx(2.5, rel=1e-3)


def test_no_history_returns_none_safely():
    p = PatientState("10006")
    assert p.current_cr() is None
    assert p.baseline_cr() is None
    assert p.cr_ratio() is None
    assert p.cr_delta_48h() is None
    assert p.urine_per_kg_hr(6) is None

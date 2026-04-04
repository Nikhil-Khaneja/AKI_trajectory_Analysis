"""
Module 2 v1.2.0: KDIGO labeling-engine tests.
Owner: Karthik

Pin down the v1.2.0 staging logic — every branch of compute_kdigo is covered.
"""
from datetime import datetime, timedelta

import pytest

from labels.kdigo import classify_kdigo, compute_kdigo, stage_from_state
from state.patient_state import PatientState


# ── Stage 3 cases ────────────────────────────────────────────────────────────

def test_stage3_ratio_ge_3():
    r = classify_kdigo(cr_current=3.6, cr_baseline_48h=1.0, uo_6h=1.0, uo_12h=1.0, uo_24h=1.0)
    assert r.stage == 3 and r.triggered_by == "cr_ratio>=3.0"


def test_stage3_cr_ge_4():
    r = classify_kdigo(cr_current=4.5, cr_baseline_48h=2.5, uo_6h=1.0, uo_12h=1.0, uo_24h=1.0)
    # ratio = 1.8 (would be stage 1 by ratio), but absolute cr_current >= 4.0 forces stage 3.
    assert r.stage == 3 and r.triggered_by == "cr_current>=4.0"


def test_stage3_uo_24h_below_03():
    r = classify_kdigo(cr_current=1.0, cr_baseline_48h=1.0, uo_6h=1.0, uo_12h=1.0, uo_24h=0.25)
    assert r.stage == 3 and r.triggered_by == "uo_24h<0.3"


# ── Stage 2 cases ────────────────────────────────────────────────────────────

def test_stage2_ratio_2_to_3():
    r = classify_kdigo(cr_current=2.4, cr_baseline_48h=1.0, uo_6h=1.0, uo_12h=1.0, uo_24h=1.0)
    assert r.stage == 2 and r.triggered_by == "2.0<=cr_ratio<3.0"


def test_stage2_uo_12h_below_05():
    r = classify_kdigo(cr_current=1.0, cr_baseline_48h=1.0, uo_6h=1.0, uo_12h=0.4, uo_24h=1.0)
    assert r.stage == 2 and r.triggered_by == "uo_12h<0.5"


# ── Stage 1 cases ────────────────────────────────────────────────────────────

def test_stage1_delta_ge_03():
    r = classify_kdigo(cr_current=1.4, cr_baseline_48h=1.0, uo_6h=1.0, uo_12h=1.0, uo_24h=1.0)
    # ratio = 1.4 (< 1.5) but absolute delta = 0.4 >= 0.3 → stage 1
    assert r.stage == 1 and r.triggered_by == "cr_delta_48h>=0.3"


def test_stage1_ratio_1p5_to_2():
    # Pick a small baseline so 1.5x ratio still leaves delta < 0.3 (isolates the ratio branch).
    r = classify_kdigo(cr_current=0.65, cr_baseline_48h=0.4, uo_6h=1.0, uo_12h=1.0, uo_24h=1.0)
    assert r.stage == 1 and r.triggered_by == "1.5<=cr_ratio<2.0"


def test_stage1_uo_6h_below_05():
    r = classify_kdigo(cr_current=1.0, cr_baseline_48h=1.0, uo_6h=0.4, uo_12h=1.0, uo_24h=1.0)
    assert r.stage == 1 and r.triggered_by == "uo_6h<0.5"


# ── Stage 0 ──────────────────────────────────────────────────────────────────

def test_stage0_no_criteria():
    r = classify_kdigo(cr_current=1.1, cr_baseline_48h=1.0, uo_6h=1.0, uo_12h=1.0, uo_24h=1.0)
    assert r.stage == 0


def test_stage0_when_inputs_unknown():
    assert compute_kdigo(None, None, None, None, None) == 0


# ── Stage precedence: most severe wins ───────────────────────────────────────

def test_stage_precedence_picks_highest():
    # ratio=1.5 (stage 1) AND uo_12h=0.4 (stage 2) → stage 2 wins
    assert (
        compute_kdigo(cr_current=1.5, cr_baseline_48h=1.0, uo_6h=0.4, uo_12h=0.4, uo_24h=1.0)
        == 2
    )
    # uo_24h=0.2 (stage 3) trumps everything
    assert (
        compute_kdigo(cr_current=1.5, cr_baseline_48h=1.0, uo_6h=0.4, uo_12h=0.4, uo_24h=0.2)
        == 3
    )


# ── Integration with PatientState (uses 48h MIN baseline) ────────────────────

def test_stage_from_state_uses_min_baseline():
    p = PatientState("10001", weight_kg=70.0)
    t0 = datetime(2150, 1, 1, 0, 0)
    p.ingest("creatinine", 1.0, t0)                          # baseline = 1.0
    p.ingest("creatinine", 0.9, t0 + timedelta(hours=6))     # baseline drops to 0.9
    p.ingest("creatinine", 1.4, t0 + timedelta(hours=12))    # ratio = 1.4 / 0.9 ≈ 1.55 → stage 1

    result = stage_from_state(p)
    assert result.stage == 1
    assert result.ratio == pytest.approx(1.4 / 0.9, rel=1e-3)
    assert result.delta_48h == pytest.approx(1.4 - 0.9, rel=1e-3)

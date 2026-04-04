"""
Module 2 v1.2.0: Real-time KDIGO labeling engine (min-baseline).
Owner: Karthik

KDIGO AKI staging using the v1.2.0 48h-min creatinine baseline. The thresholds
match the published KDIGO Clinical Practice Guideline for Acute Kidney Injury
(2012), but the comparison reference is now the 48h-window minimum produced by
`state.patient_state.PatientState.baseline_cr()`, NOT a median of early values.

Inputs are raw scalars so this module can also be called from batch jobs
(Module 1 silver), notebooks, and unit tests without instantiating a
streaming PatientState.

    Stage 3:  ratio ≥ 3.0   OR  Cr_current ≥ 4.0   OR  uo_24h < 0.3
    Stage 2:  2.0 ≤ ratio < 3.0                     OR  uo_12h < 0.5
    Stage 1:  delta_48h ≥ 0.3   OR  1.5 ≤ ratio < 2.0   OR  uo_6h < 0.5
    Stage 0:  otherwise

Where:
    ratio     = cr_current / cr_baseline_48h
    delta_48h = cr_current - cr_baseline_48h
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class KDIGOResult:
    stage: int
    ratio: Optional[float]
    delta_48h: Optional[float]
    triggered_by: str

    def as_dict(self) -> dict:
        return {
            "stage": self.stage,
            "ratio": self.ratio,
            "delta_48h": self.delta_48h,
            "triggered_by": self.triggered_by,
        }


def compute_kdigo(
    cr_current: Optional[float],
    cr_baseline_48h: Optional[float],
    uo_6h: Optional[float],
    uo_12h: Optional[float],
    uo_24h: Optional[float],
) -> int:
    """
    Compute KDIGO stage (0–3) from v1.2.0 inputs.

    Parameters
    ----------
    cr_current : current creatinine (mg/dL)
    cr_baseline_48h : 48h-window MIN creatinine (mg/dL)
    uo_6h, uo_12h, uo_24h : urine output rate (mL/kg/hr) over those trailing windows
    """
    return classify_kdigo(cr_current, cr_baseline_48h, uo_6h, uo_12h, uo_24h).stage


def classify_kdigo(
    cr_current: Optional[float],
    cr_baseline_48h: Optional[float],
    uo_6h: Optional[float],
    uo_12h: Optional[float],
    uo_24h: Optional[float],
) -> KDIGOResult:
    """Same as compute_kdigo but also returns the trigger reason for traceability."""
    ratio: Optional[float] = None
    delta_48h: Optional[float] = None
    if cr_current is not None and cr_baseline_48h and cr_baseline_48h > 0:
        ratio = cr_current / cr_baseline_48h
        delta_48h = cr_current - cr_baseline_48h

    # ── Stage 3 ──────────────────────────────────────────────────────────
    if ratio is not None and ratio >= 3.0:
        return KDIGOResult(3, ratio, delta_48h, "cr_ratio>=3.0")
    if cr_current is not None and cr_current >= 4.0:
        return KDIGOResult(3, ratio, delta_48h, "cr_current>=4.0")
    if uo_24h is not None and uo_24h < 0.3:
        return KDIGOResult(3, ratio, delta_48h, "uo_24h<0.3")

    # ── Stage 2 ──────────────────────────────────────────────────────────
    if ratio is not None and 2.0 <= ratio < 3.0:
        return KDIGOResult(2, ratio, delta_48h, "2.0<=cr_ratio<3.0")
    if uo_12h is not None and uo_12h < 0.5:
        return KDIGOResult(2, ratio, delta_48h, "uo_12h<0.5")

    # ── Stage 1 ──────────────────────────────────────────────────────────
    if delta_48h is not None and delta_48h >= 0.3:
        return KDIGOResult(1, ratio, delta_48h, "cr_delta_48h>=0.3")
    if ratio is not None and 1.5 <= ratio < 2.0:
        return KDIGOResult(1, ratio, delta_48h, "1.5<=cr_ratio<2.0")
    if uo_6h is not None and uo_6h < 0.5:
        return KDIGOResult(1, ratio, delta_48h, "uo_6h<0.5")

    return KDIGOResult(0, ratio, delta_48h, "no_aki_criteria")


def stage_from_state(state) -> KDIGOResult:
    """Convenience wrapper: pull v1.2.0 inputs out of a PatientState."""
    return classify_kdigo(
        cr_current=state.current_cr(),
        cr_baseline_48h=state.baseline_cr(),
        uo_6h=state.urine_per_kg_hr(6),
        uo_12h=state.urine_per_kg_hr(12),
        uo_24h=state.urine_per_kg_hr(24),
    )

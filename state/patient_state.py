"""
Module 2 v1.2.0: Per-patient 48-hour sliding-window clinical state.
Owner: Karthik

v1.2.0 changes (vs v1.1):
    * Histories are bounded by TIME (48h), not by deque maxlen=20.
    * baseline_cr is the MIN over the 48h window (not median of first 3).
    * cr_ratio = cr_current / baseline_48h_min.
    * cr_delta_48h = cr_current - baseline_48h_min.
    * Urine velocity is computed over arbitrary trailing windows (6h / 12h / 24h)
      using the urine totals captured in that window.

The window is enforced relative to the LATEST event's charttime so that batch
replay (Module 1 silver → Kafka) and live ingestion both produce identical
state — see `_expire`.
"""

from __future__ import annotations

from collections import deque
from datetime import datetime, timedelta
from typing import Deque, Optional, Tuple, Union

WINDOW_HOURS: int = 48

TimeLike = Union[str, datetime]
HistoryEntry = Tuple[datetime, float]


def _parse_ts(ts: TimeLike) -> datetime:
    """Coerce a charttime input into a naive datetime (no tz arithmetic surprises)."""
    if isinstance(ts, datetime):
        return ts.replace(tzinfo=None) if ts.tzinfo else ts
    s = str(ts).strip().replace("Z", "+00:00")
    # Tolerate "2150-01-01 08:00", "2150-01-01T08:00:00", and ISO with offset.
    s = s.replace(" ", "T")
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        dt = datetime.strptime(s[:19], "%Y-%m-%dT%H:%M:%S")
    return dt.replace(tzinfo=None) if dt.tzinfo else dt


class PatientState:
    """
    Rolling 48h state for a single ICU patient.

    Histories store (charttime, value) tuples. On every ingest we expire any
    entries older than (last_charttime - 48h).
    """

    __slots__ = (
        "subject_id",
        "weight_kg",
        "creatinine_history",
        "urine_history",
        "event_count",
        "last_charttime",
    )

    def __init__(self, subject_id: str, weight_kg: float = 70.0):
        self.subject_id: str = str(subject_id)
        self.weight_kg: float = float(weight_kg)
        self.creatinine_history: Deque[HistoryEntry] = deque()
        self.urine_history: Deque[HistoryEntry] = deque()
        self.event_count: int = 0
        self.last_charttime: Optional[datetime] = None

    # ── Ingest ────────────────────────────────────────────────────────────

    def ingest(self, event_type: str, value: float, charttime: TimeLike) -> None:
        """Record a new clinical event and prune anything older than 48h."""
        ct = _parse_ts(charttime)
        # Sliding window is anchored at the latest event we've seen. For
        # out-of-order events we still use the max charttime as anchor.
        if self.last_charttime is None or ct > self.last_charttime:
            self.last_charttime = ct

        et = (event_type or "").strip().lower()
        if et in ("creatinine", "cr"):
            self.creatinine_history.append((ct, float(value)))
        elif et in ("urine", "urine_output", "uo"):
            self.urine_history.append((ct, float(value)))

        self.event_count += 1
        self._expire(self.last_charttime)

    def _expire(self, anchor: datetime) -> None:
        """Drop entries whose charttime is older than anchor - WINDOW_HOURS."""
        cutoff = anchor - timedelta(hours=WINDOW_HOURS)
        while self.creatinine_history and self.creatinine_history[0][0] < cutoff:
            self.creatinine_history.popleft()
        while self.urine_history and self.urine_history[0][0] < cutoff:
            self.urine_history.popleft()

    # ── Creatinine accessors ──────────────────────────────────────────────

    def current_cr(self) -> Optional[float]:
        if not self.creatinine_history:
            return None
        return self.creatinine_history[-1][1]

    def baseline_cr(self) -> Optional[float]:
        """48-hour MIN baseline (v1.2.0 — replaces the v1.1 median-of-first-3)."""
        if not self.creatinine_history:
            return None
        return min(v for _, v in self.creatinine_history)

    def cr_ratio(self) -> Optional[float]:
        """cr_current / baseline_48h_min."""
        cur = self.current_cr()
        base = self.baseline_cr()
        if cur is None or not base or base <= 0:
            return None
        return round(cur / base, 4)

    def cr_delta_48h(self) -> Optional[float]:
        """cr_current - baseline_48h_min."""
        cur = self.current_cr()
        base = self.baseline_cr()
        if cur is None or base is None:
            return None
        return round(cur - base, 4)

    # ── Urine accessors ───────────────────────────────────────────────────

    def urine_per_kg_hr(self, hours: int = 6) -> Optional[float]:
        """
        Urine output rate in mL/kg/hr over the trailing `hours` window
        (anchored at last_charttime).

        Returns None if no urine measurements lie inside that window.
        """
        if not self.urine_history or self.last_charttime is None:
            return None
        cutoff = self.last_charttime - timedelta(hours=hours)
        total_ml = sum(v for t, v in self.urine_history if t >= cutoff)
        # If all urine entries are older than `hours`, the trailing window is empty.
        if total_ml == 0 and not any(t >= cutoff for t, _ in self.urine_history):
            return None
        return round(total_ml / (self.weight_kg * float(hours)), 4)

    # ── Convenience snapshot ──────────────────────────────────────────────

    def to_features(self) -> dict:
        """Materialise the v1.2.0 PATIENT_TRACE feature block for this patient."""
        return {
            "subject_id": self.subject_id,
            "last_charttime": self.last_charttime.isoformat() if self.last_charttime else None,
            "current_cr": self.current_cr(),
            "baseline_cr_48h": self.baseline_cr(),
            "cr_ratio": self.cr_ratio(),
            "cr_delta_48h": self.cr_delta_48h(),
            "uo_6h": self.urine_per_kg_hr(6),
            "uo_12h": self.urine_per_kg_hr(12),
            "uo_24h": self.urine_per_kg_hr(24),
            "event_count": self.event_count,
            "window_hours": WINDOW_HOURS,
            "cr_window_size": len(self.creatinine_history),
            "uo_window_size": len(self.urine_history),
        }

    def anomaly_score(self) -> float:
        """Composite 0–100 risk score driven by ratio, delta and UO_6h."""
        score = 0.0
        ratio = self.cr_ratio()
        delta = self.cr_delta_48h()
        uo6 = self.urine_per_kg_hr(6)

        if ratio is not None:
            score += min(ratio * 20, 50)
        if delta is not None and delta >= 0.3:
            score += 15
        elif delta is not None and delta >= 0.1:
            score += 8
        if uo6 is not None:
            if uo6 < 0.3:
                score += 30
            elif uo6 < 0.5:
                score += 15

        return min(round(score, 1), 100.0)

    def __repr__(self) -> str:
        return (
            f"PatientState(subject_id={self.subject_id!r}, "
            f"cr_window={len(self.creatinine_history)}, "
            f"uo_window={len(self.urine_history)}, "
            f"last={self.last_charttime})"
        )

"""
Module 2 v1.2.0: DGIM-style approximate sliding-window event count.
Owner: Karthik

Used by the streaming consumer to expose `dgim_events_1h` in the PATIENT_TRACE
JSON output without retaining the full per-patient event history.

The classic DGIM algorithm uses log-bucketed timestamps for O(log² N) memory
on a binary stream. For our event-count use case we keep the same public API
but back it with a deque of timestamps — equivalent semantics, simpler code,
and the data volume in this project (one ICU patient, ~100 events / 24h)
keeps memory negligible.
"""

from __future__ import annotations

from collections import deque
from typing import Deque


class DGIM:
    """
    Approximate count of events seen within the last `window_sec` seconds.

    Time inputs are unix epoch seconds. Calling estimate() is O(1).
    """

    __slots__ = ("window_sec", "_timestamps")

    def __init__(self, window_sec: int = 3600):
        if window_sec <= 0:
            raise ValueError("window_sec must be > 0")
        self.window_sec: int = int(window_sec)
        self._timestamps: Deque[float] = deque()

    def add(self, timestamp: float) -> None:
        """Record an event at unix `timestamp` and expire any out-of-window entries."""
        self._timestamps.append(float(timestamp))
        self._expire(float(timestamp))

    def _expire(self, current_ts: float) -> None:
        cutoff = current_ts - self.window_sec
        while self._timestamps and self._timestamps[0] < cutoff:
            self._timestamps.popleft()

    def estimate(self) -> int:
        """Approximate count of events still in the trailing window."""
        return len(self._timestamps)

    def __len__(self) -> int:
        return self.estimate()

    def __repr__(self) -> str:
        return f"DGIM(window_sec={self.window_sec}, count={self.estimate()})"

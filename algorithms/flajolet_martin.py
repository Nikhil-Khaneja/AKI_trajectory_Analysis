"""
Module 2 v1.2.0: Flajolet-Martin distinct-count estimator.
Owner: Karthik

Used by the streaming consumer to expose `fm_distinct_patients` — the number
of distinct subject_ids that have appeared on any of the three Kafka topics —
without storing the patient set itself.

Memory: O(K) where K = number of independent hash registers (default 64).
Estimate is the harmonic mean of 2^max_trailing_zeros across registers,
divided by the standard FM correction factor (≈0.77351).
"""

from __future__ import annotations

import hashlib
from typing import Iterable, List

PHI = 0.77351  # Flajolet-Martin correction constant


def _trailing_zeros(x: int) -> int:
    if x == 0:
        return 0
    return (x & -x).bit_length() - 1


def _hash_register(value: str, register: int) -> int:
    h = hashlib.sha1(f"{register}|{value}".encode()).digest()
    # Take 32 bits as an unsigned int.
    return int.from_bytes(h[:4], "big")


class FlajoletMartin:
    """Distinct-element count estimator with K independent hash registers."""

    __slots__ = ("_num_registers", "_max_zeros")

    def __init__(self, num_registers: int = 64):
        if num_registers <= 0:
            raise ValueError("num_registers must be > 0")
        self._num_registers: int = int(num_registers)
        self._max_zeros: List[int] = [0] * self._num_registers

    def add(self, value: object) -> None:
        """Hash `value` into all K registers and update each register's max trailing zeros."""
        s = str(value)
        for i in range(self._num_registers):
            h = _hash_register(s, i)
            tz = _trailing_zeros(h)
            if tz > self._max_zeros[i]:
                self._max_zeros[i] = tz

    def update_many(self, values: Iterable[object]) -> None:
        for v in values:
            self.add(v)

    def estimate(self) -> int:
        """Return the corrected harmonic-mean estimate of distinct values."""
        # Harmonic mean of 2^z across all registers, divided by PHI.
        if not any(self._max_zeros):
            return 0
        denom = sum(2.0 ** -z for z in self._max_zeros)
        if denom == 0:
            return 0
        harmonic = self._num_registers / denom
        return int(round(harmonic / PHI))

    def __repr__(self) -> str:
        return f"FlajoletMartin(num_registers={self._num_registers}, estimate={self.estimate()})"

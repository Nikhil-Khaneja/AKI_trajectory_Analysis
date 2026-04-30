"""
Module 2 v1.2.0: Reservoir sampling for balanced mini-batch construction.
Owner: Karthik

Algorithm R (Vitter, 1985): each item in the stream has equal probability
k / n of ending up in the final sample of size k.

The streaming consumer uses this to keep a uniformly-random sample of recent
patient traces for the live dashboard (so we never bias toward the most
recent N events).
"""

from __future__ import annotations

import random
from typing import Any, Generic, List, TypeVar

T = TypeVar("T")


class ReservoirSampling(Generic[T]):
    """Uniform random sample of size k from a stream of unknown length."""

    __slots__ = ("k", "_reservoir", "_n", "_rng")

    def __init__(self, k: int = 100, seed: int | None = None):
        if k <= 0:
            raise ValueError("k must be > 0")
        self.k: int = int(k)
        self._reservoir: List[T] = []
        self._n: int = 0
        self._rng = random.Random(seed) if seed is not None else random

    def add(self, item: T) -> None:
        """Offer `item` to the reservoir."""
        self._n += 1
        if len(self._reservoir) < self.k:
            self._reservoir.append(item)
            return
        # With probability k/n, replace a random element.
        j = self._rng.randint(0, self._n - 1)
        if j < self.k:
            self._reservoir[j] = item

    def get_sample(self) -> List[T]:
        """Return a copy of the current sample (size ≤ k)."""
        return list(self._reservoir)

    def reset(self) -> None:
        self._reservoir.clear()
        self._n = 0

    def __len__(self) -> int:
        return len(self._reservoir)

    def __repr__(self) -> str:
        return f"ReservoirSampling(k={self.k}, seen={self._n}, sample_size={len(self._reservoir)})"

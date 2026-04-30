"""
Module 2 v1.2.0 — streaming algorithms package.

DGIM, Flajolet-Martin and Reservoir Sampling are used by the streaming
consumer to produce O(1) summaries (event counts, distinct patient counts,
balanced mini-batch samples) without retaining raw event history.

Note: Bloom Filter is intentionally NOT here — v1.2.0 places Silver-layer
deduplication in Module 1 (`validation/silver_bloom_filter.py`).
"""
from algorithms.dgim import DGIM
from algorithms.flajolet_martin import FlajoletMartin
from algorithms.reservoir_sampling import ReservoirSampling

__all__ = ["DGIM", "FlajoletMartin", "ReservoirSampling"]

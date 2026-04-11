"""
Module 2 v1.2.0: Tests for streaming algorithms.
Owner: Karthik

Covers DGIM (sliding window count), Flajolet-Martin (distinct count),
and Reservoir Sampling (uniform random sample).

NOTE: Bloom Filter is tested under tests/test_bloom_filter.py — that's the
SILVER-layer SilverBloomFilter from Module 1, not a streaming dedup.
"""
import random

import pytest

from algorithms import DGIM, FlajoletMartin, ReservoirSampling


# ── DGIM ─────────────────────────────────────────────────────────────────────

def test_dgim_counts_events_in_window():
    dgim = DGIM(window_sec=10)
    dgim.add(0.0)
    dgim.add(5.0)
    dgim.add(9.0)
    assert dgim.estimate() == 3


def test_dgim_expires_old_events():
    dgim = DGIM(window_sec=10)
    dgim.add(0.0)
    dgim.add(5.0)
    dgim.add(9.0)
    # Adding an event 11s in expires t=0.
    dgim.add(11.0)
    assert dgim.estimate() == 3


def test_dgim_full_window_drain():
    dgim = DGIM(window_sec=5)
    for i in range(5):
        dgim.add(float(i))
    # Jump ahead — every prior event must expire.
    dgim.add(100.0)
    assert dgim.estimate() == 1


def test_dgim_invalid_window():
    with pytest.raises(ValueError):
        DGIM(window_sec=0)


# ── Flajolet-Martin ──────────────────────────────────────────────────────────

def test_flajolet_martin_grows_with_distinct_inputs():
    fm = FlajoletMartin(num_registers=64)
    for i in range(10_000):
        fm.add(f"patient_{i}")
    est = fm.estimate()
    # Expect roughly 10_000; allow generous slack because this is a probabilistic estimator.
    assert 2_000 < est < 50_000


def test_flajolet_martin_dedup_is_idempotent():
    """Repeated additions of the same value don't inflate the estimate."""
    fm = FlajoletMartin(num_registers=32)
    for _ in range(1_000):
        fm.add("only-one-patient")
    assert fm.estimate() <= 8


def test_flajolet_martin_zero_when_empty():
    fm = FlajoletMartin()
    assert fm.estimate() == 0


# ── Reservoir Sampling ───────────────────────────────────────────────────────

def test_reservoir_holds_at_most_k():
    rs: ReservoirSampling[int] = ReservoirSampling(k=10)
    for i in range(100):
        rs.add(i)
    sample = rs.get_sample()
    assert len(sample) == 10


def test_reservoir_keeps_everything_below_k():
    rs: ReservoirSampling[int] = ReservoirSampling(k=10)
    for i in range(5):
        rs.add(i)
    assert sorted(rs.get_sample()) == [0, 1, 2, 3, 4]


def test_reservoir_invalid_k():
    with pytest.raises(ValueError):
        ReservoirSampling(k=0)


def test_reservoir_uniform_distribution_smoke():
    """Each element from a small stream should appear in the sample with roughly equal frequency."""
    counts = {i: 0 for i in range(20)}
    trials = 4_000
    rng = random.Random(7)
    for _ in range(trials):
        rs: ReservoirSampling[int] = ReservoirSampling(k=5, seed=rng.randint(0, 2**32 - 1))
        for i in range(20):
            rs.add(i)
        for x in rs.get_sample():
            counts[x] += 1
    # Each element should appear ~ trials * 5/20 = 1000 times. Allow ±25%.
    for i, c in counts.items():
        assert 750 < c < 1250, f"element {i} skewed: count={c}"

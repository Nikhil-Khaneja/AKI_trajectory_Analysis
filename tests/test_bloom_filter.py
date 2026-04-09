"""
Module 1 v1.2.0: pytest suite for SilverBloomFilter.
Owner: Nikhil

Tests:
  - New key returns UNIQUE
  - Replayed key returns DUP
  - Different keys all return UNIQUE
  - Stats counters are correct
  - make_key produces correct format
  - Thread safety: concurrent inserts don't corrupt state
  - Reset clears all state
"""

import threading
import sys
from pathlib import Path
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "validation"))

from silver_bloom_filter import SilverBloomFilter


@pytest.fixture
def bloom():
    """Fresh SilverBloomFilter for each test."""
    return SilverBloomFilter(window_days=7)


class TestBloomFilterBasicBehaviour:

    def test_new_key_returns_unique(self, bloom):
        key = bloom.make_key(10001, "2150-01-01 08:00:00", "creatinine", 1.2)
        assert bloom.check_and_insert(key) == "UNIQUE"

    def test_replay_returns_dup(self, bloom):
        key = bloom.make_key(10001, "2150-01-01 08:00:00", "creatinine", 1.2)
        bloom.check_and_insert(key)           # first insertion
        assert bloom.check_and_insert(key) == "DUP"   # replay

    def test_different_keys_all_unique(self, bloom):
        keys = [
            bloom.make_key(10001, "2150-01-01 08:00:00", "creatinine", 1.2),
            bloom.make_key(10001, "2150-01-01 09:00:00", "creatinine", 1.4),  # different time
            bloom.make_key(10001, "2150-01-01 08:00:00", "urine", 300.0),     # different type
            bloom.make_key(10002, "2150-01-01 08:00:00", "creatinine", 1.2),  # different patient
        ]
        for key in keys:
            assert bloom.check_and_insert(key) == "UNIQUE", f"Expected UNIQUE for {key}"

    def test_value_difference_treated_as_unique(self, bloom):
        """Same patient+time+type but different value = different event."""
        k1 = bloom.make_key(10001, "2150-01-01 08:00:00", "creatinine", 1.2)
        k2 = bloom.make_key(10001, "2150-01-01 08:00:00", "creatinine", 1.3)
        assert bloom.check_and_insert(k1) == "UNIQUE"
        assert bloom.check_and_insert(k2) == "UNIQUE"


class TestBloomFilterStats:

    def test_stats_after_inserts(self, bloom):
        keys = [
            bloom.make_key(10001, "2150-01-01 08:00:00", "creatinine", 1.2),
            bloom.make_key(10002, "2150-01-01 09:00:00", "urine", 300.0),
            bloom.make_key(10001, "2150-01-01 08:00:00", "creatinine", 1.2),  # DUP
        ]
        for key in keys:
            bloom.check_and_insert(key)

        stats = bloom.get_stats()
        assert stats["new_count"] == 2
        assert stats["dup_count"] == 1
        assert stats["total_checked"] == 3
        assert stats["dup_rate_pct"] == pytest.approx(33.33, abs=0.01)

    def test_empty_stats_returns_zero_dup_rate(self, bloom):
        stats = bloom.get_stats()
        assert stats["dup_rate_pct"] == 0.0
        assert stats["total_checked"] == 0


class TestBloomFilterMakeKey:

    def test_key_format_string_charttime(self, bloom):
        key = bloom.make_key(10001, "2150-01-01 08:00:00", "creatinine", 1.2)
        assert key == "10001|2150-01-01 08:00:00|creatinine|1.2"

    def test_key_format_datetime_charttime(self, bloom):
        dt = datetime(2150, 1, 1, 8, 0, 0)
        key = bloom.make_key(10001, dt, "creatinine", 1.2)
        assert key == "10001|2150-01-01 08:00:00|creatinine|1.2"

    def test_key_format_matches_spec(self, bloom):
        """Key must match spec: f'{subject_id}|{charttime}|{event_type}|{value}'"""
        key = bloom.make_key("10001", "2150-01-01 08:00:00", "urine", "450.0")
        parts = key.split("|")
        assert len(parts) == 4
        assert parts[0] == "10001"
        assert parts[2] == "urine"


class TestBloomFilterReset:

    def test_reset_clears_stats(self, bloom):
        key = bloom.make_key(10001, "2150-01-01 08:00:00", "creatinine", 1.2)
        bloom.check_and_insert(key)
        bloom.check_and_insert(key)
        bloom.reset()
        stats = bloom.get_stats()
        assert stats["new_count"] == 0
        assert stats["dup_count"] == 0

    def test_reset_allows_reinsert_as_unique(self, bloom):
        key = bloom.make_key(10001, "2150-01-01 08:00:00", "creatinine", 1.2)
        bloom.check_and_insert(key)
        bloom.reset()
        assert bloom.check_and_insert(key) == "UNIQUE"


class TestBloomFilterThreadSafety:

    def test_concurrent_inserts_no_corruption(self, bloom):
        """1000 concurrent threads inserting unique keys — all must return UNIQUE."""
        results = []
        lock = threading.Lock()

        def insert(i):
            key = bloom.make_key(i, "2150-01-01 08:00:00", "creatinine", 1.0)
            result = bloom.check_and_insert(key)
            with lock:
                results.append(result)

        threads = [threading.Thread(target=insert, args=(i,)) for i in range(1000)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert all(r == "UNIQUE" for r in results), (
            f"Expected all UNIQUE, got: {set(results)}"
        )
        stats = bloom.get_stats()
        assert stats["new_count"] == 1000
        assert stats["dup_count"] == 0


class TestBloomFilterTTLPruning:

    def test_expired_timestamps_pruned(self, bloom):
        """Timestamps older than 7 days should be pruned from sidecar dict."""
        key = bloom.make_key(10001, "2150-01-01 08:00:00", "creatinine", 1.2)
        bloom.check_and_insert(key)
        # Manually backdate the timestamp to simulate expiry
        old_ts = datetime.now(tz=timezone.utc) - timedelta(days=8)
        bloom._timestamps[key] = old_ts
        # Trigger pruning
        bloom._prune_expired()
        assert key not in bloom._timestamps

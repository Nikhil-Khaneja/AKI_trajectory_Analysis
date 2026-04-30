"""
Module 1 v1.2.0: Silver Bloom Filter — 7-day historical deduplication.
Owner: Nikhil

Purpose:
    Prevent duplicate rows in Snowflake SILVER when Kafka replays events.
    Fingerprints all event keys seen in the last 7 days.

Key format:
    f"{subject_id}|{charttime}|{event_type}|{value}"

Behaviour:
    - check_and_insert(key) -> "UNIQUE" | "DUP"
    - UNIQUE: key added to Bloom; caller proceeds with Silver INSERT
    - DUP:    key already seen; caller skips INSERT and logs

v1.2.0: Bloom Filter lives at SILVER layer (not in streaming consumer).
"""

import logging
import threading
from datetime import datetime, timedelta, timezone
from typing import Literal

try:
    from pybloom_live import ScalableBloomFilter
except ImportError:
    raise ImportError(
        "pybloom_live is required: pip install pybloom-live"
    )

log = logging.getLogger(__name__)

WINDOW_DAYS = 7
BLOOM_ERROR_RATE = 0.01
BLOOM_INITIAL_CAPACITY = 1_000_000
PRUNE_EVERY_N_CHECKS = 10_000


class SilverBloomFilter:
    """
    Thread-safe Bloom Filter for Silver layer deduplication.

    Example:
        bf = SilverBloomFilter()
        result = bf.check_and_insert("10001|2150-01-01 08:00|creatinine|1.2")
        # -> "UNIQUE" first time, "DUP" on replay
    """

    def __init__(
        self,
        window_days: int = WINDOW_DAYS,
        error_rate: float = BLOOM_ERROR_RATE,
        initial_capacity: int = BLOOM_INITIAL_CAPACITY,
    ):
        self.window_days = window_days
        self._bloom = ScalableBloomFilter(
            initial_capacity=initial_capacity,
            error_rate=error_rate,
            mode=ScalableBloomFilter.LARGE_SET_GROWTH,
        )
        self._timestamps: dict[str, datetime] = {}
        self._lock = threading.Lock()
        self._check_count = 0
        self._new_count = 0
        self._dup_count = 0

    @staticmethod
    def make_key(
        subject_id,
        charttime,
        event_type: str,
        value,
    ) -> str:
        """Build canonical fingerprint: '{subject_id}|{charttime}|{event_type}|{value}'."""
        if isinstance(charttime, datetime):
            charttime = charttime.strftime("%Y-%m-%d %H:%M:%S")
        return f"{subject_id}|{charttime}|{event_type}|{value}"

    def check_and_insert(self, key: str) -> Literal["UNIQUE", "DUP"]:
        """
        Check key against the 7-day window Bloom Filter.

        Returns "UNIQUE" (new, proceed with Silver INSERT) or
        "DUP" (seen before, skip INSERT and log).
        """
        with self._lock:
            self._check_count += 1
            if self._check_count % PRUNE_EVERY_N_CHECKS == 0:
                self._prune_expired()

            if key in self._bloom:
                self._dup_count += 1
                log.warning("BLOOM DUP  key=%s", key)
                return "DUP"

            self._bloom.add(key)
            self._timestamps[key] = datetime.now(tz=timezone.utc)
            self._new_count += 1
            log.debug("BLOOM NEW  key=%s", key)
            return "UNIQUE"

    def get_stats(self) -> dict:
        """Return deduplication counters for monitoring/dashboard."""
        with self._lock:
            total = self._new_count + self._dup_count
            dup_rate = (self._dup_count / total * 100) if total else 0.0
            return {
                "new_count": self._new_count,
                "dup_count": self._dup_count,
                "total_checked": total,
                "dup_rate_pct": round(dup_rate, 2),
                "window_days": self.window_days,
                "bloom_error_rate": BLOOM_ERROR_RATE,
            }

    def reset(self):
        """Full reset — clears Bloom and stats."""
        with self._lock:
            self._bloom = ScalableBloomFilter(
                initial_capacity=BLOOM_INITIAL_CAPACITY,
                error_rate=BLOOM_ERROR_RATE,
                mode=ScalableBloomFilter.LARGE_SET_GROWTH,
            )
            self._timestamps.clear()
            self._new_count = self._dup_count = self._check_count = 0
        log.info("SilverBloomFilter reset.")

    def _prune_expired(self):
        """Remove timestamp entries older than window_days (lazy TTL)."""
        cutoff = datetime.now(tz=timezone.utc) - timedelta(days=self.window_days)
        expired = [k for k, ts in self._timestamps.items() if ts < cutoff]
        for k in expired:
            del self._timestamps[k]
        if expired:
            log.info("Bloom TTL prune: %d expired keys removed.", len(expired))


# Module-level singleton
_default_filter: "SilverBloomFilter | None" = None
_singleton_lock = threading.Lock()


def get_silver_bloom_filter() -> SilverBloomFilter:
    """Return the shared module-level SilverBloomFilter singleton."""
    global _default_filter
    if _default_filter is None:
        with _singleton_lock:
            if _default_filter is None:
                _default_filter = SilverBloomFilter()
                log.info(
                    "SilverBloomFilter initialised (window=%d days, error_rate=%.2f%%)",
                    WINDOW_DAYS, BLOOM_ERROR_RATE * 100,
                )
    return _default_filter


if __name__ == "__main__":
    import json
    logging.basicConfig(level=logging.INFO)
    bf = SilverBloomFilter()

    test_cases = [
        (bf.make_key(10001, "2150-01-01 08:00:00", "creatinine", 1.2), "UNIQUE"),
        (bf.make_key(10002, "2150-01-01 09:00:00", "urine", 450.0), "UNIQUE"),
        (bf.make_key(10001, "2150-01-01 08:00:00", "creatinine", 1.2), "DUP"),   # replay
        (bf.make_key(10003, "2150-01-01 10:00:00", "creatinine", 0.9), "UNIQUE"),
    ]

    for key, expected in test_cases:
        result = bf.check_and_insert(key)
        icon = "OK" if result == expected else "FAIL"
        print(f"[{icon}] {result} (expected {expected}) | {key}")

    print("\nStats:", json.dumps(bf.get_stats(), indent=2))

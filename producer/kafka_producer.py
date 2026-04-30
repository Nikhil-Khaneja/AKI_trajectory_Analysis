"""
Module 2 v1.2.0: Kafka producer for streaming AKI events (3-topic routing).
Owner: Karthik

v1.2.0 routing — events are partitioned across THREE topics by event_type:

    labs   topic  ← creatinine, bmp, electrolytes, lab
    vitals topic  ← heart_rate, blood_pressure, spo2, temperature, vital
    fluids topic  ← urine, urine_output, fluid, iv_total

This replaces the old v1.1 two-topic split (creatinine / urine) and lets the
streaming consumer subscribe to a single topic-pattern (`aki.*.events`)
while keeping per-stream backpressure isolated.

Source rows are read from the Module 1 SILVER layer (Snowflake) — Bloom
deduplication has already happened in Module 1, so the producer only
replays events in patient time order.

Usage:
    # Replay from Snowflake silver
    python producer/kafka_producer.py --source snowflake

    # Replay from a local CSV (subject_id, charttime, event_type, value, [valueuom])
    python producer/kafka_producer.py --source csv --csv-path /tmp/silver_events.csv
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from typing import Iterable, Optional

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

# ── v1.2.0 topic routing ─────────────────────────────────────────────────────

LABS_EVENTS = {"creatinine", "bmp", "electrolytes", "lab"}
VITALS_EVENTS = {"heart_rate", "blood_pressure", "spo2", "temperature", "vital"}
FLUIDS_EVENTS = {"urine", "urine_output", "fluid", "iv_total"}

TOPIC_LABS = os.getenv("KAFKA_TOPIC_LABS", "aki.labs.events")
TOPIC_VITALS = os.getenv("KAFKA_TOPIC_VITALS", "aki.vitals.events")
TOPIC_FLUIDS = os.getenv("KAFKA_TOPIC_FLUIDS", "aki.fluids.events")

ALL_TOPICS = (TOPIC_LABS, TOPIC_VITALS, TOPIC_FLUIDS)


def topic_for(event_type: Optional[str]) -> Optional[str]:
    """Map a Silver-layer event_type to one of the three v1.2.0 Kafka topics."""
    et = (event_type or "").strip().lower()
    if et in LABS_EVENTS:
        return TOPIC_LABS
    if et in VITALS_EVENTS:
        return TOPIC_VITALS
    if et in FLUIDS_EVENTS:
        return TOPIC_FLUIDS
    return None


# ── Source readers ───────────────────────────────────────────────────────────

def iter_events_from_csv(path: str) -> Iterable[dict]:
    import csv
    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            yield dict(row)


def iter_events_from_snowflake(query: str) -> Iterable[dict]:
    """Stream rows from Snowflake SILVER (subject_id, charttime, event_type, value, ...)."""
    from ingestion.snowflake_connection import get_connection

    conn = get_connection(schema="SILVER")
    try:
        cur = conn.cursor()
        cur.execute(query)
        cols = [c[0].lower() for c in cur.description]
        for row in cur:
            yield dict(zip(cols, row))
    finally:
        conn.close()


# ── Producer build + replay ─────────────────────────────────────────────────

def build_producer(bootstrap_servers: str):
    from confluent_kafka import Producer

    return Producer(
        {
            "bootstrap.servers": bootstrap_servers,
            "linger.ms": 50,
            "compression.type": "snappy",
            "acks": "all",
            "enable.idempotence": True,
        }
    )


def _delivery_report(err, msg):
    if err is not None:
        log.error("DELIVERY FAILED topic=%s err=%s", msg.topic(), err)


def replay(producer, events: Iterable[dict], delay_sec: float = 0.0) -> dict:
    """
    Sort events by (subject_id, charttime), route by event_type, publish to Kafka.

    Returns a counter dict of per-topic + skipped event counts.
    """
    rows = sorted(
        events,
        key=lambda r: (str(r.get("subject_id", "")), str(r.get("charttime", ""))),
    )

    counts = {TOPIC_LABS: 0, TOPIC_VITALS: 0, TOPIC_FLUIDS: 0, "skipped": 0}

    for row in rows:
        topic = topic_for(row.get("event_type"))
        if topic is None:
            counts["skipped"] += 1
            continue
        key = str(row.get("subject_id", "")).encode()
        value = json.dumps(row, default=str).encode()
        producer.produce(topic, key=key, value=value, on_delivery=_delivery_report)
        counts[topic] += 1
        if delay_sec:
            time.sleep(delay_sec)
        producer.poll(0)

    producer.flush()
    return counts


# ── Entry point ─────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Module 2 v1.2.0 — Kafka producer (labs / vitals / fluids)"
    )
    p.add_argument(
        "--bootstrap",
        default=os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"),
        help="Kafka bootstrap.servers",
    )
    p.add_argument(
        "--source",
        choices=["csv", "snowflake"],
        default=os.getenv("REPLAY_SOURCE", "snowflake"),
        help="Where to read replay events from",
    )
    p.add_argument(
        "--csv-path",
        default=os.getenv("REPLAY_CSV"),
        help="Path to CSV (used when --source=csv)",
    )
    p.add_argument(
        "--query",
        default=os.getenv(
            "REPLAY_QUERY",
            "SELECT subject_id, charttime, event_type, value, valueuom "
            "FROM AKI_DB.SILVER.SILVER_STREAM_EVENTS "
            "ORDER BY subject_id, charttime",
        ),
        help="Snowflake query (used when --source=snowflake)",
    )
    p.add_argument(
        "--delay-sec",
        type=float,
        default=float(os.getenv("REPLAY_DELAY_SEC", "0")),
        help="Optional inter-event sleep to throttle replay (seconds)",
    )
    return p.parse_args()


def main():
    args = parse_args()

    log.info("=== Module 2 v1.2.0 — Kafka 3-topic producer ===")
    log.info("Bootstrap   : %s", args.bootstrap)
    log.info("Topics      : labs=%s vitals=%s fluids=%s", *ALL_TOPICS)
    log.info("Source      : %s", args.source)

    producer = build_producer(args.bootstrap)

    if args.source == "csv":
        if not args.csv_path:
            raise SystemExit("--csv-path or REPLAY_CSV is required for --source=csv")
        events = iter_events_from_csv(args.csv_path)
    else:
        events = iter_events_from_snowflake(args.query)

    counts = replay(producer, events, delay_sec=args.delay_sec)

    log.info("── Publish report ──")
    for topic in ALL_TOPICS:
        log.info("  %-22s  %d events", topic, counts.get(topic, 0))
    log.info("  %-22s  %d events", "skipped (unknown type)", counts["skipped"])
    log.info("Replay complete.")


if __name__ == "__main__":
    main()

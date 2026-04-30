"""
Module 2 v1.2.0: Snowflake sink for live AKI risk scores.
Owner: Karthik

Writes one row per PATIENT_TRACE event to AKI_DB.MART.LIVE_RISK_SCORES so the
clinical dashboard has a real-time view of every patient's KDIGO stage,
anomaly score, creatinine ratio, and trailing urine velocities.

The sink is built so it can be plugged into a Spark Structured Streaming
`foreachBatch` callback OR called directly from a Python loop.
"""

from __future__ import annotations

import logging
from typing import Iterable, Optional

log = logging.getLogger(__name__)

LIVE_SCORES_TABLE = "AKI_DB.MART.LIVE_RISK_SCORES"

LIVE_SCORES_DDL = f"""
CREATE TABLE IF NOT EXISTS {LIVE_SCORES_TABLE} (
    subject_id          NUMBER          NOT NULL,
    score_ts            TIMESTAMP_NTZ   NOT NULL,   -- charttime of the triggering event
    kdigo_stage         NUMBER          NOT NULL,
    anomaly_score       FLOAT           NOT NULL,
    cr_current          FLOAT,
    baseline_cr_48h     FLOAT,
    cr_ratio            FLOAT,
    cr_delta_48h        FLOAT,
    urine_velocity_6h   FLOAT,
    urine_velocity_12h  FLOAT,
    urine_velocity_24h  FLOAT,
    bloom_status        VARCHAR(16)     DEFAULT 'UNIQUE',
    fm_distinct_patients NUMBER,
    dgim_events_1h      NUMBER,
    triggered_by        VARCHAR(64),
    ingested_at         TIMESTAMP_LTZ   DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (subject_id, score_ts);
"""


# ── Row builder ──────────────────────────────────────────────────────────────

def build_score_row(trace: dict) -> tuple:
    """
    Convert a PATIENT_TRACE dict into the column tuple expected by
    LIVE_RISK_SCORES.insert(...).
    """
    return (
        int(trace["patient_id"]),
        trace["timestamp"],
        int(trace.get("kdigo_stage", 0)),
        float(trace.get("anomaly_score", 0.0)),
        trace.get("cr_current"),
        trace.get("baseline_cr_48h"),
        trace.get("cr_ratio"),
        trace.get("cr_delta_48h"),
        trace.get("urine_velocity_6h"),
        trace.get("urine_velocity_12h"),
        trace.get("urine_velocity_24h"),
        trace.get("bloom", "UNIQUE"),
        trace.get("fm_distinct_patients"),
        trace.get("dgim_events_1h"),
        trace.get("triggered_by"),
    )


INSERT_SQL = f"""
INSERT INTO {LIVE_SCORES_TABLE} (
    subject_id, score_ts, kdigo_stage, anomaly_score,
    cr_current, baseline_cr_48h, cr_ratio, cr_delta_48h,
    urine_velocity_6h, urine_velocity_12h, urine_velocity_24h,
    bloom_status, fm_distinct_patients, dgim_events_1h, triggered_by
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""


# ── DDL bootstrap ────────────────────────────────────────────────────────────

def ensure_table(conn) -> None:
    """Create AKI_DB.MART.LIVE_RISK_SCORES if it does not exist."""
    cur = conn.cursor()
    try:
        cur.execute(LIVE_SCORES_DDL)
        log.info("Ensured table %s", LIVE_SCORES_TABLE)
    finally:
        cur.close()


# ── Write paths ──────────────────────────────────────────────────────────────

def write_traces(traces: Iterable[dict], conn=None) -> int:
    """Bulk-insert PATIENT_TRACE rows. If `conn` is None, opens a new MART connection."""
    rows = [build_score_row(t) for t in traces]
    if not rows:
        return 0

    own_conn = False
    if conn is None:
        from ingestion.snowflake_connection import get_connection

        conn = get_connection(schema="MART")
        own_conn = True
    try:
        ensure_table(conn)
        cur = conn.cursor()
        try:
            cur.executemany(INSERT_SQL, rows)
            conn.commit()
            log.info("Wrote %d live-score rows to %s", len(rows), LIVE_SCORES_TABLE)
        finally:
            cur.close()
    finally:
        if own_conn:
            conn.close()
    return len(rows)


# ── Spark foreachBatch hook ──────────────────────────────────────────────────

def make_foreach_batch_writer(snowflake_conn_factory=None):
    """
    Returns a callable suitable for `streamingQuery.foreachBatch(...)`.

    `snowflake_conn_factory` is a 0-arg callable returning a fresh Snowflake
    connection — defaults to ingestion.snowflake_connection.get_connection.
    """

    def _writer(batch_df, batch_id):
        rows = batch_df.toJSON().collect()
        if not rows:
            return
        import json as _json

        traces = [_json.loads(r) for r in rows]

        if snowflake_conn_factory is None:
            from ingestion.snowflake_connection import get_connection

            conn = get_connection(schema="MART")
        else:
            conn = snowflake_conn_factory()

        try:
            n = write_traces(traces, conn=conn)
            log.info("foreachBatch %s wrote %d rows", batch_id, n)
        finally:
            conn.close()

    return _writer

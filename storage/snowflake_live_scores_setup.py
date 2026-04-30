"""
Module 2 v1.2.0: DDL bootstrap for AKI_DB.MART.LIVE_RISK_SCORES.
Owner: Karthik

Creates the table that the streaming consumer writes PATIENT_TRACE rows into.
Idempotent — safe to re-run.

Usage:
    python storage/snowflake_live_scores_setup.py
"""

from __future__ import annotations

import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)


def main():
    from consumer.sink_snowflake import LIVE_SCORES_TABLE, ensure_table
    from ingestion.snowflake_connection import get_connection

    log.info("=== Module 2 v1.2.0 — provisioning %s ===", LIVE_SCORES_TABLE)
    conn = get_connection(schema="MART")
    try:
        ensure_table(conn)
        log.info("✅ %s ready.", LIVE_SCORES_TABLE)
    finally:
        conn.close()


if __name__ == "__main__":
    main()

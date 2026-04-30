"""
Module 1 v1.2.0: Snowflake COPY INTO pipeline for MIMIC-IV CSVs.
Owner: Nikhil
Dataset: MIMIC-IV ONLY (no eICU — v1.2.0)
Storage: Snowflake BRONZE (no Apache Iceberg — v1.2.0)

Loads all 5 MIMIC-IV source files into BRONZE raw tables via Snowflake
internal stage. Supports both PUT (local dev) and S3 external stage
(production) via SF_STAGE_MODE env var.

Usage:
    # Local dev (PUT files to internal stage):
    SF_STAGE_MODE=local python run_copy_into.py --data-dir /path/to/mimic-iv/

    # Production (S3 stage pre-configured):
    SF_STAGE_MODE=s3 python run_copy_into.py
"""

import os
import sys
import argparse
import logging
from pathlib import Path

from snowflake_connection import get_connection, get_row_counts

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

# ── v1.2.0 item-ID constants ─────────────────────────────────────────────────

CREATININE_ITEM_IDS = [50912, 51081, 52024, 52546]   # labevents
URINE_ITEM_IDS = [226559, 226560, 226561, 226567, 226627, 226631, 227489]  # outputevents

# ── Table load configurations ─────────────────────────────────────────────────

TABLE_CONFIGS = [
    {
        "csv": "labevents.csv",
        "table": "AKI_DB.BRONZE.RAW_LABEVENTS",
        "description": "Creatinine lab events (MIMIC-IV)",
    },
    {
        "csv": "outputevents.csv",
        "table": "AKI_DB.BRONZE.RAW_OUTPUTEVENTS",
        "description": "Urine output events (MIMIC-IV)",
    },
    {
        "csv": "icustays.csv",
        "table": "AKI_DB.BRONZE.RAW_ICUSTAYS",
        "description": "ICU stay windows (MIMIC-IV)",
    },
    {
        "csv": "admissions.csv",
        "table": "AKI_DB.BRONZE.RAW_ADMISSIONS",
        "description": "Hospital admissions (MIMIC-IV)",
    },
    {
        "csv": "patients.csv",
        "table": "AKI_DB.BRONZE.RAW_PATIENTS",
        "description": "Patient demographics (MIMIC-IV)",
    },
]

BRONZE_TABLES = [cfg["table"] for cfg in TABLE_CONFIGS]


# ── Stage helpers ─────────────────────────────────────────────────────────────

def put_file_to_stage(conn, local_path: Path, stage: str = "@AKI_DB.BRONZE.AKI_STAGE"):
    """Upload a local CSV to Snowflake internal stage (dev mode)."""
    cur = conn.cursor()
    put_sql = f"PUT file://{local_path} {stage} AUTO_COMPRESS=TRUE OVERWRITE=TRUE;"
    log.info("PUT  %s → %s", local_path.name, stage)
    cur.execute(put_sql)
    results = cur.fetchall()
    for row in results:
        log.info("     %s", row)
    cur.close()


def copy_into_table(
    conn,
    table: str,
    csv_filename: str,
    stage: str = "@AKI_DB.BRONZE.AKI_STAGE",
):
    """
    Execute COPY INTO <table> from <stage>/<file>.

    ON_ERROR=CONTINUE lets us capture bad rows without aborting the load.
    The file format enforces NULL mapping and header skip.
    """
    cur = conn.cursor()
    sql = f"""
        COPY INTO {table}
        FROM {stage}/{csv_filename}.gz
        FILE_FORMAT = (FORMAT_NAME = 'AKI_DB.BRONZE.CSV_FORMAT')
        ON_ERROR = 'CONTINUE'
        PURGE = FALSE;
    """
    log.info("COPY INTO %s ← %s", table, csv_filename)
    cur.execute(sql)
    rows = cur.fetchall()
    for row in rows:
        log.info("  status=%s | rows_loaded=%s | errors=%s", row[0], row[3], row[5])
    cur.close()
    return rows


# ── Per-table loaders ─────────────────────────────────────────────────────────

def load_table(
    conn,
    config: dict,
    data_dir: Path | None,
    stage_mode: str,
    stage: str,
):
    """PUT (if local mode) + COPY INTO for a single table."""
    csv_name = config["csv"]
    table = config["table"]
    log.info("── Loading: %s (%s) ──", table, config["description"])

    if stage_mode == "local":
        csv_path = data_dir / csv_name
        if not csv_path.exists():
            log.warning("File not found, skipping: %s", csv_path)
            return
        put_file_to_stage(conn, csv_path, stage)

    copy_into_table(conn, table, csv_name, stage)


def load_all_tables(conn, data_dir: Path | None, stage_mode: str):
    """Load all 5 MIMIC-IV source tables into Snowflake BRONZE."""
    stage = os.getenv("SF_STAGE", "@AKI_DB.BRONZE.AKI_STAGE")

    log.info("=== Module 1 v1.2.0 — MIMIC-IV Bronze Ingestion ===")
    log.info("Stage mode : %s", stage_mode)
    log.info("Stage name : %s", stage)
    log.info("Tables     : %d", len(TABLE_CONFIGS))

    for cfg in TABLE_CONFIGS:
        load_table(conn, cfg, data_dir, stage_mode, stage)

    # ── Row count report (PR evidence) ───────────────────────────────────────
    log.info("── Row Count Report ──")
    counts = get_row_counts(conn, BRONZE_TABLES)
    for tbl, cnt in counts.items():
        log.info("  %-45s  %12d rows", tbl, cnt)

    log.info("✅ Bronze ingestion complete. Dataset: MIMIC-IV ONLY. Storage: Snowflake ONLY.")


# ── Entry point ───────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Module 1 v1.2.0 — MIMIC-IV → Snowflake BRONZE")
    p.add_argument(
        "--data-dir",
        type=Path,
        default=Path(os.getenv("MIMIC_DATA_DIR", ".")),
        help="Directory containing MIMIC-IV CSV files (local stage mode only)",
    )
    p.add_argument(
        "--stage-mode",
        choices=["local", "s3"],
        default=os.getenv("SF_STAGE_MODE", "local"),
        help="'local' = PUT files; 's3' = use pre-configured external stage",
    )
    return p.parse_args()


def main():
    args = parse_args()
    conn = get_connection(schema="BRONZE")
    try:
        load_all_tables(conn, args.data_dir, args.stage_mode)
    finally:
        conn.close()


if __name__ == "__main__":
    main()

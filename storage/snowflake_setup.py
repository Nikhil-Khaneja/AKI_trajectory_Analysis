"""
Module 1 v1.2.0: Snowflake schema + table DDL setup.
Owner: Nikhil
Replaces: storage/iceberg_setup.py (Apache Iceberg — REMOVED in v1.2.0)

Provisions:
    AKI_DB / BRONZE / SILVER / GOLD / MART schemas
    Stage + File Format for COPY INTO
    All Bronze raw tables
"""

import os
import logging
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# DDL constants
# ---------------------------------------------------------------------------

SCHEMA_DDL = [
    "CREATE DATABASE IF NOT EXISTS AKI_DB;",
    "CREATE SCHEMA IF NOT EXISTS AKI_DB.BRONZE;",
    "CREATE SCHEMA IF NOT EXISTS AKI_DB.SILVER;",
    "CREATE SCHEMA IF NOT EXISTS AKI_DB.GOLD;",
    "CREATE SCHEMA IF NOT EXISTS AKI_DB.MART;",
]

FILE_FORMAT_DDL = """
CREATE OR REPLACE FILE FORMAT AKI_DB.BRONZE.CSV_FORMAT
    TYPE = 'CSV'
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    SKIP_HEADER = 1
    NULL_IF = ('', 'NULL', 'null')
    EMPTY_FIELD_AS_NULL = TRUE
    DATE_FORMAT = 'AUTO'
    TIMESTAMP_FORMAT = 'AUTO';
"""

STAGE_DDL = """
CREATE STAGE IF NOT EXISTS AKI_DB.BRONZE.AKI_STAGE
    FILE_FORMAT = AKI_DB.BRONZE.CSV_FORMAT
    COMMENT = 'Internal stage for MIMIC-IV CSV uploads';
"""

BRONZE_TABLES_DDL = [
    # ---- RAW_LABEVENTS -------------------------------------------------------
    """
    CREATE TABLE IF NOT EXISTS AKI_DB.BRONZE.RAW_LABEVENTS (
        labevent_id   NUMBER,
        subject_id    NUMBER        NOT NULL,
        hadm_id       NUMBER,
        specimen_id   NUMBER,
        itemid        NUMBER        NOT NULL,
        charttime     TIMESTAMP_NTZ NOT NULL,
        storetime     TIMESTAMP_NTZ,
        value         VARCHAR(200),
        valuenum      FLOAT,
        valueuom      VARCHAR(20),
        ref_range_lower FLOAT,
        ref_range_upper FLOAT,
        flag          VARCHAR(10),
        priority      VARCHAR(7),
        comments      VARCHAR(500),
        -- audit
        _loaded_at    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    );
    """,
    # ---- RAW_OUTPUTEVENTS ----------------------------------------------------
    """
    CREATE TABLE IF NOT EXISTS AKI_DB.BRONZE.RAW_OUTPUTEVENTS (
        subject_id    NUMBER        NOT NULL,
        hadm_id       NUMBER,
        stay_id       NUMBER,
        charttime     TIMESTAMP_NTZ NOT NULL,
        storetime     TIMESTAMP_NTZ,
        itemid        NUMBER        NOT NULL,
        value         FLOAT,
        valueuom      VARCHAR(20),
        _loaded_at    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    );
    """,
    # ---- RAW_ICUSTAYS --------------------------------------------------------
    """
    CREATE TABLE IF NOT EXISTS AKI_DB.BRONZE.RAW_ICUSTAYS (
        subject_id    NUMBER        NOT NULL,
        hadm_id       NUMBER,
        stay_id       NUMBER        NOT NULL,
        first_careunit  VARCHAR(50),
        last_careunit   VARCHAR(50),
        intime        TIMESTAMP_NTZ NOT NULL,
        outtime       TIMESTAMP_NTZ,
        los           FLOAT,
        _loaded_at    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    );
    """,
    # ---- RAW_ADMISSIONS ------------------------------------------------------
    """
    CREATE TABLE IF NOT EXISTS AKI_DB.BRONZE.RAW_ADMISSIONS (
        subject_id        NUMBER        NOT NULL,
        hadm_id           NUMBER        NOT NULL,
        admittime         TIMESTAMP_NTZ NOT NULL,
        dischtime         TIMESTAMP_NTZ,
        deathtime         TIMESTAMP_NTZ,
        admission_type    VARCHAR(40),
        admission_location VARCHAR(60),
        discharge_location VARCHAR(60),
        insurance         VARCHAR(30),
        language          VARCHAR(10),
        marital_status    VARCHAR(20),
        race              VARCHAR(80),
        edregtime         TIMESTAMP_NTZ,
        edouttime         TIMESTAMP_NTZ,
        hospital_expire_flag NUMBER(1),
        _loaded_at        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    );
    """,
    # ---- RAW_PATIENTS --------------------------------------------------------
    """
    CREATE TABLE IF NOT EXISTS AKI_DB.BRONZE.RAW_PATIENTS (
        subject_id    NUMBER        NOT NULL,
        gender        VARCHAR(1),
        anchor_age    NUMBER,
        anchor_year   NUMBER,
        anchor_year_group VARCHAR(15),
        dod           DATE,
        _loaded_at    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    );
    """,
]


# ---------------------------------------------------------------------------
# Setup runner
# ---------------------------------------------------------------------------

def get_connection() -> snowflake.connector.SnowflakeConnection:
    return snowflake.connector.connect(
        user=os.getenv("SF_USER"),
        password=os.getenv("SF_PASSWORD"),
        account=os.getenv("SF_ACCOUNT"),
        warehouse=os.getenv("SF_WAREHOUSE"),
        database="AKI_DB",
    )


def run_ddl(conn: snowflake.connector.SnowflakeConnection, sql: str, label: str = ""):
    """Execute a single DDL statement and log outcome."""
    cur = conn.cursor()
    try:
        cur.execute(sql.strip())
        log.info("✅ DDL OK: %s", label or sql.strip()[:60])
    except Exception as exc:
        log.error("❌ DDL FAILED [%s]: %s", label, exc)
        raise
    finally:
        cur.close()


def setup_snowflake_schemas(conn: snowflake.connector.SnowflakeConnection):
    """Create AKI_DB + all four schemas."""
    for stmt in SCHEMA_DDL:
        run_ddl(conn, stmt, stmt.strip()[:60])


def setup_file_format_and_stage(conn: snowflake.connector.SnowflakeConnection):
    run_ddl(conn, FILE_FORMAT_DDL, "CSV_FORMAT")
    run_ddl(conn, STAGE_DDL, "AKI_STAGE")


def setup_bronze_tables(conn: snowflake.connector.SnowflakeConnection):
    table_names = [
        "RAW_LABEVENTS", "RAW_OUTPUTEVENTS", "RAW_ICUSTAYS",
        "RAW_ADMISSIONS", "RAW_PATIENTS",
    ]
    for ddl, name in zip(BRONZE_TABLES_DDL, table_names):
        run_ddl(conn, ddl, f"BRONZE.{name}")


def setup_snowflake_tables():
    """
    Full Snowflake provisioning:
      1. Schemas (BRONZE / SILVER / GOLD / MART)
      2. File format + internal stage
      3. All BRONZE raw tables

    v1.2.0 — Snowflake ONLY. Apache Iceberg has been removed.
    Data source: MIMIC-IV ONLY (no eICU).
    """
    log.info("=== AKI v1.2.0 Snowflake Setup — MIMIC-IV / Snowflake ONLY ===")
    conn = get_connection()
    try:
        setup_snowflake_schemas(conn)
        setup_file_format_and_stage(conn)
        setup_bronze_tables(conn)
        log.info("🎉 Snowflake setup complete. No Apache Iceberg used.")
    finally:
        conn.close()


if __name__ == "__main__":
    setup_snowflake_tables()

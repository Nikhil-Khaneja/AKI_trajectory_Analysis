"""
Module 1 v1.2.0: Shared Snowflake connection helper.
Owner: Nikhil
"""

import os
import logging
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()
log = logging.getLogger(__name__)


def get_connection(schema: str = "BRONZE") -> snowflake.connector.SnowflakeConnection:
    """
    Return an authenticated Snowflake connection.

    Reads credentials from environment variables (see .env.example).
    All Module 1 operations use AKI_DB — schema is parameterised for
    BRONZE / SILVER / GOLD / MART access patterns.
    """
    required = ["SF_USER", "SF_PASSWORD", "SF_ACCOUNT", "SF_WAREHOUSE"]
    missing = [k for k in required if not os.getenv(k)]
    if missing:
        raise EnvironmentError(
            f"Missing Snowflake env vars: {missing}. "
            "Copy .env.example to .env and fill in your credentials."
        )

    return snowflake.connector.connect(
        user=os.getenv("SF_USER"),
        password=os.getenv("SF_PASSWORD"),
        account=os.getenv("SF_ACCOUNT"),
        warehouse=os.getenv("SF_WAREHOUSE"),
        database="AKI_DB",
        schema=schema,
        session_parameters={"QUERY_TAG": "AKI_MODULE1_v1.2.0"},
    )


def get_row_counts(conn: snowflake.connector.SnowflakeConnection, tables: list[str]) -> dict:
    """Return {table_fqn: row_count} — used for PR evidence."""
    counts = {}
    cur = conn.cursor()
    for tbl in tables:
        cur.execute(f"SELECT COUNT(*) FROM {tbl}")
        counts[tbl] = cur.fetchone()[0]
    cur.close()
    return counts

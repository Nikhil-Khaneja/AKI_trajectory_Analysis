#!/usr/bin/env python3
"""Shared Snowflake env config helper."""

from __future__ import annotations

import os
from dotenv import load_dotenv

# Load environment variables from .env file if it exists
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))


def _need(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def snowflake_connect_kwargs() -> dict[str, str]:
    return {
        "account": _need("SNOWFLAKE_ACCOUNT"),
        "user": _need("SNOWFLAKE_USER"),
        "password": _need("SNOWFLAKE_PASSWORD"),
        "database": os.getenv("SNOWFLAKE_DATABASE", "AKI_DB"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "AKI_WH"),
        "role": os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
    }


def snowflake_spark_options() -> dict[str, str]:
    account = _need("SNOWFLAKE_ACCOUNT")
    return {
        "sfURL": f"{account}.snowflakecomputing.com",
        "sfUser": _need("SNOWFLAKE_USER"),
        "sfPassword": _need("SNOWFLAKE_PASSWORD"),
        "sfDatabase": os.getenv("SNOWFLAKE_DATABASE", "AKI_DB"),
        "sfWarehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "AKI_WH"),
        "sfRole": os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
    }

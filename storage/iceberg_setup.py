"""
DEPRECATED — v1.2.0

Apache Iceberg has been REMOVED from the AKI Prediction System in v1.2.0.
Storage layer is now Snowflake ONLY.

Replacement: storage/snowflake_setup.py
This file is kept for git history only. Do NOT import or execute.
"""
raise RuntimeError(
    "iceberg_setup.py is deprecated (v1.2.0). "
    "Use storage/snowflake_setup.py instead."
)

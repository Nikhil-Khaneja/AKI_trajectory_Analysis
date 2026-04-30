"""
Module 3 v1.2.0: KDIGO staging rules — applies to hourly feature rows.
Owner: Charvee
"""

from __future__ import annotations

import pandas as pd


def kdigo_stage(row: pd.Series) -> int:
    """
    KDIGO case logic (v1.2.0):
      Stage 3: creatinine_ratio >= 3.0 OR creatinine >= 4.0 OR urine_output_velocity_24h < 0.3
      Stage 2: creatinine_ratio between 2.0-2.99 OR urine_output_velocity_12h < 0.5
      Stage 1: cr_delta_48h >= 0.3 OR creatinine_ratio between 1.5-1.99 OR urine_output_velocity_6h < 0.5
      Else 0
    """
    cr_ratio = row.get("creatinine_ratio")
    cr = row.get("creatinine")
    cr_d48 = row.get("cr_delta_48h")

    uo_6 = row.get("urine_output_velocity_6h")
    uo_12 = row.get("urine_output_velocity_12h")
    uo_24 = row.get("urine_output_velocity_24h")

    if pd.notna(cr_ratio) and cr_ratio >= 3.0:
        return 3
    if pd.notna(cr) and cr >= 4.0:
        return 3
    if pd.notna(uo_24) and uo_24 < 0.3:
        return 3

    if pd.notna(cr_ratio) and 2.0 <= cr_ratio <= 2.99:
        return 2
    if pd.notna(uo_12) and uo_12 < 0.5:
        return 2

    if pd.notna(cr_d48) and cr_d48 >= 0.3:
        return 1
    if pd.notna(cr_ratio) and 1.5 <= cr_ratio <= 1.99:
        return 1
    if pd.notna(uo_6) and uo_6 < 0.5:
        return 1

    return 0


def label_dataset(df: pd.DataFrame) -> pd.DataFrame:
    """Add `kdigo_stage` + 48h progression target label to a per-hour dataset."""
    df = df.copy()
    df["kdigo_stage"] = df.apply(kdigo_stage, axis=1)

    df = df.sort_values(["subject_id", "hour_bucket"])
    df["target_progress_to_stage3_48h"] = (
        df.groupby("subject_id")["kdigo_stage"]
        .transform(
            lambda x: x.shift(-48).rolling(48, min_periods=1).max().fillna(0) >= 3
        )
        .astype(int)
    )
    return df


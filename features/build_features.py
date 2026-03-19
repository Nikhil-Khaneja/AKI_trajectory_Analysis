"""
Module 3 v1.2.0: Feature engineering — compute engineered features per patient-hour.
Owner: Charvee
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class FeatureConfig:
    weight_kg: float = 70.0  # default assumed weight


def compute_creatinine_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    v1.2.0 rules:
      - creatinine_ratio = Cr_current / MIN(Cr in 48-row lookback window)
      - deltas at 1h, 12h, 48h using row-offset diffs in sorted hour buckets
    """
    df = df.sort_values(["subject_id", "hour_bucket"]).copy()

    df["min_cr_48h"] = df.groupby("subject_id")["creatinine"].transform(
        lambda x: x.rolling(48, min_periods=1).min()
    )
    df["creatinine_ratio"] = df["creatinine"] / df["min_cr_48h"].replace(0, np.nan)

    g = df.groupby("subject_id")["creatinine"]
    df["cr_delta_1h"] = g.diff(1)
    df["cr_delta_12h"] = g.diff(12)
    df["cr_delta_48h"] = g.diff(48)
    return df


def compute_urine_features(df: pd.DataFrame, *, config: FeatureConfig = FeatureConfig()) -> pd.DataFrame:
    """Compute rolling urine output velocity features (mL/kg/hr) over 6h/12h/24h windows."""
    df = df.sort_values(["subject_id", "hour_bucket"]).copy()

    for hours in (6, 12, 24):
        rolling_mean = df.groupby("subject_id")["urine_ml"].transform(
            lambda x: x.rolling(hours, min_periods=1).mean()
        )
        df[f"urine_output_velocity_{hours}h"] = rolling_mean / (config.weight_kg * float(hours))
    return df


def build_features(creatinine_df: pd.DataFrame, urine_df: pd.DataFrame) -> pd.DataFrame:
    """Join creatinine + urine and compute Module 3 v1.2.0 features."""
    merged = creatinine_df.merge(urine_df, on=["subject_id", "hour_bucket"], how="outer")
    merged = compute_creatinine_features(merged)
    merged = compute_urine_features(merged)
    return merged


if __name__ == "__main__":
    raise SystemExit(
        "This module provides library functions. "
        "In production, compute features via dbt gold models or a batch job."
    )


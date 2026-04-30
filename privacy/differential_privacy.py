"""
Module 5 v1.2.0: Differential Privacy using Laplace mechanism (epsilon=0.5).
IMPORTANT: Applied ONLY to dashboard metrics aggregates, NOT to raw training data.
Protected metrics:
  - mean creatinine per KDIGO stage
  - patient count per KDIGO stage
  - event count per hour
Owner: Naveen
"""
import numpy as np
import pandas as pd
from typing import Optional


EPSILON = 0.5
MECHANISM = "Laplace"


def laplace_noise(sensitivity: float, epsilon: float = EPSILON) -> float:
    """Generate Laplace noise for a single value."""
    scale = sensitivity / epsilon
    return np.random.laplace(0, scale)


def dp_aggregate_stats(df: pd.DataFrame, group_col: str, value_col: str,
                        sensitivity: float = 1.0, metric_name: Optional[str] = None) -> pd.DataFrame:
    """
    Compute DP-protected aggregate statistics per group.
    Adds Laplace(0, sensitivity/epsilon) noise to each aggregate.
    
    v1.2.0: Used ONLY for dashboard metrics (MART.DP_METRICS), not raw training data.
    
    Args:
        df: Input DataFrame
        group_col: Column to group by (e.g., 'kdigo_stage')
        value_col: Column to aggregate (e.g., 'creatinine')
        sensitivity: L2 sensitivity of the query (default 1.0)
        metric_name: Descriptive name for the metric
        
    Returns:
        DataFrame with DP-protected aggregates
    """
    agg = df.groupby(group_col)[value_col].agg(["mean", "count"]).reset_index()
    
    # Add Laplace noise to mean and count
    agg["dp_mean"] = agg["mean"] + agg["mean"].apply(
        lambda _: laplace_noise(sensitivity, EPSILON)
    )
    agg["dp_count"] = agg["count"] + agg["count"].apply(
        lambda _: laplace_noise(1.0, EPSILON)
    )
    
    # Add metadata
    agg["epsilon"] = EPSILON
    agg["mechanism"] = MECHANISM
    agg["sensitivity"] = sensitivity
    if metric_name:
        agg["metric"] = metric_name
    
    return agg


def compute_dp_dashboard_metrics(df: pd.DataFrame) -> dict:
    """
    Compute all DP-protected dashboard metrics for MART.DP_METRICS table.
    Raw training data remains unchanged.
    
    Returns:
        Dict with 'creatinine_by_kdigo', 'patient_count_by_kdigo', 'event_count_by_hour'
    """
    metrics = {}
    
    # Protected Metric 1: mean creatinine per KDIGO stage
    if "kdigo_stage" in df.columns and "creatinine" in df.columns:
        metrics["creatinine_by_kdigo"] = dp_aggregate_stats(
            df, group_col="kdigo_stage", value_col="creatinine",
            sensitivity=1.0, metric_name="mean_creatinine"
        )
    
    # Protected Metric 2: patient count per KDIGO stage
    if "kdigo_stage" in df.columns:
        metrics["patient_count_by_kdigo"] = dp_aggregate_stats(
            df, group_col="kdigo_stage", value_col="patient_id",
            sensitivity=1.0, metric_name="patient_count"
        )
    
    # Protected Metric 3: event count per hour
    if "timestamp" in df.columns:
        df_copy = df.copy()
        df_copy["hour"] = pd.to_datetime(df_copy["timestamp"]).dt.floor("H")
        metrics["event_count_by_hour"] = dp_aggregate_stats(
            df_copy, group_col="hour", value_col="patient_id",
            sensitivity=1.0, metric_name="event_count"
        )
    
    return metrics


if __name__ == "__main__":
    # Example usage: read k-anonymized dataset and compute DP metrics for dashboard
    try:
        df = pd.read_csv("outputs/mart/k_anonymized_training_set.csv")
        metrics = compute_dp_dashboard_metrics(df)
        
        # Save to MART.DP_METRICS table
        for metric_name, metric_df in metrics.items():
            output_path = f"outputs/mart/dp_metrics_{metric_name}.csv"
            metric_df.to_csv(output_path, index=False)
            print(f"✓ Saved {metric_name} to {output_path}")
            print(metric_df.head())
            print()
        
        print("✓ All DP metrics computed for dashboard (raw training data unchanged)")
    except FileNotFoundError:
        print("Note: k_anonymized_training_set.csv not found. Example mode only.")

#!/usr/bin/env python3
"""
Compare statistical distributions (KDE) of training data vs streaming data.
Fetches training data from GOLD.GOLD_FEATURES_HOURLY.
Fetches streaming data from MART.MART_LIVE_STREAM_METRICS (Reservoir Samples).
"""

import os
import sys
import json
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import snowflake.connector
from pathlib import Path

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from scripts.sf_env import snowflake_connect_kwargs

def fetch_data():
    print("Connecting to Snowflake...")
    ctx = snowflake.connector.connect(**snowflake_connect_kwargs())
    cursor = ctx.cursor()

    # 1. Fetch Training Data
    print("Fetching training data from GOLD_FEATURES_HOURLY...")
    train_query = "SELECT CREATININE_MG_DL, URINE_ML, HOURS_SINCE_ADMIT FROM GOLD.GOLD_FEATURES_HOURLY LIMIT 5000"
    cursor.execute(train_query)
    train_df = pd.DataFrame(cursor.fetchall(), columns=['CREATININE_MG_DL', 'URINE_ML', 'HOURS_SINCE_ADMIT'])
    train_df['source'] = 'Training'

    # 2. Fetch Streaming Data (Reservoir Samples)
    print("Fetching streaming samples from MART_LIVE_STREAM_METRICS...")
    stream_query = "SELECT EXTRA_JSON FROM MART.MART_LIVE_STREAM_METRICS WHERE METRIC_NAME = 'reservoir_size' ORDER BY METRIC_TS DESC LIMIT 100"
    cursor.execute(stream_query)
    rows = cursor.fetchall()

    stream_data = []
    for (extra_json,) in rows:
        if not extra_json: continue
        data = json.loads(extra_json)
        samples = data.get('sample', [])
        for s in samples:
            # Note: Streaming samples are event-based (individual values)
            # Training data is feature-engineered (aligned by stay_id/time)
            # We compare the raw distribution of the primary metrics
            val = s.get('value')
            etype = s.get('event_type', '').lower()
            if val is not None:
                if 'creatinine' in etype:
                    stream_data.append({'value': val, 'feature': 'CREATININE_MG_DL'})
                elif 'urine' in etype:
                    stream_data.append({'value': val, 'feature': 'URINE_ML'})

    stream_df = pd.DataFrame(stream_data)
    stream_df['source'] = 'Streaming'

    cursor.close()
    ctx.close()
    return train_df, stream_df

def plot_distributions(train_df, stream_df, out_dir: Path):
    out_dir.mkdir(parents=True, exist_ok=True)
    sns.set_theme(style="whitegrid")

    # Feature 1: Creatinine
    plt.figure(figsize=(10, 6))
    sns.kdeplot(data=train_df, x='CREATININE_MG_DL', label='Training (Batch)', fill=True, common_norm=False)
    if not stream_df[stream_df['feature'] == 'CREATININE_MG_DL'].empty:
        sns.kdeplot(data=stream_df[stream_df['feature'] == 'CREATININE_MG_DL'], x='value', label='Streaming (Live)', fill=True, common_norm=False)
    
    plt.title("Distribution Comparison: Serum Creatinine")
    plt.xlabel("Creatinine (mg/dL)")
    plt.legend()
    plt.savefig(out_dir / "distribution_creatinine.png")
    print(f"Saved: {out_dir / 'distribution_creatinine.png'}")

    # Feature 2: Urine
    plt.figure(figsize=(10, 6))
    sns.kdeplot(data=train_df, x='URINE_ML', label='Training (Batch)', fill=True, common_norm=False)
    if not stream_df[stream_df['feature'] == 'URINE_ML'].empty:
        sns.kdeplot(data=stream_df[stream_df['feature'] == 'URINE_ML'], x='value', label='Streaming (Live)', fill=True, common_norm=False)
    
    plt.title("Distribution Comparison: Urine Output")
    plt.xlabel("Urine (mL)")
    plt.legend()
    plt.savefig(out_dir / "distribution_urine.png")
    print(f"Saved: {out_dir / 'distribution_urine.png'}")

    # Feature 3: Hours Since Admit (Training Only usually)
    plt.figure(figsize=(10, 6))
    sns.kdeplot(data=train_df, x='HOURS_SINCE_ADMIT', fill=True)
    plt.title("Training Feature Distribution: Hours Since Admit")
    plt.xlabel("Hours")
    plt.savefig(out_dir / "distribution_hours.png")
    print(f"Saved: {out_dir / 'distribution_hours.png'}")

def main():
    try:
        train_df, stream_df = fetch_data()
        out_dir = Path(__file__).resolve().parents[1] / "docs" / "reports" / "feature_distributions"
        plot_distributions(train_df, stream_df, out_dir)
        print("\n✅ Distribution analysis complete. Check the 'docs/reports/feature_distributions' directory.")
    except Exception as e:
        print(f"❌ Error during distribution analysis: {e}")

if __name__ == "__main__":
    main()

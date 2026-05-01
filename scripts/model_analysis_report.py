import os
import sys
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import snowflake.connector
from pathlib import Path
from joblib import load
from sklearn.metrics import confusion_matrix, ConfusionMatrixDisplay

# Add project root to path
ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT))

try:
    from scripts.sf_env import snowflake_connect_kwargs
except ImportError:
    # Fallback if scripts folder is not in path correctly
    def snowflake_connect_kwargs():
        return {
            "account": os.getenv("SNOWFLAKE_ACCOUNT"),
            "user": os.getenv("SNOWFLAKE_USER"),
            "password": os.getenv("SNOWFLAKE_PASSWORD"),
            "database": os.getenv("SNOWFLAKE_DATABASE", "AKI_DB"),
            "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "AKI_WH"),
            "role": os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
        }

# Constants
OUTPUT_DIR = ROOT / "docs" / "reports" / "model_analysis"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
MODELS_DIR = ROOT / "outputs" / "models"
LOCAL_DATA = ROOT / "outputs" / "tables" / "synthetic_eicu_training_set.csv"

FEATURE_COLS = [
    "creatinine", "baseline_creatinine", "creatinine_ratio", 
    "creatinine_delta_1h", "creatinine_delta_48h", "urine_output_ml",
    "urine_ml_per_kg_hr_6h", "urine_ml_per_kg_hr_12h", "urine_ml_per_kg_hr_24h"
]
TARGET_COL = "target_progress_to_stage3_48h"

def fetch_data_from_snowflake():
    print("Connecting to Snowflake...")
    try:
        conn = snowflake.connector.connect(**snowflake_connect_kwargs())
        query = "SELECT * FROM GOLD.AKI_TRAINING_SET LIMIT 10000"
        print(f"Executing: {query}")
        df = pd.read_sql(query, conn)
        conn.close()
        print("Data successfully fetched from Snowflake.")
        return df
    except Exception as e:
        print(f"Warning: Could not fetch from Snowflake ({e}). Falling back to local data.")
        if LOCAL_DATA.exists():
            return pd.read_csv(LOCAL_DATA)
        else:
            raise FileNotFoundError("Neither Snowflake nor local data could be accessed.")

def analyze_and_plot(df):
    # Ensure column names are lowercase (Snowflake often returns uppercase)
    df.columns = [c.lower() for c in df.columns]
    
    # 1. Feature Distributions
    print("Generating feature distributions...")
    fig, axes = plt.subplots(3, 3, figsize=(15, 12))
    axes = axes.flatten()
    for i, col in enumerate(FEATURE_COLS):
        if col in df.columns:
            sns.histplot(df[col], kde=True, ax=axes[i])
            axes[i].set_title(f"Distribution: {col}")
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / "feature_distributions.png")

    # 2. Correlation Matrix
    print("Generating correlation matrix...")
    plt.figure(figsize=(12, 10))
    corr = df[FEATURE_COLS + [TARGET_COL]].corr()
    sns.heatmap(corr, annot=True, cmap='coolwarm', fmt=".2f")
    plt.title("Feature Correlation Matrix")
    plt.savefig(OUTPUT_DIR / "correlation_matrix.png")

    # 3. Covariance Matrix
    print("Generating covariance matrix...")
    plt.figure(figsize=(12, 10))
    cov = df[FEATURE_COLS].cov()
    sns.heatmap(cov, annot=False, cmap='YlGnBu')
    plt.title("Feature Covariance Matrix")
    plt.savefig(OUTPUT_DIR / "covariance_matrix.png")

    # 4. Confusion Matrices for Models
    print("Generating confusion matrices...")
    X = df[FEATURE_COLS].fillna(0)
    y_true = df[TARGET_COL]

    model_files = {
        "Logistic Regression": MODELS_DIR / "logistic_regression.joblib",
        "Gradient Boosting": MODELS_DIR / "gradient_boosting.joblib"
    }

    for name, path in model_files.items():
        if path.exists():
            model = load(path)
            y_pred = model.predict(X)
            cm = confusion_matrix(y_true, y_pred)
            
            plt.figure(figsize=(6, 5))
            disp = ConfusionMatrixDisplay(confusion_matrix=cm, display_labels=["Stable", "Progressing"])
            disp.plot(cmap='Blues')
            plt.title(f"Confusion Matrix: {name}")
            plt.savefig(OUTPUT_DIR / f"confusion_matrix_{name.lower().replace(' ', '_')}.png")
            print(f"Confusion Matrix for {name} saved.")
        else:
            print(f"Model file not found: {path}")

def main():
    print("=== AKI Model Analysis Report ===")
    df = fetch_data_from_snowflake()
    analyze_and_plot(df)
    print(f"\nReport complete. All visualizations saved to: {OUTPUT_DIR}")

if __name__ == "__main__":
    main()

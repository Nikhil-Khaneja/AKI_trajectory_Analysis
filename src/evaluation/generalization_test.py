import pandas as pd
from pathlib import Path
from joblib import load
from src.evaluation.metrics import binary_classification_metrics

ROOT = Path(__file__).resolve().parents[2]
MODELS_DIR = ROOT / "outputs" / "models"
ANONYMIZED_DATA = ROOT / "outputs" / "mart" / "k_anonymized_training_set.csv"
REPORTS_DIR = ROOT / "docs" / "reports" / "generalization"
REPORTS_DIR.mkdir(parents=True, exist_ok=True)

FEATURE_COLS = [
    "creatinine", "baseline_creatinine", "creatinine_ratio", 
    "creatinine_delta_1h", "creatinine_delta_48h", "urine_output_ml",
    "urine_ml_per_kg_hr_6h", "urine_ml_per_kg_hr_12h", "urine_ml_per_kg_hr_24h"
]
TARGET_COL = "target_progress_to_stage3_48h"

def run_generalization_test():
    """
    Step 9: Cross-Dataset Generalization.
    Simulates training on one ICU type (Source) and testing on another (Target).
    """
    print("Starting Cross-Dataset Generalization Test...")
    if not ANONYMIZED_DATA.exists():
        print("Data missing.")
        return
        
    df = pd.read_csv(ANONYMIZED_DATA)
    
    # Simulate Hospital A (MICU) and Hospital B (SICU)
    hospital_a = df[df["unittype"] == "MICU"]
    hospital_b = df[df["unittype"] == "SICU"]
    
    print(f"Hospital A (Source - MICU) size: {len(hospital_a)}")
    print(f"Hospital B (Target - SICU) size: {len(hospital_b)}")
    
    gbt_path = MODELS_DIR / "gradient_boosting.joblib"
    if not gbt_path.exists():
        print("Model missing.")
        return
        
    model = load(gbt_path)
    
    # 1. Performance on Source
    print("Evaluating on Source (Internal Validation)...")
    y_prob_a = model.predict_proba(hospital_a[FEATURE_COLS])[:, 1]
    metrics_a = binary_classification_metrics(hospital_a[TARGET_COL], y_prob_a)
    
    # 2. Performance on Target
    print("Evaluating on Target (Cross-Hospital Generalization)...")
    y_prob_b = model.predict_proba(hospital_b[FEATURE_COLS])[:, 1]
    metrics_b = binary_classification_metrics(hospital_b[TARGET_COL], y_prob_b)
    
    report = pd.DataFrame([
        {"site": "Hospital A (Source)", **metrics_a},
        {"site": "Hospital B (Target)", **metrics_b}
    ])
    
    out_path = REPORTS_DIR / "cross_site_report.csv"
    report.to_csv(out_path, index=False)
    
    print("\nGeneralization Results:")
    print(report[["site", "auroc", "f1", "ece"]])
    
    # Check for Calibration Decay
    ece_diff = metrics_b["ece"] - metrics_a["ece"]
    print(f"\nCalibration Decay (ECE Target - ECE Source): {ece_diff:.4f}")
    if ece_diff > 0.05:
        print("WARNING: Significant calibration decay detected in cross-site transfer.")
    else:
        print("SUCCESS: Model maintains good calibration across sites.")

if __name__ == "__main__":
    run_generalization_test()

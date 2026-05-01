import pandas as pd
from pathlib import Path
from joblib import load
from sklearn.metrics import precision_score, recall_score, f1_score

ROOT = Path(__file__).resolve().parents[2]
MODELS_DIR = ROOT / "outputs" / "models"
ANONYMIZED_DATA = ROOT / "outputs" / "mart" / "k_anonymized_training_set.csv"
REPORTS_DIR = ROOT / "docs" / "reports" / "fairness"
REPORTS_DIR.mkdir(parents=True, exist_ok=True)

FEATURE_COLS = [
    "creatinine", "baseline_creatinine", "creatinine_ratio", 
    "creatinine_delta_1h", "creatinine_delta_48h", "urine_output_ml",
    "urine_ml_per_kg_hr_6h", "urine_ml_per_kg_hr_12h", "urine_ml_per_kg_hr_24h"
]
TARGET_COL = "target_progress_to_stage3_48h"

def evaluate_fairness(df, model, model_name):
    print(f"Evaluating fairness for {model_name}...")
    
    X = df[FEATURE_COLS].fillna(0)
    y_true = df[TARGET_COL]
    y_pred = model.predict(X)
    
    df_results = df.copy()
    df_results["y_pred"] = y_pred
    
    fairness_reports = []
    
    # Evaluate across Gender and Age Group
    for subgroup in ["gender", "age_group"]:
        for value in df_results[subgroup].unique():
            subset = df_results[df_results[subgroup] == value]
            if len(subset) < 10: continue
            
            p = precision_score(subset[TARGET_COL], subset["y_pred"], zero_division=0)
            r = recall_score(subset[TARGET_COL], subset["y_pred"], zero_division=0)
            f1 = f1_score(subset[TARGET_COL], subset["y_pred"], zero_division=0)
            
            fairness_reports.append({
                "model": model_name,
                "attribute": subgroup,
                "value": str(value),
                "count": len(subset),
                "precision": p,
                "recall": r,
                "f1": f1
            })
            
    return pd.DataFrame(fairness_reports)

def main():
    if not ANONYMIZED_DATA.exists():
        print("Please run k_anonymity.py first.")
        return
        
    df = pd.read_csv(ANONYMIZED_DATA)
    gbt_path = MODELS_DIR / "gradient_boosting.joblib"
    
    if not gbt_path.exists():
        print("Model not found. Run training first.")
        return
        
    model = load(gbt_path)
    report = evaluate_fairness(df, model, "Gradient Boosting")
    
    out_path = REPORTS_DIR / "fairness_report.csv"
    report.to_csv(out_path, index=False)
    
    print("\nFairness Disparity Check (Subgroup Performance):")
    print(report.sort_values(["attribute", "f1"]))
    print(f"\nFairness report saved to {out_path}")

if __name__ == "__main__":
    main()

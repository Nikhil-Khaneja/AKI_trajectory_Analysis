from pathlib import Path
import pandas as pd
from joblib import load
from src.evaluation.metrics import binary_classification_metrics, get_roc_curve_data

ROOT = Path(__file__).resolve().parents[2]
INPUT = ROOT / "outputs" / "tables" / "synthetic_eicu_training_set.csv"
MODELS = ROOT / "outputs" / "models"
METRICS = ROOT / "outputs" / "metrics"
METRICS.mkdir(parents=True, exist_ok=True)

def evaluate(model_path: Path, label: str):
    print(f"Evaluating {label}...")
    df = pd.read_csv(INPUT)
    
    # Identify target and features
    target_col = "target_progress_to_stage3_48h"
    if target_col not in df.columns:
        print(f"Warning: {target_col} not found in {INPUT}. Checking columns...")
        # Fallback for dynamic datasets if needed, but here we assume the schema is fixed
        # or we might need to find which column is the target.
        print(f"Available columns: {df.columns.tolist()}")
        return

    y = df[target_col]
    cols_to_drop = ["patientunitstayid", "hour_bucket", "kdigo_stage", target_col]
    X = df.drop(columns=[c for c in cols_to_drop if c in df.columns])
    
    model = load(model_path)
    y_prob = model.predict_proba(X)[:, 1]
    
    # Calculate scalar metrics
    m = binary_classification_metrics(y, y_prob)
    out = pd.DataFrame([{"model": label, **m}])
    
    # Save scalar metrics
    metrics_file = METRICS / f"{label}_metrics.csv"
    out.to_csv(metrics_file, index=False)
    print(f"Metrics saved to {metrics_file}")
    print(out)
    
    # Calculate and save ROC curve data
    roc_data = get_roc_curve_data(y, y_prob)
    roc_file = METRICS / f"{label}_roc_curve.csv"
    roc_data.to_csv(roc_file, index=False)
    print(f"ROC curve data saved to {roc_file}")

def main():
    lr = MODELS / "logistic_regression.joblib"
    gbt = MODELS / "gradient_boosting.joblib"
    
    evaluated = False
    if lr.exists():
        evaluate(lr, "logistic_regression")
        evaluated = True
    else:
        print(f"Model not found: {lr}")
        
    if gbt.exists():
        evaluate(gbt, "gradient_boosting")
        evaluated = True
    else:
        print(f"Model not found: {gbt}")
    
    if not evaluated:
        print("No models found to evaluate in outputs/models/")

if __name__ == "__main__":
    main()

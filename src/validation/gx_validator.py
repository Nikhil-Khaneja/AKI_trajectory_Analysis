from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
RAW = ROOT / "data" / "raw"
OUTPUTS = ROOT / "outputs" / "tables"
OUTPUTS.mkdir(parents=True, exist_ok=True)

def robust_quality_gate(df: pd.DataFrame, dataset_name: str) -> pd.DataFrame:
    """
    Step 2: Ingestion Quality Gate.
    Checks for realistic creatinine, non-negative urine, and valid metadata.
    """
    checks = []
    
    # 1. Creatinine Range Check (0.3 - 15.0 mg/dL)
    if "creatinine" in df.columns:
        valid_cr = ((df["creatinine"] >= 0.3) & (df["creatinine"] <= 15.0)).all()
        checks.append({"dataset": dataset_name, "check": "creatinine_range_valid", "passed": bool(valid_cr)})
    
    # 2. No Negative Urine Output
    if "urine_output_ml" in df.columns:
        non_negative_urine = (df["urine_output_ml"] >= 0).all()
        checks.append({"dataset": dataset_name, "check": "urine_non_negative", "passed": bool(non_negative_urine)})
        
    # 3. No Missing Patient IDs
    id_col = "patientunitstayid" if "patientunitstayid" in df.columns else "subject_id"
    if id_col in df.columns:
        no_missing_ids = df[id_col].notnull().all()
        checks.append({"dataset": dataset_name, "check": "no_missing_patient_ids", "passed": bool(no_missing_ids)})
        
    # 4. Valid Timestamps
    if "hour_bucket" in df.columns:
        try:
            pd.to_datetime(df["hour_bucket"])
            valid_ts = True
        except:
            valid_ts = False
        checks.append({"dataset": dataset_name, "check": "valid_timestamps", "passed": bool(valid_ts)})
        
    return pd.DataFrame(checks)

def main():
    # We validate the training set as the "Gate" before it moves to Mart/Model
    training_set_path = ROOT / "outputs" / "tables" / "synthetic_eicu_training_set.csv"
    if training_set_path.exists():
        df = pd.read_csv(training_set_path)
        report = robust_quality_gate(df, "eICU_Training_Set")
        
        report_path = OUTPUTS / "quality_gate_report.csv"
        report.to_csv(report_path, index=False)
        print("=== Step 2: Quality Gate Report ===")
        print(report)
        
        if not report["passed"].all():
            print("\nWARNING: Quality Gate checks failed. Review data before proceeding.")
        else:
            print("\nSUCCESS: All Quality Gate checks passed.")

if __name__ == "__main__":
    main()

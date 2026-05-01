from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
LABELED = ROOT / "data" / "labeled"
OUTPUT_TABLES = ROOT / "outputs" / "tables"
OUTPUT_METRICS = ROOT / "outputs" / "metrics"
OUTPUT_TABLES.mkdir(parents=True, exist_ok=True)
OUTPUT_METRICS.mkdir(parents=True, exist_ok=True)

def summarize(path: Path, dataset_name: str, id_col: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    results = {
        "dataset": dataset_name,
        "rows": len(df),
        "unique_ids": df[id_col].nunique(dropna=True) if id_col in df.columns else None,
        "missing_id_rows": int(df[id_col].isna().sum()) if id_col in df.columns else None,
        "invalid_kdigo_rows": int((~df["kdigo_stage"].isin([0, 1, 2, 3])).sum()) if "kdigo_stage" in df.columns else None,
        "negative_urine_rows": int((df["urine_output_ml"].fillna(0) < 0).sum()) if "urine_output_ml" in df.columns else None,
        "low_creatinine_rows": int((df["creatinine"].dropna() < 0.3).sum()) if "creatinine" in df.columns else None,
        "duplicate_patient_hour_rows": int(df.duplicated(subset=[c for c in [id_col, "hour_bucket"] if c in df.columns]).sum()) if id_col in df.columns and "hour_bucket" in df.columns else None,
    }
    stage_counts = df["kdigo_stage"].value_counts().sort_index().to_dict() if "kdigo_stage" in df.columns else {}
    for k, v in stage_counts.items():
        results[f"stage_{k}_rows"] = int(v)
    return pd.DataFrame([results])

def main():
    summaries = []

    eicu = LABELED / "synthetic_eicu_demo" / "hourly_kdigo_labeled.csv"
    if eicu.exists():
        summaries.append(summarize(eicu, "synthetic_eicu_demo", "patientunitstayid"))

    mimic = LABELED / "mimic_demo" / "hourly_kdigo_labeled.csv"
    if mimic.exists():
        summaries.append(summarize(mimic, "mimic_demo", "subject_id"))

    if not summaries:
        raise FileNotFoundError("No labeled datasets found under data/labeled/")

    out = pd.concat(summaries, ignore_index=True)
    out.to_csv(OUTPUT_METRICS / "validation_summary.csv", index=False)
    out.to_csv(OUTPUT_TABLES / "validation_summary.csv", index=False)
    print(out)

if __name__ == "__main__":
    main()

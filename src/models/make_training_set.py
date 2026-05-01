from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
LABELED = ROOT / "data" / "labeled"
OUTPUTS = ROOT / "outputs" / "tables"
OUTPUTS.mkdir(parents=True, exist_ok=True)

FEATURE_COLS = [
    "creatinine",
    "baseline_creatinine",
    "creatinine_ratio",
    "creatinine_delta_1h",
    "creatinine_delta_48h",
    "urine_output_ml",
    "urine_ml_per_kg_hr_6h",
    "urine_ml_per_kg_hr_12h",
    "urine_ml_per_kg_hr_24h",
]

def build_progression_target(df: pd.DataFrame, id_col: str) -> pd.DataFrame:
    out = df.copy().sort_values([id_col, "hour_bucket"]).reset_index(drop=True)
    out["future_max_stage_48h"] = out.groupby(id_col)["kdigo_stage"].transform(lambda s: s.shift(-1).rolling(48, min_periods=1).max())
    out["target_progress_to_stage3_48h"] = ((out["kdigo_stage"] < 3) & (out["future_max_stage_48h"] >= 3)).astype(int)
    return out

def main():
    src = LABELED / "synthetic_eicu_demo" / "hourly_kdigo_labeled.csv"
    if not src.exists():
        raise FileNotFoundError(f"Missing labeled dataset: {src}")
    df = pd.read_csv(src, parse_dates=["hour_bucket"])
    train = build_progression_target(df, "patientunitstayid")
    keep = ["patientunitstayid", "hour_bucket", "kdigo_stage", "target_progress_to_stage3_48h"] + [c for c in FEATURE_COLS if c in train.columns]
    train[keep].to_csv(OUTPUTS / "synthetic_eicu_training_set.csv", index=False)
    print("Saved training set to", OUTPUTS / "synthetic_eicu_training_set.csv")

if __name__ == "__main__":
    main()

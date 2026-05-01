from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
COMBINED = ROOT / "data" / "combined"
FEATURES = ROOT / "data" / "features"

def engineer(df: pd.DataFrame, id_col: str, weight_col: str) -> pd.DataFrame:
    df = df.copy().sort_values([id_col, "hour_bucket"]).reset_index(drop=True)
    df["hour_bucket"] = pd.to_datetime(df["hour_bucket"])
    df["urine_output_ml"] = df["urine_output_ml"].fillna(0)
    df["baseline_creatinine"] = df.groupby(id_col)["creatinine"].transform("min")
    df["creatinine_ratio"] = df["creatinine"] / df["baseline_creatinine"]
    df["creatinine_prev_1h"] = df.groupby(id_col)["creatinine"].shift(1)
    df["creatinine_delta_1h"] = df["creatinine"] - df["creatinine_prev_1h"]
    df["creatinine_prev_48h"] = df.groupby(id_col)["creatinine"].shift(48)
    df["creatinine_delta_48h"] = df["creatinine"] - df["creatinine_prev_48h"]
    df["urine_ml_6h"] = df.groupby(id_col)["urine_output_ml"].transform(lambda s: s.rolling(6, min_periods=1).sum())
    df["urine_ml_12h"] = df.groupby(id_col)["urine_output_ml"].transform(lambda s: s.rolling(12, min_periods=1).sum())
    df["urine_ml_24h"] = df.groupby(id_col)["urine_output_ml"].transform(lambda s: s.rolling(24, min_periods=1).sum())
    df["urine_ml_per_kg_hr_6h"] = df["urine_ml_6h"] / (df[weight_col] * 6)
    df["urine_ml_per_kg_hr_12h"] = df["urine_ml_12h"] / (df[weight_col] * 12)
    df["urine_ml_per_kg_hr_24h"] = df["urine_ml_24h"] / (df[weight_col] * 24)
    return df

def main():
    # synthetic eICU
    eicu_in = COMBINED / "synthetic_eicu_demo" / "hourly_features.csv"
    if eicu_in.exists():
        out_dir = FEATURES / "synthetic_eicu_demo"
        out_dir.mkdir(parents=True, exist_ok=True)
        eicu = pd.read_csv(eicu_in, parse_dates=["hour_bucket"])
        eicu_feat = engineer(eicu, "patientunitstayid", "admissionweight")
        eicu_feat.to_csv(out_dir / "hourly_engineered_features.csv", index=False)
        print("Saved", out_dir / "hourly_engineered_features.csv")

    # mimic demo
    mimic_in = COMBINED / "mimic_demo" / "hourly_features.csv"
    if mimic_in.exists():
        out_dir = FEATURES / "mimic_demo"
        out_dir.mkdir(parents=True, exist_ok=True)
        mimic = pd.read_csv(mimic_in, parse_dates=["hour_bucket"])
        if "admissionweight" not in mimic.columns:
            mimic["admissionweight"] = 70.0
        mimic_feat = engineer(mimic, "subject_id", "admissionweight")
        mimic_feat.to_csv(out_dir / "hourly_engineered_features.csv", index=False)
        print("Saved", out_dir / "hourly_engineered_features.csv")

if __name__ == "__main__":
    main()

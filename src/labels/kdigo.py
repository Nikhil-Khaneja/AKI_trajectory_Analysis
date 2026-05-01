from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
FEATURES = ROOT / "data" / "features"
LABELED = ROOT / "data" / "labeled"

def kdigo_stage(row) -> int:
    if pd.notna(row["creatinine_ratio"]) and row["creatinine_ratio"] >= 3.0:
        return 3
    if pd.notna(row["creatinine"]) and row["creatinine"] >= 4.0:
        return 3
    if pd.notna(row["urine_ml_per_kg_hr_24h"]) and row["urine_ml_per_kg_hr_24h"] < 0.3:
        return 3
    if pd.notna(row["creatinine_ratio"]) and row["creatinine_ratio"] >= 2.0:
        return 2
    if pd.notna(row["urine_ml_per_kg_hr_12h"]) and row["urine_ml_per_kg_hr_12h"] < 0.5:
        return 2
    if pd.notna(row["creatinine_delta_48h"]) and row["creatinine_delta_48h"] >= 0.3:
        return 1
    if pd.notna(row["creatinine_ratio"]) and row["creatinine_ratio"] >= 1.5:
        return 1
    if pd.notna(row["urine_ml_per_kg_hr_6h"]) and row["urine_ml_per_kg_hr_6h"] < 0.5:
        return 1
    return 0

def label_dataset(input_csv: Path, output_csv: Path) -> None:
    df = pd.read_csv(input_csv, parse_dates=["hour_bucket"])
    df["kdigo_stage"] = df.apply(kdigo_stage, axis=1)
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_csv, index=False)

def main():
    eicu_in = FEATURES / "synthetic_eicu_demo" / "hourly_engineered_features.csv"
    if eicu_in.exists():
        label_dataset(eicu_in, LABELED / "synthetic_eicu_demo" / "hourly_kdigo_labeled.csv")
        print("Saved eICU KDIGO labels")

    mimic_in = FEATURES / "mimic_demo" / "hourly_engineered_features.csv"
    if mimic_in.exists():
        label_dataset(mimic_in, LABELED / "mimic_demo" / "hourly_kdigo_labeled.csv")
        print("Saved MIMIC KDIGO labels")

if __name__ == "__main__":
    main()

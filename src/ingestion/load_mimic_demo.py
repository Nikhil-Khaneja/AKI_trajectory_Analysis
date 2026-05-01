from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
RAW = ROOT / "data" / "raw" / "mimic-iv-clinical-database-demo-2.2"
CLEAN = ROOT / "data" / "cleaned" / "mimic_demo"
COMBINED = ROOT / "data" / "combined" / "mimic_demo"
FEATURES = ROOT / "data" / "features" / "mimic_demo"
LABELED = ROOT / "data" / "labeled" / "mimic_demo"

for p in [CLEAN, COMBINED, FEATURES, LABELED]:
    p.mkdir(parents=True, exist_ok=True)

def required_paths() -> dict[str, Path]:
    return {
        "patients": RAW / "hosp" / "patients.csv.gz",
        "admissions": RAW / "hosp" / "admissions.csv.gz",
        "labevents": RAW / "hosp" / "labevents.csv.gz",
        "d_labitems": RAW / "hosp" / "d_labitems.csv.gz",
        "icustays": RAW / "icu" / "icustays.csv.gz",
        "outputevents": RAW / "icu" / "outputevents.csv.gz",
        "d_items": RAW / "icu" / "d_items.csv.gz",
    }

def load_raw_tables() -> dict[str, pd.DataFrame]:
    paths = required_paths()
    for name, path in paths.items():
        if not path.exists():
            raise FileNotFoundError(f"Missing required MIMIC demo file: {path}")
    return {name: pd.read_csv(path) for name, path in paths.items()}

def clean_tables(tables: dict[str, pd.DataFrame]) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    patients = tables["patients"].drop_duplicates()
    admissions = tables["admissions"].drop_duplicates()
    icustays = tables["icustays"].drop_duplicates()

    d_labitems = tables["d_labitems"]
    labevents = tables["labevents"]

    creat_items = d_labitems[d_labitems["label"].str.contains("creatinine", case=False, na=False)][["itemid", "label"]].drop_duplicates()
    creat = labevents.merge(creat_items, on="itemid", how="inner")
    creat = creat.rename(columns={"valuenum": "creatinine", "label": "lab_label"})
    creat = creat[
        creat["subject_id"].notna() &
        creat["hadm_id"].notna() &
        creat["charttime"].notna() &
        creat["creatinine"].notna()
    ]
    creat["event_time"] = pd.to_datetime(creat["charttime"], errors="coerce")
    creat = creat[creat["event_time"].notna()]
    creat = creat[(creat["creatinine"] >= 0.3) & (creat["creatinine"] <= 15)]
    creat = creat.drop_duplicates()

    d_items = tables["d_items"]
    outputevents = tables["outputevents"]
    urine_items = d_items[d_items["label"].str.contains("urine|foley|void", case=False, na=False)][["itemid", "label"]].drop_duplicates()
    urine = outputevents.merge(urine_items, on="itemid", how="inner")
    urine = urine.rename(columns={"value": "urine_output_ml", "label": "output_label"})
    urine = urine[
        urine["subject_id"].notna() &
        urine["hadm_id"].notna() &
        urine["stay_id"].notna() &
        urine["charttime"].notna() &
        urine["urine_output_ml"].notna()
    ]
    urine["event_time"] = pd.to_datetime(urine["charttime"], errors="coerce")
    urine = urine[urine["event_time"].notna()]
    urine = urine[(urine["urine_output_ml"] >= 0) & (urine["urine_output_ml"] <= 5000)]
    urine = urine.drop_duplicates()

    return patients, admissions, icustays, creat, urine

def combine_and_engineer(creat: pd.DataFrame, urine: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    creat_events = creat[["subject_id", "hadm_id", "event_time", "creatinine"]].copy()
    creat_events["event_type"] = "creatinine"
    creat_events["value"] = creat_events["creatinine"]
    creat_events["unit"] = "mg/dL"
    creat_events = creat_events.drop(columns=["creatinine"])

    urine_events = urine[["subject_id", "hadm_id", "stay_id", "event_time", "urine_output_ml"]].copy()
    urine_events["event_type"] = "urine_output"
    urine_events["value"] = urine_events["urine_output_ml"]
    urine_events["unit"] = "mL"
    urine_events = urine_events.drop(columns=["urine_output_ml"])

    timeline = pd.concat([creat_events, urine_events], ignore_index=True, sort=False)
    timeline = timeline.sort_values(["subject_id", "event_time", "event_type"]).reset_index(drop=True)

    creat_hourly = (
        creat.assign(hour_bucket=lambda d: d["event_time"].dt.floor("h"))
        .groupby(["subject_id", "hadm_id", "hour_bucket"], as_index=False)
        .agg({"creatinine": "last"})
    )

    urine_hourly = (
        urine.assign(hour_bucket=lambda d: d["event_time"].dt.floor("h"))
        .groupby(["subject_id", "hadm_id", "stay_id", "hour_bucket"], as_index=False)
        .agg({"urine_output_ml": "sum"})
    )

    hourly = (
        pd.merge(creat_hourly, urine_hourly, on=["subject_id", "hadm_id", "hour_bucket"], how="outer")
        .sort_values(["subject_id", "hour_bucket"])
        .reset_index(drop=True)
    )

    # MIMIC demo does not always have weight handy in the reduced pipeline; use placeholder
    if "admissionweight" not in hourly.columns:
        hourly["admissionweight"] = 70.0

    hourly["urine_output_ml"] = hourly["urine_output_ml"].fillna(0)
    hourly["baseline_creatinine"] = hourly.groupby("subject_id")["creatinine"].transform("min")
    hourly["creatinine_ratio"] = hourly["creatinine"] / hourly["baseline_creatinine"]
    hourly["creatinine_prev_1h"] = hourly.groupby("subject_id")["creatinine"].shift(1)
    hourly["creatinine_delta_1h"] = hourly["creatinine"] - hourly["creatinine_prev_1h"]
    hourly["creatinine_prev_48h"] = hourly.groupby("subject_id")["creatinine"].shift(48)
    hourly["creatinine_delta_48h"] = hourly["creatinine"] - hourly["creatinine_prev_48h"]
    hourly["urine_ml_6h"] = hourly.groupby("subject_id")["urine_output_ml"].transform(lambda s: s.rolling(6, min_periods=1).sum())
    hourly["urine_ml_12h"] = hourly.groupby("subject_id")["urine_output_ml"].transform(lambda s: s.rolling(12, min_periods=1).sum())
    hourly["urine_ml_24h"] = hourly.groupby("subject_id")["urine_output_ml"].transform(lambda s: s.rolling(24, min_periods=1).sum())
    hourly["urine_ml_per_kg_hr_6h"] = hourly["urine_ml_6h"] / (hourly["admissionweight"] * 6)
    hourly["urine_ml_per_kg_hr_12h"] = hourly["urine_ml_12h"] / (hourly["admissionweight"] * 12)
    hourly["urine_ml_per_kg_hr_24h"] = hourly["urine_ml_24h"] / (hourly["admissionweight"] * 24)

    return timeline, hourly, hourly.copy()

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

def main():
    tables = load_raw_tables()
    patients, admissions, icustays, creat, urine = clean_tables(tables)

    patients.to_csv(CLEAN / "patients_clean.csv", index=False)
    admissions.to_csv(CLEAN / "admissions_clean.csv", index=False)
    icustays.to_csv(CLEAN / "icustays_clean.csv", index=False)
    creat.to_csv(CLEAN / "creatinine_clean.csv", index=False)
    urine.to_csv(CLEAN / "urine_clean.csv", index=False)

    timeline, hourly, features = combine_and_engineer(creat, urine)
    timeline.to_csv(COMBINED / "patient_timeline.csv", index=False)
    hourly.to_csv(COMBINED / "hourly_features.csv", index=False)
    features.to_csv(FEATURES / "hourly_engineered_features.csv", index=False)

    labeled = features.copy()
    labeled["kdigo_stage"] = labeled.apply(kdigo_stage, axis=1)
    labeled.to_csv(LABELED / "hourly_kdigo_labeled.csv", index=False)

    print("Finished MIMIC demo pipeline:")
    print(" cleaned ->", CLEAN)
    print(" combined ->", COMBINED)
    print(" features ->", FEATURES)
    print(" labeled ->", LABELED)

if __name__ == "__main__":
    main()

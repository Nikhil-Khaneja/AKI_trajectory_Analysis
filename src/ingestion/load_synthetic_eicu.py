from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
RAW = ROOT / "data" / "raw" / "synthetic_eicu_demo"
CLEAN = ROOT / "data" / "cleaned" / "synthetic_eicu_demo"
COMBINED = ROOT / "data" / "combined" / "synthetic_eicu_demo"
FEATURES = ROOT / "data" / "features" / "synthetic_eicu_demo"
LABELED = ROOT / "data" / "labeled" / "synthetic_eicu_demo"

for p in [CLEAN, COMBINED, FEATURES, LABELED]:
    p.mkdir(parents=True, exist_ok=True)

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
    patient = pd.read_csv(RAW / "patient.csv").drop_duplicates()
    lab = pd.read_csv(RAW / "lab.csv").drop_duplicates()
    intake = pd.read_csv(RAW / "intakeOutput.csv").drop_duplicates()

    patient = patient[patient["patientunitstayid"].notna()]
    patient = patient[(patient["age"] >= 0) & (patient["age"] <= 120)]
    patient = patient[(patient["admissionweight"] >= 30) & (patient["admissionweight"] <= 250)]

    lab["labname"] = lab["labname"].astype(str).str.strip().str.lower()
    lab = lab[lab["patientunitstayid"].notna() & lab["labresultoffset"].notna() & lab["labresult"].notna()]
    lab = lab[(lab["labname"] != "creatinine") | ((lab["labresult"] >= 0.3) & (lab["labresult"] <= 15.0))]

    intake = intake[intake["patientunitstayid"].notna() & intake["intakeoutputoffset"].notna() & intake["cellvaluenumeric"].notna()]
    intake = intake[(intake["cellvaluenumeric"] >= 0) & (intake["cellvaluenumeric"] <= 5000)]

    patient.to_csv(CLEAN / "patient_clean.csv", index=False)
    lab.to_csv(CLEAN / "lab_clean.csv", index=False)
    intake.to_csv(CLEAN / "intakeOutput_clean.csv", index=False)

    creat = lab[lab["labname"] == "creatinine"].copy()
    creat = creat.rename(columns={"labresultoffset": "offset_min", "labresult": "creatinine"})
    intake = intake.rename(columns={"intakeoutputoffset": "offset_min", "cellvaluenumeric": "urine_output_ml"}).copy()

    base_time = pd.Timestamp("2022-01-01 00:00:00")
    creat["event_time"] = base_time + pd.to_timedelta(creat["offset_min"], unit="m")
    intake["event_time"] = base_time + pd.to_timedelta(intake["offset_min"], unit="m")

    meta_cols = [
        "patientunitstayid", "uniquepid", "hospitalid", "region", "unittype",
        "age", "gender", "ethnicity", "admissionweight", "apacheadmissiondx"
    ]
    creat = creat.merge(patient[meta_cols], on="patientunitstayid", how="left")
    intake = intake.merge(patient[meta_cols], on="patientunitstayid", how="left")

    creat_events = creat[["patientunitstayid", "uniquepid", "hospitalid", "event_time", "age", "gender", "ethnicity", "admissionweight", "unittype", "apacheadmissiondx", "creatinine"]].copy()
    creat_events["event_type"] = "creatinine"
    creat_events["value"] = creat_events["creatinine"]
    creat_events["unit"] = "mg/dL"
    creat_events = creat_events.drop(columns=["creatinine"])

    urine_events = intake[["patientunitstayid", "uniquepid", "hospitalid", "event_time", "age", "gender", "ethnicity", "admissionweight", "unittype", "apacheadmissiondx", "urine_output_ml"]].copy()
    urine_events["event_type"] = "urine_output"
    urine_events["value"] = urine_events["urine_output_ml"]
    urine_events["unit"] = "mL"
    urine_events = urine_events.drop(columns=["urine_output_ml"])

    timeline = pd.concat([creat_events, urine_events], ignore_index=True, sort=False)
    timeline = timeline.sort_values(["patientunitstayid", "event_time", "event_type"]).reset_index(drop=True)
    timeline.to_csv(COMBINED / "patient_timeline.csv", index=False)
    creat_events.to_csv(COMBINED / "creatinine_events.csv", index=False)
    urine_events.to_csv(COMBINED / "urine_events.csv", index=False)

    creat_hourly = (
        creat.assign(hour_bucket=lambda d: d["event_time"].dt.floor("h"))
        .groupby(["patientunitstayid", "hour_bucket", "admissionweight", "hospitalid", "age", "gender"], as_index=False)
        .agg({"creatinine": "last"})
    )

    urine_hourly = (
        intake.assign(hour_bucket=lambda d: d["event_time"].dt.floor("h"))
        .groupby(["patientunitstayid", "hour_bucket", "admissionweight", "hospitalid", "age", "gender"], as_index=False)
        .agg({"urine_output_ml": "sum"})
    )

    hourly = pd.merge(
        creat_hourly,
        urine_hourly,
        on=["patientunitstayid", "hour_bucket", "admissionweight", "hospitalid", "age", "gender"],
        how="outer"
    ).sort_values(["patientunitstayid", "hour_bucket"]).reset_index(drop=True)

    hourly["urine_output_ml"] = hourly["urine_output_ml"].fillna(0)
    hourly["baseline_creatinine"] = hourly.groupby("patientunitstayid")["creatinine"].transform("min")
    hourly["creatinine_ratio"] = hourly["creatinine"] / hourly["baseline_creatinine"]
    hourly["creatinine_prev_1h"] = hourly.groupby("patientunitstayid")["creatinine"].shift(1)
    hourly["creatinine_delta_1h"] = hourly["creatinine"] - hourly["creatinine_prev_1h"]
    hourly["creatinine_prev_48h"] = hourly.groupby("patientunitstayid")["creatinine"].shift(48)
    hourly["creatinine_delta_48h"] = hourly["creatinine"] - hourly["creatinine_prev_48h"]
    hourly["urine_ml_6h"] = hourly.groupby("patientunitstayid")["urine_output_ml"].transform(lambda s: s.rolling(6, min_periods=1).sum())
    hourly["urine_ml_12h"] = hourly.groupby("patientunitstayid")["urine_output_ml"].transform(lambda s: s.rolling(12, min_periods=1).sum())
    hourly["urine_ml_24h"] = hourly.groupby("patientunitstayid")["urine_output_ml"].transform(lambda s: s.rolling(24, min_periods=1).sum())
    hourly["urine_ml_per_kg_hr_6h"] = hourly["urine_ml_6h"] / (hourly["admissionweight"] * 6)
    hourly["urine_ml_per_kg_hr_12h"] = hourly["urine_ml_12h"] / (hourly["admissionweight"] * 12)
    hourly["urine_ml_per_kg_hr_24h"] = hourly["urine_ml_24h"] / (hourly["admissionweight"] * 24)

    hourly.to_csv(COMBINED / "hourly_features.csv", index=False)
    hourly.to_csv(FEATURES / "hourly_engineered_features.csv", index=False)

    labeled = hourly.copy()
    labeled["kdigo_stage"] = labeled.apply(kdigo_stage, axis=1)
    labeled.to_csv(LABELED / "hourly_kdigo_labeled.csv", index=False)

    print("Finished synthetic eICU pipeline:")
    print(" cleaned ->", CLEAN)
    print(" combined ->", COMBINED)
    print(" features ->", FEATURES)
    print(" labeled ->", LABELED)

if __name__ == "__main__":
    main()

import pandas as pd
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
TRAINING_DATA = ROOT / "outputs" / "tables" / "synthetic_eicu_training_set.csv"
DEMOGRAPHICS = ROOT / "data" / "combined" / "synthetic_eicu_demo" / "patient_timeline.csv"
MART_DIR = ROOT / "outputs" / "mart"
MART_DIR.mkdir(parents=True, exist_ok=True)

def apply_k_anonymity(df, k=5, qi_cols=None):
    """
    Ensures each combination of quasi-identifiers has at least k records.
    Rows that don't meet the threshold are suppressed.
    """
    if qi_cols is None:
        qi_cols = ["age_group", "gender", "unittype"]
        
    # Group by quasi-identifiers
    grouped = df.groupby(qi_cols).size().reset_index(name='count')
    
    # Identify valid groups (size >= k)
    valid_groups = grouped[grouped['count'] >= k][qi_cols]
    
    # Filter original dataframe
    anonymized_df = df.merge(valid_groups, on=qi_cols, how='inner')
    
    suppressed_count = len(df) - len(anonymized_df)
    print(f"K-Anonymity (k={k}) check complete.")
    print(f"Total rows: {len(df)}")
    print(f"Anonymized rows: {len(anonymized_df)}")
    print(f"Suppressed rows: {suppressed_count} ({suppressed_count/len(df):.2%})")
    
    return anonymized_df

def main():
    print("Loading datasets for anonymization...")
    train_df = pd.read_csv(TRAINING_DATA)
    demo_df = pd.read_csv(DEMOGRAPHICS)
    
    # Extract unique demographics per patient
    demo_unique = demo_df[["patientunitstayid", "age", "gender", "ethnicity", "unittype"]].drop_duplicates()
    
    # Join features with demographics
    merged_df = train_df.merge(demo_unique, on="patientunitstayid", how="left")
    
    # 1. Generalization Step
    print("Generalizing quasi-identifiers...")
    # Age bucketing (e.g., 18-30, 30-40, ...)
    merged_df['age_group'] = pd.cut(
        merged_df['age'].astype(float), 
        bins=[0, 18, 30, 40, 50, 60, 70, 80, 120],
        labels=['<18', '18-30', '30-40', '40-50', '50-60', '60-70', '70-80', '80+']
    )
    
    # 2. Privacy Check
    qi_cols = ["age_group", "gender", "unittype"]
    anonymized_mart = apply_k_anonymity(merged_df, k=5, qi_cols=qi_cols)
    
    # Save to Mart layer
    out_path = MART_DIR / "k_anonymized_training_set.csv"
    anonymized_mart.to_csv(out_path, index=False)
    print(f"Anonymized Mart table saved to {out_path}")

if __name__ == "__main__":
    main()

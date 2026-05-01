import numpy as np
import pandas as pd
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
MART_DIR = ROOT / "outputs" / "mart"
ANONYMIZED_DATA = MART_DIR / "k_anonymized_training_set.csv"

def laplace_mechanism(value, sensitivity, epsilon):
    """Adds Laplace noise to a value."""
    if epsilon == 0:
        return value
    noise = np.random.laplace(0, sensitivity / epsilon)
    return value + noise

def calculate_dp_aggregates(df, epsilon=1.0):
    """
    Calculates differentially private average creatinine per ICU unit.
    Sensitivity is approximated as (max - min) / n_rows_in_group.
    """
    print(f"Calculating DP aggregates with epsilon={epsilon}...")
    
    results = []
    for unit, group in df.groupby("unittype"):
        n = len(group)
        if n == 0: continue
        
        actual_mean = group["creatinine"].mean()
        
        # Sensitivity for mean = (max - min) / n
        # For creatinine, max approx 5.0, min 0.5
        sensitivity = (5.0 - 0.5) / n
        
        dp_mean = laplace_mechanism(actual_mean, sensitivity, epsilon)
        
        results.append({
            "unittype": unit,
            "patient_count": n,
            "actual_avg_creatinine": actual_mean,
            "dp_avg_creatinine": dp_mean,
            "noise_added": dp_mean - actual_mean
        })
        
    return pd.DataFrame(results)

def main():
    if not ANONYMIZED_DATA.exists():
        print("Please run k_anonymity.py first.")
        return
        
    df = pd.read_csv(ANONYMIZED_DATA)
    
    # Generate DP Report for Dashboard
    dp_report = calculate_dp_aggregates(df, epsilon=0.5)
    
    out_path = MART_DIR / "dp_unit_metrics.csv"
    dp_report.to_csv(out_path, index=False)
    print("\nDifferentially Private Aggregates for Dashboard:")
    print(dp_report[["unittype", "actual_avg_creatinine", "dp_avg_creatinine", "noise_added"]])
    print(f"\nDP metrics saved to {out_path}")

if __name__ == "__main__":
    main()

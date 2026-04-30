"""
Module 5 v1.2.0: K-Anonymity with k>=5 for patient privacy protection.
Age generalization: decade buckets (62 → '60-69')
Owner: Naveen
"""
import pandas as pd
from typing import List


QUASI_IDENTIFIERS = ["age_bucket", "gender", "care_unit"]
K_MIN = 5


def generalize_age(age: float) -> str:
    """
    Generalize age to decade buckets per v1.2.0.
    Example: 62 → '60-69'
    """
    if pd.isna(age):
        return "unknown"
    age = float(age)
    if age < 40:
        return "<40"
    elif age < 50:
        return "40-49"
    elif age < 60:
        return "50-59"
    elif age < 70:
        return "60-69"
    elif age < 80:
        return "70-79"
    else:
        return "80+"


def generalize_care_unit(unit: str) -> str:
    """Generalize care unit to broader categories."""
    if pd.isna(unit):
        return "other"
    unit_lower = str(unit).lower()
    if "med" in unit_lower:
        return "medical"
    if "surg" in unit_lower or "sicu" in unit_lower:
        return "surgical"
    if "card" in unit_lower or "ccu" in unit_lower:
        return "cardiac"
    return "other"


def apply_k_anonymity(df: pd.DataFrame, k: int = K_MIN, mimic_iv_only: bool = True) -> pd.DataFrame:
    """
    Apply K-Anonymity generalization. Returns privacy-safe DataFrame.
    v1.2.0: Applied on MIMIC-IV only data.
    
    Args:
        df: Input DataFrame
        k: Minimum group size (default 5)
        mimic_iv_only: If True, filter to MIMIC-IV data only
    
    Returns:
        K-anonymized DataFrame
    """
    df = df.copy()
    
    # Filter to MIMIC-IV only if requested
    if mimic_iv_only and "source" in df.columns:
        df = df[df["source"] == "mimic-iv"]
    
    if "age" in df.columns:
        df["age_bucket"] = df["age"].apply(generalize_age)
    if "care_unit" in df.columns:
        df["care_unit"] = df["care_unit"].apply(generalize_care_unit)

    # Check group sizes and suppress small groups
    qi_cols = [c for c in QUASI_IDENTIFIERS if c in df.columns]
    if qi_cols:
        group_sizes = df.groupby(qi_cols).transform("count").iloc[:, 0]
        suppressed = (group_sizes < k).sum()
        df = df[group_sizes >= k]
        print(f"K-Anonymity v1.2.0 applied: k={k}, decade buckets, suppressed {suppressed} rows")
    
    return df


def validate_k_anonymity(df: pd.DataFrame, k: int = K_MIN) -> bool:
    """Assert all QI groups have size >= k."""
    qi_cols = [c for c in QUASI_IDENTIFIERS if c in df.columns]
    if not qi_cols:
        return True
    min_size = df.groupby(qi_cols).size().min()
    print(f"Min group size: {min_size} (required: {k})")
    is_valid = bool(min_size >= k)
    if not is_valid:
        print("⚠️  K-Anonymity validation FAILED")
    return is_valid


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        df = pd.read_csv(sys.argv[1])
        result = apply_k_anonymity(df, mimic_iv_only=True)
        assert validate_k_anonymity(result), "K-Anonymity validation FAILED"
        result.to_csv("outputs/mart/k_anonymized_training_set.csv", index=False)
        print("✓ K-anonymized dataset saved.")

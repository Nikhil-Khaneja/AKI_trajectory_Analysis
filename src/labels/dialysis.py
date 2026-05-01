from pathlib import Path
import pandas as pd

def add_dialysis_flag(df: pd.DataFrame, medication_col: str = "drugname") -> pd.DataFrame:
    out = df.copy()
    if medication_col in out.columns:
        out["possible_rrt_flag"] = out[medication_col].astype(str).str.contains("dialysis|crrt|hemodialysis", case=False, na=False).astype(int)
    else:
        out["possible_rrt_flag"] = 0
    return out

if __name__ == "__main__":
    print("Dialysis helper module loaded. Use add_dialysis_flag() in a larger pipeline.")

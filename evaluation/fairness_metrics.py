"""
Module 4: Fairness analysis by subgroup (age, gender).
Owner: Nikhil
"""
import pandas as pd
from joblib import load
from sklearn.metrics import roc_auc_score


def fairness_report(model_path: str, test_csv: str) -> pd.DataFrame:
    model = load(model_path)
    df = pd.read_csv(test_csv)
    feature_cols = [
        "creatinine",
        "baseline_creatinine",
        "creatinine_ratio",
        "creatinine_delta_1h",
        "creatinine_delta_48h",
        "urine_ml_per_kg_hr_6h",
        "urine_ml_per_kg_hr_12h",
        "urine_ml_per_kg_hr_24h",
    ]
    df["y_prob"] = model.predict_proba(df[feature_cols])[:, 1]
    results = []
    for group_col in ["age_group", "gender"]:
        if group_col not in df.columns:
            continue
        for group_val in df[group_col].unique():
            sub = df[df[group_col] == group_val]
            if len(sub) < 10:
                continue
            auroc = roc_auc_score(sub["target_progress_to_stage3_48h"], sub["y_prob"])
            results.append(
                {"group_col": group_col, "group_val": group_val, "n": len(sub), "auroc": auroc}
            )
    return pd.DataFrame(results)


if __name__ == "__main__":
    print("Run fairness analysis — provide model path and test CSV")

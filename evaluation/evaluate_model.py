"""
Module 4: Model evaluation — compute all metrics.
Owner: Nikhil
"""
from pathlib import Path

import pandas as pd
from joblib import load
from sklearn.metrics import (
    accuracy_score,
    average_precision_score,
    brier_score_loss,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)

ROOT = Path(__file__).resolve().parents[1]


def evaluate(model_path: str, test_csv: str, model_name: str):
    model = load(model_path)
    df = pd.read_csv(test_csv)
    x = df.drop(
        columns=[
            "patientunitstayid",
            "hour_bucket",
            "kdigo_stage",
            "target_progress_to_stage3_48h",
        ]
    )
    y = df["target_progress_to_stage3_48h"]

    y_prob = model.predict_proba(x)[:, 1]
    y_pred = model.predict(x)

    metrics = {
        "model": model_name,
        "auroc": roc_auc_score(y, y_prob),
        "auprc": average_precision_score(y, y_prob),
        "precision": precision_score(y, y_pred),
        "recall": recall_score(y, y_pred),
        "f1": f1_score(y, y_pred),
        "accuracy": accuracy_score(y, y_pred),
        "brier_score": brier_score_loss(y, y_prob),
    }

    out_dir = ROOT / "outputs/metrics"
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / f"{model_name}_metrics.csv"
    pd.DataFrame([metrics]).to_csv(out, index=False)
    print(f"Metrics saved: {out}")
    return metrics


if __name__ == "__main__":
    evaluate(
        str(ROOT / "outputs/models/logistic_regression.joblib"),
        str(ROOT / "outputs/tables/synthetic_eicu_training_set.csv"),
        "logistic_regression",
    )
    evaluate(
        str(ROOT / "outputs/models/gradient_boosting.joblib"),
        str(ROOT / "outputs/tables/synthetic_eicu_training_set.csv"),
        "gradient_boosting",
    )

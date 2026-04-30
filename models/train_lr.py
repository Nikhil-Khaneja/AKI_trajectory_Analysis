"""
Module 4: Logistic Regression training with MLflow tracking.
Owner: Nikhil
"""
from pathlib import Path

import mlflow
import mlflow.sklearn
import pandas as pd
from joblib import dump
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import average_precision_score, f1_score, roc_auc_score
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline

ROOT = Path(__file__).resolve().parents[1]
INPUT = ROOT / "outputs/tables/synthetic_eicu_training_set.csv"
MODELS = ROOT / "outputs/models"
MODELS.mkdir(parents=True, exist_ok=True)

FEATURE_COLS = [
    "creatinine",
    "baseline_creatinine",
    "creatinine_ratio",
    "creatinine_delta_1h",
    "creatinine_delta_48h",
    "urine_ml_per_kg_hr_6h",
    "urine_ml_per_kg_hr_12h",
    "urine_ml_per_kg_hr_24h",
    "hours_since_admit",
]


def train():
    df = pd.read_csv(INPUT)
    x = df[FEATURE_COLS]
    y = df["target_progress_to_stage3_48h"]
    x_train, x_test, y_train, y_test = train_test_split(
        x, y, test_size=0.2, random_state=42, stratify=y
    )

    with mlflow.start_run(run_name="logistic_regression"):
        model = Pipeline(
            [
                ("imputer", SimpleImputer(strategy="median")),
                ("lr", LogisticRegression(max_iter=1000, C=1.0, solver="lbfgs")),
            ]
        )
        model.fit(x_train, y_train)

        y_prob = model.predict_proba(x_test)[:, 1]
        y_pred = model.predict(x_test)

        auroc = roc_auc_score(y_test, y_prob)
        auprc = average_precision_score(y_test, y_prob)
        f1 = f1_score(y_test, y_pred)

        mlflow.log_params({"C": 1.0, "max_iter": 1000, "solver": "lbfgs"})
        mlflow.log_metrics({"auroc": auroc, "auprc": auprc, "f1": f1})
        mlflow.sklearn.log_model(model, "logistic_regression")

        dump(model, MODELS / "logistic_regression.joblib")
        print(f"LR AUROC={auroc:.4f} | AUPRC={auprc:.4f} | F1={f1:.4f}")


if __name__ == "__main__":
    train()

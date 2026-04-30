"""
Module 4: Gradient Boosting training with MLflow tracking.
Owner: Nikitha
"""
from pathlib import Path

import mlflow
import mlflow.sklearn
import pandas as pd
from joblib import dump
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.impute import SimpleImputer
from sklearn.metrics import average_precision_score, f1_score, roc_auc_score
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline

ROOT = Path(__file__).resolve().parents[1]
INPUT = ROOT / "outputs/tables/synthetic_eicu_training_set.csv"
MODELS = ROOT / "outputs/models"
MODELS.mkdir(parents=True, exist_ok=True)


def train():
    df = pd.read_csv(INPUT)
    x = df.drop(
        columns=[
            "patientunitstayid",
            "hour_bucket",
            "kdigo_stage",
            "target_progress_to_stage3_48h",
        ]
    )
    y = df["target_progress_to_stage3_48h"]
    x_train, x_test, y_train, y_test = train_test_split(
        x, y, test_size=0.2, random_state=42, stratify=y
    )

    with mlflow.start_run(run_name="gradient_boosting"):
        model = Pipeline(
            [
                ("imputer", SimpleImputer(strategy="median")),
                (
                    "gbt",
                    GradientBoostingClassifier(
                        n_estimators=100, max_depth=3, learning_rate=0.1, random_state=42
                    ),
                ),
            ]
        )
        model.fit(x_train, y_train)

        y_prob = model.predict_proba(x_test)[:, 1]
        y_pred = model.predict(x_test)

        auroc = roc_auc_score(y_test, y_prob)
        auprc = average_precision_score(y_test, y_prob)
        f1 = f1_score(y_test, y_pred)

        mlflow.log_params({"n_estimators": 100, "max_depth": 3, "learning_rate": 0.1})
        mlflow.log_metrics({"auroc": auroc, "auprc": auprc, "f1": f1})
        mlflow.sklearn.log_model(model, "gradient_boosting")

        dump(model, MODELS / "gradient_boosting.joblib")
        print(f"GBT AUROC={auroc:.4f} | AUPRC={auprc:.4f} | F1={f1:.4f}")


if __name__ == "__main__":
    train()

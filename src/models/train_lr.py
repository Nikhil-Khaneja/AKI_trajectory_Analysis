from pathlib import Path
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.linear_model import LogisticRegression
from joblib import dump

ROOT = Path(__file__).resolve().parents[2]
INPUT = ROOT / "outputs" / "tables" / "synthetic_eicu_training_set.csv"
MODELS = ROOT / "outputs" / "models"
MODELS.mkdir(parents=True, exist_ok=True)

def main():
    df = pd.read_csv(INPUT)
    y = df["target_progress_to_stage3_48h"]
    X = df.drop(columns=["patientunitstayid", "hour_bucket", "kdigo_stage", "target_progress_to_stage3_48h"])

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y if y.nunique() > 1 else None
    )

    model = Pipeline([
        ("imputer", SimpleImputer(strategy="median")),
        ("lr", LogisticRegression(max_iter=1000))
    ])
    model.fit(X_train, y_train)
    dump(model, MODELS / "logistic_regression.joblib")
    print("Saved model to", MODELS / "logistic_regression.joblib")

if __name__ == "__main__":
    main()

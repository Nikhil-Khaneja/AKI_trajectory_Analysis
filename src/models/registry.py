import json
import hashlib
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
REGISTRY_PATH = ROOT / "outputs" / "models" / "registry.json"
TRAINING_DATA = ROOT / "outputs" / "tables" / "synthetic_eicu_training_set.csv"
METRICS_PATH = ROOT / "outputs" / "metrics" / "gradient_boosting_metrics.csv"

def get_file_hash(path: Path):
    """Generates a SHA-256 hash of a file to simulate version tracking."""
    hasher = hashlib.sha256()
    with open(path, 'rb') as f:
        buf = f.read()
        hasher.update(buf)
    return hasher.hexdigest()

def register_model():
    print("Registering model in MLflow-like registry...")
    
    if not TRAINING_DATA.exists() or not METRICS_PATH.exists():
        print("Missing data or metrics. Run pipeline first.")
        return

    # Load metrics
    metrics_df = pd.read_csv(METRICS_PATH)
    metrics = metrics_df.iloc[0].to_dict()
    
    # Calculate lineage
    data_version = get_file_hash(TRAINING_DATA)
    
    registry_entry = {
        "run_id": "aki_run_" + data_version[:8],
        "model_name": "Gradient Boosting AKI Predictor",
        "timestamp": pd.Timestamp.now().isoformat(),
        "lineage": {
            "training_data_file": str(TRAINING_DATA.relative_to(ROOT)),
            "data_version_hash": data_version,
            "dbt_logic_version": "v1.2.0" # Simulated dbt version
        },
        "parameters": {
            "n_estimators": 100,
            "learning_rate": 0.1,
            "max_depth": 3
        },
        "metrics": metrics
    }
    
    # Load existing registry or create new
    if REGISTRY_PATH.exists():
        with open(REGISTRY_PATH, "r") as f:
            registry = json.load(f)
    else:
        registry = []
        
    registry.append(registry_entry)
    
    with open(REGISTRY_PATH, "w") as f:
        json.dump(registry, f, indent=4)
        
    print(f"Model successfully registered with Run ID: {registry_entry['run_id']}")
    print(f"Metrics tracked: AUROC={metrics['auroc']:.4f}, F1={metrics['f1']:.4f}")

if __name__ == "__main__":
    register_model()

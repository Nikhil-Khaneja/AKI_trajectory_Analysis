import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
METRICS = ROOT / "outputs" / "metrics"

def plot_roc():
    plt.figure(figsize=(10, 6))
    
    # Find all roc_curve.csv files
    roc_files = list(METRICS.glob("*_roc_curve.csv"))
    
    if not roc_files:
        print("No ROC curve data found.")
        return

    for rf in roc_files:
        label = rf.name.replace("_roc_curve.csv", "").replace("_", " ").title()
        df = pd.read_csv(rf)
        plt.plot(df["fpr"], df["tpr"], label=label)
        
    plt.plot([0, 1], [0, 1], 'k--', label='Random (AUC = 0.5)')
    plt.xlabel('False Positive Rate')
    plt.ylabel('True Positive Rate')
    plt.title('ROC Curves')
    plt.legend()
    plt.grid(True, alpha=0.3)
    
    plot_path = METRICS / "roc_curves.png"
    plt.savefig(plot_path)
    print(f"ROC curves plot saved to {plot_path}")

if __name__ == "__main__":
    plot_roc()

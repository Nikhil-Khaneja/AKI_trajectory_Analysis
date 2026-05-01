import numpy as np
import pandas as pd
from sklearn.metrics import (
    roc_auc_score, 
    average_precision_score, 
    brier_score_loss,
    precision_score,
    recall_score,
    f1_score,
    accuracy_score,
    roc_curve
)

def calculate_ece(y_true, y_prob, n_bins=10):
    """Calculates Expected Calibration Error (ECE)."""
    bin_boundaries = np.linspace(0, 1, n_bins + 1)
    bin_lowers = bin_boundaries[:-1]
    bin_uppers = bin_boundaries[1:]
    
    ece = 0.0
    for bin_lower, bin_upper in zip(bin_lowers, bin_uppers):
        # Index of items in this bin
        in_bin = (y_prob > bin_lower) & (y_prob <= bin_upper)
        prop_in_bin = np.mean(in_bin)
        
        if prop_in_bin > 0:
            accuracy_in_bin = np.mean(y_true[in_bin])
            avg_confidence_in_bin = np.mean(y_prob[in_bin])
            ece += prop_in_bin * np.abs(avg_confidence_in_bin - accuracy_in_bin)
            
    return float(ece)

def binary_classification_metrics(y_true, y_prob, threshold=0.5) -> dict:
    y_pred = (y_prob >= threshold).astype(int)
    metrics = {}
    
    if len(np.unique(y_true)) > 1:
        metrics["auroc"] = float(roc_auc_score(y_true, y_prob))
        metrics["auprc"] = float(average_precision_score(y_true, y_prob))
    else:
        metrics["auroc"] = None
        metrics["auprc"] = None
        
    metrics["precision"] = float(precision_score(y_true, y_pred, zero_division=0))
    metrics["recall"] = float(recall_score(y_true, y_pred, zero_division=0))
    metrics["f1"] = float(f1_score(y_true, y_pred, zero_division=0))
    metrics["accuracy"] = float(accuracy_score(y_true, y_pred))
    metrics["brier_score"] = float(brier_score_loss(y_true, y_prob))
    metrics["ece"] = calculate_ece(y_true, y_prob)
    
    return metrics

def get_roc_curve_data(y_true, y_prob):
    fpr, tpr, thresholds = roc_curve(y_true, y_prob)
    return pd.DataFrame({
        "fpr": fpr,
        "tpr": tpr,
        "thresholds": thresholds
    })

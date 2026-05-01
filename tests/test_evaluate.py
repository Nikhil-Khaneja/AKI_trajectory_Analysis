"""Unit tests for evaluation utilities."""
from sklearn.metrics import roc_auc_score


def test_auroc_perfect():
    y_true = [0, 0, 1, 1]
    y_prob = [0.1, 0.2, 0.8, 0.9]
    assert roc_auc_score(y_true, y_prob) == 1.0


def test_auroc_random():
    y_true = [0, 1, 0, 1]
    y_prob = [0.5, 0.5, 0.5, 0.5]
    assert roc_auc_score(y_true, y_prob) == 0.5

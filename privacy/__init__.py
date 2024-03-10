"""
Module 5: Privacy & Fairness Package
Owner: Naveen
Version: 1.2.0
"""

from .k_anonymity import apply_k_anonymity, validate_k_anonymity, generalize_age
from .differential_privacy import dp_aggregate_stats, laplace_noise

__all__ = [
    "apply_k_anonymity",
    "validate_k_anonymity", 
    "generalize_age",
    "dp_aggregate_stats",
    "laplace_noise"
]

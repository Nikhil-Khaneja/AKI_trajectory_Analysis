"""
Module 1 v1.2.0: pytest fixtures shared across all module_1_ingestion tests.
Owner: Nikhil
"""

import pandas as pd
import pytest


# ── Creatinine DataFrames ─────────────────────────────────────────────────────

@pytest.fixture
def valid_creatinine_df():
    """Well-formed creatinine DataFrame that should pass GX Checkpoint #1."""
    return pd.DataFrame({
        "subject_id": [10001, 10002, 10003, 10004],
        "hadm_id": [200001, 200002, 200003, 200004],
        "charttime": pd.to_datetime([
            "2150-01-01 08:00", "2150-01-01 09:00",
            "2150-01-02 10:00", "2150-01-03 11:00",
        ]),
        "valuenum": [1.2, 0.8, 3.5, 0.3],    # 0.3 is the lower bound (inclusive)
        "valueuom": ["mg/dL"] * 4,
        "itemid": [50912, 51081, 52024, 52546],
    })


@pytest.fixture
def boundary_creatinine_df():
    """Boundary values 0.3 and 15.0 — both should PASS (inclusive range)."""
    return pd.DataFrame({
        "subject_id": [10010, 10011],
        "hadm_id": [200010, 200011],
        "charttime": pd.to_datetime(["2150-01-01 08:00", "2150-01-01 09:00"]),
        "valuenum": [0.3, 15.0],
        "valueuom": ["mg/dL", "mg/dL"],
        "itemid": [50912, 50912],
    })


@pytest.fixture
def out_of_range_low_creatinine_df():
    """Values below 0.3 — should FAIL creatinine range expectation."""
    return pd.DataFrame({
        "subject_id": [10020, 10021],
        "hadm_id": [200020, 200021],
        "charttime": pd.to_datetime(["2150-01-01 08:00", "2150-01-01 09:00"]),
        "valuenum": [0.1, 0.2],   # below 0.3 minimum
        "valueuom": ["mg/dL", "mg/dL"],
        "itemid": [50912, 50912],
    })


@pytest.fixture
def out_of_range_high_creatinine_df():
    """Values above 15.0 — should FAIL creatinine range expectation."""
    return pd.DataFrame({
        "subject_id": [10030, 10031],
        "hadm_id": [200030, 200031],
        "charttime": pd.to_datetime(["2150-01-01 08:00", "2150-01-01 09:00"]),
        "valuenum": [16.0, 20.0],   # above 15.0 maximum
        "valueuom": ["mg/dL", "mg/dL"],
        "itemid": [50912, 50912],
    })


@pytest.fixture
def null_subject_id_df():
    """Null subject_id — should FAIL 100% null policy."""
    return pd.DataFrame({
        "subject_id": [None, 10002],
        "hadm_id": [200001, 200002],
        "charttime": pd.to_datetime(["2150-01-01 08:00", "2150-01-01 09:00"]),
        "valuenum": [1.2, 0.8],
        "valueuom": ["mg/dL", "mg/dL"],
        "itemid": [50912, 50912],
    })


@pytest.fixture
def null_charttime_df():
    """Null charttime — should FAIL 100% null policy."""
    return pd.DataFrame({
        "subject_id": [10001, 10002],
        "hadm_id": [200001, 200002],
        "charttime": [None, pd.Timestamp("2150-01-01 09:00")],
        "valuenum": [1.2, 0.8],
        "valueuom": ["mg/dL", "mg/dL"],
        "itemid": [50912, 50912],
    })


@pytest.fixture
def wrong_uom_df():
    """valueuom not in ['mg/dL'] — should FAIL valueuom expectation."""
    return pd.DataFrame({
        "subject_id": [10001, 10002],
        "hadm_id": [200001, 200002],
        "charttime": pd.to_datetime(["2150-01-01 08:00", "2150-01-01 09:00"]),
        "valuenum": [1.2, 0.8],
        "valueuom": ["umol/L", "mmol/L"],   # wrong units
        "itemid": [50912, 50912],
    })


# ── Urine output DataFrames ───────────────────────────────────────────────────

@pytest.fixture
def valid_urine_df():
    return pd.DataFrame({
        "subject_id": [10001, 10002, 10003],
        "hadm_id": [200001, 200002, 200003],
        "charttime": pd.to_datetime([
            "2150-01-01 08:00", "2150-01-01 09:00", "2150-01-01 10:00"
        ]),
        "valuenum": [250.0, 0.0, 450.5],
        "valueuom": ["mL", "mL", "mL"],
        "itemid": [226559, 226560, 226567],
    })

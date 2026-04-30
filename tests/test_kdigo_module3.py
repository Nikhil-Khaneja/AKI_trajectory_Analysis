import pandas as pd

from labels.kdigo import kdigo_stage


def test_stage_3_by_ratio():
    row = pd.Series(
        {
            "creatinine_ratio": 3.5,
            "creatinine": 2.0,
            "urine_output_velocity_24h": 0.5,
            "urine_output_velocity_12h": 1.0,
            "cr_delta_48h": 0.1,
            "urine_output_velocity_6h": 1.0,
        }
    )
    assert kdigo_stage(row) == 3


def test_stage_2_by_urine_12h():
    row = pd.Series(
        {
            "creatinine_ratio": 1.6,
            "creatinine": 1.2,
            "urine_output_velocity_24h": 0.6,
            "urine_output_velocity_12h": 0.4,
            "cr_delta_48h": 0.1,
            "urine_output_velocity_6h": 0.6,
        }
    )
    assert kdigo_stage(row) == 2


def test_stage_1_by_delta_48h():
    row = pd.Series(
        {
            "creatinine_ratio": 1.0,
            "creatinine": 1.1,
            "urine_output_velocity_24h": 1.0,
            "urine_output_velocity_12h": 1.0,
            "cr_delta_48h": 0.31,
            "urine_output_velocity_6h": 1.0,
        }
    )
    assert kdigo_stage(row) == 1


def test_stage_0():
    row = pd.Series(
        {
            "creatinine_ratio": 1.0,
            "creatinine": 1.0,
            "urine_output_velocity_24h": 1.0,
            "urine_output_velocity_12h": 1.0,
            "cr_delta_48h": 0.1,
            "urine_output_velocity_6h": 1.0,
        }
    )
    assert kdigo_stage(row) == 0


"""
Module 1 v1.2.0: pytest suite for GX Checkpoint #1.
Owner: Nikhil

Tests:
  - Valid creatinine data passes all expectations
  - Boundary values 0.3 and 15.0 pass (inclusive)
  - Values below 0.3 fail range expectation
  - Values above 15.0 fail range expectation
  - Null subject_id fails 100% null policy
  - Null charttime fails 100% null policy
  - Wrong valueuom fails set expectation
  - Valid urine output passes
  - Empty DataFrame raises ValueError
"""

import pytest
import pandas as pd
import sys
from pathlib import Path

# Ensure module_1_ingestion is importable
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "validation"))

from ge_checkpoint_1 import run_checkpoint_1


class TestGXCheckpoint1Creatinine:
    """v1.2.0 creatinine expectations (range 0.3–15.0, 100% nulls)."""

    def test_valid_data_passes(self, valid_creatinine_df):
        result = run_checkpoint_1(valid_creatinine_df, source_type="creatinine", save_report=False)
        assert result["passed"] is True
        assert result["success_rate"] == 1.0
        assert result["failed_expectations"] == []

    def test_boundary_values_pass(self, boundary_creatinine_df):
        """0.3 and 15.0 are inclusive boundaries — both must PASS."""
        result = run_checkpoint_1(boundary_creatinine_df, source_type="creatinine", save_report=False)
        assert result["passed"] is True, (
            f"Boundary values 0.3 and 15.0 should pass. "
            f"Failed: {result['failed_expectations']}"
        )

    def test_below_min_range_fails(self, out_of_range_low_creatinine_df):
        """Values 0.1 and 0.2 are below 0.3 minimum — must FAIL."""
        result = run_checkpoint_1(out_of_range_low_creatinine_df, source_type="creatinine", save_report=False)
        assert result["passed"] is False
        failed_cols = [f["column"] for f in result["failed_expectations"]]
        assert "valuenum" in failed_cols

    def test_above_max_range_fails(self, out_of_range_high_creatinine_df):
        """Values 16.0 and 20.0 are above 15.0 maximum — must FAIL."""
        result = run_checkpoint_1(out_of_range_high_creatinine_df, source_type="creatinine", save_report=False)
        assert result["passed"] is False
        failed_cols = [f["column"] for f in result["failed_expectations"]]
        assert "valuenum" in failed_cols

    def test_null_subject_id_fails(self, null_subject_id_df):
        """Null subject_id violates 100% coverage policy — must FAIL."""
        result = run_checkpoint_1(null_subject_id_df, source_type="creatinine", save_report=False)
        assert result["passed"] is False
        failed_cols = [f["column"] for f in result["failed_expectations"]]
        assert "subject_id" in failed_cols

    def test_null_charttime_fails(self, null_charttime_df):
        """Null charttime violates 100% coverage policy — must FAIL."""
        result = run_checkpoint_1(null_charttime_df, source_type="creatinine", save_report=False)
        assert result["passed"] is False
        failed_cols = [f["column"] for f in result["failed_expectations"]]
        assert "charttime" in failed_cols

    def test_wrong_uom_fails(self, wrong_uom_df):
        """valueuom not in ['mg/dL'] must FAIL."""
        result = run_checkpoint_1(wrong_uom_df, source_type="creatinine", save_report=False)
        assert result["passed"] is False
        failed_cols = [f["column"] for f in result["failed_expectations"]]
        assert "valueuom" in failed_cols

    def test_empty_dataframe_raises(self):
        """Empty DataFrame should raise ValueError, not silently pass."""
        with pytest.raises(ValueError, match="empty DataFrame"):
            run_checkpoint_1(pd.DataFrame(), source_type="creatinine", save_report=False)

    def test_missing_column_raises(self):
        """DataFrame missing required columns should raise ValueError."""
        df = pd.DataFrame({"subject_id": [1], "charttime": ["2150-01-01"]})
        with pytest.raises(ValueError, match="missing required columns"):
            run_checkpoint_1(df, source_type="creatinine", save_report=False)


class TestGXCheckpoint1Urine:
    """v1.2.0 urine output expectations (>= 0, 100% nulls)."""

    def test_valid_urine_passes(self, valid_urine_df):
        result = run_checkpoint_1(valid_urine_df, source_type="urine", save_report=False)
        assert result["passed"] is True

    def test_invalid_source_type_raises(self, valid_creatinine_df):
        with pytest.raises(ValueError, match="Unknown source_type"):
            run_checkpoint_1(valid_creatinine_df, source_type="vitals", save_report=False)


class TestGXCheckpoint1HTMLReport:
    """Validate that HTML report is saved when save_report=True."""

    def test_report_saved(self, valid_creatinine_df, tmp_path, monkeypatch):
        import ge_checkpoint_1 as gx1
        monkeypatch.setattr(gx1, "GX_REPORTS_DIR", tmp_path)
        result = run_checkpoint_1(valid_creatinine_df, source_type="creatinine", save_report=True)
        assert result["report_path"] is not None
        report_file = Path(result["report_path"])
        assert report_file.exists()
        content = report_file.read_text()
        assert "GX Quality Gate #1" in content
        assert "0.3" in content   # confirm v1.2.0 range is present in report
        assert "15.0" in content

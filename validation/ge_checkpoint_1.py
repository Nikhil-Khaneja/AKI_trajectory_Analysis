"""
Module 1 v1.2.0: Great Expectations Quality Gate #1 — Raw data validation.
Owner: Nikhil

Gate position: BEFORE writing to Snowflake BRONZE layer.
Runs against each source DataFrame (labevents, outputevents).

v1.2.0 Changes:
  - creatinine range: 0.3–15.0 mg/dL  (was 0.1–20.0 — BREAKING CHANGE)
  - subject_id:  100% not-null (mostly=1.0, zero tolerance)
  - charttime:   100% not-null (mostly=1.0, zero tolerance)
  - valueuom:    IN ['mg/dL'] for creatinine rows
"""

import os
import json
import logging
from datetime import datetime
from pathlib import Path

import pandas as pd
import great_expectations as gx
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.data_context.types.base import DataContextConfig

log = logging.getLogger(__name__)

# Where to save the HTML validation report for the PR
GX_REPORTS_DIR = Path(__file__).resolve().parents[2] / "gx_reports"
GX_REPORTS_DIR.mkdir(exist_ok=True)


# ---------------------------------------------------------------------------
# Expectation builders
# ---------------------------------------------------------------------------

def _add_creatinine_expectations(validator):
    """Expectations applied to creatinine (labevents) DataFrames."""
    # ── v1.2.0: range 0.3–15.0 mg/dL (was 0.1–20.0) ─────────────────────
    validator.expect_column_values_to_be_between(
        column="valuenum",
        min_value=0.3,
        max_value=15.0,
        mostly=0.99,        # allow ≤ 1% measurement outliers but flag if more
    )
    # ── valueuom must be mg/dL for creatinine rows ─────────────────────────
    validator.expect_column_values_to_be_in_set(
        column="valueuom",
        value_set=["mg/dL"],
    )


def _add_urine_expectations(validator):
    """Expectations applied to urine output (outputevents) DataFrames."""
    validator.expect_column_values_to_be_greater_than_or_equal_to(
        column="valuenum",
        min_value=0,
    )


def _add_common_expectations(validator):
    """Shared expectations for both source types — 100% null tolerance."""
    # ── v1.2.0: zero null tolerance ────────────────────────────────────────
    validator.expect_column_values_to_not_be_null(
        column="subject_id",
        mostly=1.0,         # 100%: zero nulls allowed
    )
    validator.expect_column_values_to_not_be_null(
        column="charttime",
        mostly=1.0,         # 100%: zero nulls allowed
    )
    # Structural expectations
    validator.expect_column_to_exist(column="subject_id")
    validator.expect_column_to_exist(column="charttime")
    validator.expect_column_to_exist(column="valuenum")


# ---------------------------------------------------------------------------
# Main checkpoint runner
# ---------------------------------------------------------------------------

def run_checkpoint_1(
    df: pd.DataFrame,
    source_type: str = "creatinine",
    save_report: bool = True,
) -> dict:
    """
    Validate raw ingested data before writing to Snowflake BRONZE.

    Args:
        df:          Raw DataFrame (labevents or outputevents slice).
        source_type: 'creatinine' | 'urine' — controls which expectations run.
        save_report: If True, save an HTML report to gx_reports/.

    Returns:
        {
            "passed":       bool,
            "success_rate": float,          # 0.0 – 1.0
            "failed_expectations": list,
            "report_path":  str | None,
        }

    Raises:
        ValueError if the DataFrame is empty or missing required columns.
    """
    if df.empty:
        raise ValueError("GX Checkpoint 1: received an empty DataFrame — aborting.")

    required_cols = {"subject_id", "charttime", "valuenum"}
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"GX Checkpoint 1: missing required columns: {missing}")

    # ── Build in-memory GX context (no persistent store needed) ───────────
    context = gx.get_context()
    ds = context.sources.add_or_update_pandas(name="module1_runtime")
    asset = ds.add_dataframe_asset(name=f"raw_{source_type}")
    batch_request = asset.build_batch_request(dataframe=df)

    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=f"aki_raw_{source_type}_suite_v1_2_0",
    )

    # ── Apply expectations ─────────────────────────────────────────────────
    _add_common_expectations(validator)
    if source_type == "creatinine":
        _add_creatinine_expectations(validator)
    elif source_type == "urine":
        _add_urine_expectations(validator)
    else:
        raise ValueError(f"Unknown source_type: {source_type!r}")

    # ── Run ────────────────────────────────────────────────────────────────
    results = validator.validate()

    passed = results["success"]
    stats = results["statistics"]
    total = stats.get("evaluated_expectations", 1)
    successful = stats.get("successful_expectations", 0)
    success_rate = successful / total if total else 0.0

    failed = [
        {
            "expectation": r["expectation_config"]["expectation_type"],
            "column": r["expectation_config"]["kwargs"].get("column", "N/A"),
            "result": r.get("result", {}),
        }
        for r in results["results"]
        if not r["success"]
    ]

    # ── HTML report (for PR evidence) ─────────────────────────────────────
    report_path = None
    if save_report:
        ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        report_path = GX_REPORTS_DIR / f"ge_checkpoint_1_{source_type}_{ts}.html"
        # Build minimal HTML report manually (GX fluent API HTML export)
        _save_html_report(results, report_path, source_type)

    # ── Logging summary ───────────────────────────────────────────────────
    status = "✅ PASS" if passed else "❌ FAIL"
    log.info(
        "GX Checkpoint #1 [%s] %s | %d/%d expectations passed (%.1f%%)",
        source_type, status, successful, total, success_rate * 100,
    )
    if failed:
        for f in failed:
            log.warning("  FAILED: %s on column '%s'", f["expectation"], f["column"])

    return {
        "passed": passed,
        "success_rate": success_rate,
        "failed_expectations": failed,
        "report_path": str(report_path) if report_path else None,
        "statistics": stats,
    }


# ---------------------------------------------------------------------------
# HTML report writer
# ---------------------------------------------------------------------------

def _save_html_report(results: dict, path: Path, source_type: str):
    """Persist a minimal but readable HTML validation report."""
    ts = datetime.utcnow().isoformat() + "Z"
    overall = "PASS ✅" if results["success"] else "FAIL ❌"
    stats = results["statistics"]

    rows_html = ""
    for r in results["results"]:
        exp_type = r["expectation_config"]["expectation_type"]
        col = r["expectation_config"]["kwargs"].get("column", "—")
        status = "✅" if r["success"] else "❌"
        rows_html += f"<tr><td>{status}</td><td>{exp_type}</td><td>{col}</td></tr>\n"

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>GX Checkpoint #1 — {source_type} — {ts}</title>
<style>
  body {{ font-family: Arial, sans-serif; margin: 40px; }}
  h1 {{ color: #1565C0; }}
  table {{ border-collapse: collapse; width: 100%; }}
  th, td {{ border: 1px solid #ccc; padding: 8px 12px; text-align: left; }}
  th {{ background: #E3F2FD; }}
  .pass {{ color: #2E7D32; font-weight: bold; }}
  .fail {{ color: #C62828; font-weight: bold; }}
</style>
</head>
<body>
<h1>AKI Prediction System — GX Quality Gate #1</h1>
<p><strong>Version:</strong> 1.2.0 &nbsp;|&nbsp;
   <strong>Source:</strong> MIMIC-IV ONLY &nbsp;|&nbsp;
   <strong>Storage:</strong> Snowflake ONLY &nbsp;|&nbsp;
   <strong>Run:</strong> {ts}</p>
<h2>Source type: <code>{source_type}</code></h2>
<p class="{'pass' if results['success'] else 'fail'}">Overall: {overall}</p>
<ul>
  <li>Evaluated: {stats.get('evaluated_expectations', '—')}</li>
  <li>Successful: {stats.get('successful_expectations', '—')}</li>
  <li>Unsuccessful: {stats.get('unsuccessful_expectations', '—')}</li>
</ul>
<h3>Expectation Details</h3>
<table>
<tr><th>Status</th><th>Expectation</th><th>Column</th></tr>
{rows_html}
</table>
<hr>
<p><em>Creatinine range enforced: 0.3–15.0 mg/dL (v1.2.0).
subject_id + charttime: 100% not-null (zero tolerance).</em></p>
</body>
</html>"""

    path.write_text(html, encoding="utf-8")
    log.info("GX HTML report saved: %s", path)


# ---------------------------------------------------------------------------
# CLI shim
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO)
    # Quick smoke-test with synthetic data
    sample = pd.DataFrame({
        "subject_id": [10001, 10002, 10003],
        "charttime": pd.to_datetime(["2150-01-01 08:00", "2150-01-01 09:00", "2150-01-01 10:00"]),
        "valuenum": [1.2, 0.8, 3.5],
        "valueuom": ["mg/dL", "mg/dL", "mg/dL"],
        "itemid": [50912, 50912, 51081],
    })
    result = run_checkpoint_1(sample, source_type="creatinine")
    print(json.dumps(result, indent=2, default=str))
    sys.exit(0 if result["passed"] else 1)

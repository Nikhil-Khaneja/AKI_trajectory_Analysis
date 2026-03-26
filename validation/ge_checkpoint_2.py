"""
Module 1 v1.2.0: Great Expectations Quality Gate #2 — Silver model output validation.
Owner: Nikhil

Gate position: AFTER dbt Silver models run, BEFORE Gold feature computation.
Validates normalized, deduplicated Silver layer DataFrames.
"""

import json
import logging
from datetime import datetime
from pathlib import Path

import pandas as pd
import great_expectations as gx

log = logging.getLogger(__name__)
GX_REPORTS_DIR = Path(__file__).resolve().parents[2] / "gx_reports"
GX_REPORTS_DIR.mkdir(exist_ok=True)

VALID_KDIGO_STAGES = [0, 1, 2, 3]


def run_checkpoint_2(
    df: pd.DataFrame,
    source_type: str = "creatinine",
    save_report: bool = True,
) -> dict:
    """
    Validate Silver model output before Gold feature computation.

    Expectations:
      - creatinine values in [0.3, 15.0] mg/dL
      - urine_output_ml >= 0
      - subject_id: 100% not-null
      - hour_bucket: 100% not-null

    Args:
        df:          Silver DataFrame (creatinine_events or urine_output_events).
        source_type: 'creatinine' | 'urine'
        save_report: Save HTML report to gx_reports/.

    Returns dict with keys: passed, success_rate, failed_expectations, report_path.
    """
    if df.empty:
        raise ValueError("GX Checkpoint 2: received an empty DataFrame.")

    context = gx.get_context()
    ds = context.sources.add_or_update_pandas(name="module1_silver_runtime")
    asset = ds.add_dataframe_asset(name=f"silver_{source_type}")
    batch_request = asset.build_batch_request(dataframe=df)

    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=f"aki_silver_{source_type}_suite_v1_2_0",
    )

    # ── Common Silver expectations ─────────────────────────────────────────
    validator.expect_column_values_to_not_be_null(column="subject_id", mostly=1.0)
    validator.expect_column_values_to_not_be_null(column="hour_bucket", mostly=1.0)

    # ── Source-specific ───────────────────────────────────────────────────
    if source_type == "creatinine":
        validator.expect_column_values_to_be_between(
            column="creatinine", min_value=0.3, max_value=15.0
        )
    elif source_type == "urine":
        validator.expect_column_values_to_be_greater_than_or_equal_to(
            column="urine_output_ml", min_value=0
        )
    else:
        raise ValueError(f"Unknown source_type: {source_type!r}")

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
        }
        for r in results["results"]
        if not r["success"]
    ]

    report_path = None
    if save_report:
        ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        report_path = GX_REPORTS_DIR / f"ge_checkpoint_2_{source_type}_{ts}.html"
        _save_html_report(results, report_path, source_type, gate=2)

    status = "PASS" if passed else "FAIL"
    log.info(
        "GX Checkpoint #2 [%s] %s | %d/%d expectations passed (%.1f%%)",
        source_type, status, successful, total, success_rate * 100,
    )

    return {
        "passed": passed,
        "success_rate": success_rate,
        "failed_expectations": failed,
        "report_path": str(report_path) if report_path else None,
        "statistics": stats,
    }


def _save_html_report(results: dict, path: Path, source_type: str, gate: int = 2):
    ts = datetime.utcnow().isoformat() + "Z"
    overall = "PASS" if results["success"] else "FAIL"
    stats = results["statistics"]
    rows_html = "".join(
        f"<tr><td>{'OK' if r['success'] else 'FAIL'}</td>"
        f"<td>{r['expectation_config']['expectation_type']}</td>"
        f"<td>{r['expectation_config']['kwargs'].get('column','—')}</td></tr>\n"
        for r in results["results"]
    )
    html = f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="UTF-8">
<title>GX Checkpoint #{gate} — Silver {source_type} — {ts}</title>
<style>body{{font-family:Arial,sans-serif;margin:40px}}
table{{border-collapse:collapse;width:100%}}
th,td{{border:1px solid #ccc;padding:8px 12px;text-align:left}}
th{{background:#E3F2FD}}</style></head>
<body>
<h1>AKI v1.2.0 — GX Quality Gate #{gate} (Silver)</h1>
<p><strong>Source:</strong> MIMIC-IV ONLY | <strong>Run:</strong> {ts}</p>
<p><strong>Source type:</strong> {source_type} | <strong>Overall:</strong> {overall}</p>
<ul>
  <li>Evaluated: {stats.get('evaluated_expectations','—')}</li>
  <li>Successful: {stats.get('successful_expectations','—')}</li>
  <li>Unsuccessful: {stats.get('unsuccessful_expectations','—')}</li>
</ul>
<table><tr><th>Status</th><th>Expectation</th><th>Column</th></tr>
{rows_html}
</table>
</body></html>"""
    path.write_text(html, encoding="utf-8")
    log.info("GX HTML report saved: %s", path)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    sample = pd.DataFrame({
        "subject_id": [10001, 10002],
        "hour_bucket": pd.to_datetime(["2150-01-01 08:00", "2150-01-01 09:00"]),
        "creatinine": [1.2, 2.4],
    })
    result = run_checkpoint_2(sample, source_type="creatinine")
    print(json.dumps(result, indent=2, default=str))

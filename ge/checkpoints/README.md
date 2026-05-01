# GE Checkpoints Placeholder

Planned checkpoints:

1. `ingestion_gate` (before Bronze)
   - validates raw source CSVs
   - routes failed rows to quarantine outputs

2. `output_gate` (after model/stream scoring)
   - validates risk score range, stage bounds, and output keys

Until full GE bootstrap is wired, run:
- `python3 scripts/run_quality_gate.py`

# GE Expectations Placeholder

This folder will hold full Great Expectations suites once the GE context
is initialized in this repository.

Current executable quality gate is implemented in:
- `scripts/run_quality_gate.py`

That script enforces:
- required key columns
- ICU/admission time ordering
- curated creatinine unit/range checks
- curated urine non-negative/range checks
- pass/fail dataset materialization for Bronze ingestion and quarantine

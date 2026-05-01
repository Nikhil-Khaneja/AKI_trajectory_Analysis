# KDIGO Readiness Report

## Required Column Coverage
- `patients`: OK
- `admissions`: OK
- `icustays`: OK
- `labevents`: OK
- `d_labitems`: OK
- `outputevents`: OK
- `d_items`: OK

## Dictionary Discovery
- creatinine-like itemids in `d_labitems`: 20
- urine-like candidate itemids in `d_items`: 13

## Curated KDIGO Inputs
- curated serum creatinine itemids: ['50912', '51081', '52024', '52546']
- curated urine output itemids: ['226559', '226560', '226561', '226567', '226627', '226631', '227489']

## Coverage Metrics
- curated serum creatinine rows (mg/dL): 4336255
- curated urine rows: 4129979
- creatinine hadm overlap with icu hadm: 84186 / 415942
- urine stay overlap with icu stay: 90059 / 90059

## Missingness in KDIGO-Critical Fields
- creatinine nulls: {'hadm_id': 1743005, 'valuenum': 1640}
- urine nulls: {}

## Unit Distributions
- creatinine units: {'mg/dL': 4336255}
- urine units: {'mL': 4129979}

## Notes
- Keep KDIGO logic restricted to curated itemids and ICU-time windows.
- Add weight source later if strict urine mL/kg/hr rules are required.

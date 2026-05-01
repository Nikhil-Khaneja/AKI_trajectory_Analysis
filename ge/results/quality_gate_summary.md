# Quality Gate Summary

## patients
- source: `/Users/prags/Desktop/AKI/DATA/Hospital Data/patients.csv`
- processed: 364627
- passed: 364627
- failed: 0
- passed_pct: 100.0%

## admissions
- source: `/Users/prags/Desktop/AKI/DATA/Hospital Data/admissions-2.csv`
- processed: 546028
- passed: 545848
- failed: 180
- passed_pct: 99.96%
- failure_reasons:
  - bad_time_order:admittime>=dischtime: 180

## labevents
- source: `/Users/prags/Desktop/AKI/DATA/Hospital Data/labevents.csv`
- processed: 158374764
- passed: 156630033
- failed: 1744731
- passed_pct: 98.89%
- failure_reasons:
  - missing_required:hadm_id: 1742616
  - missing_required:valuenum: 1251
  - out_of_range:creatinine_mg/dL: 474
  - missing_required:hadm_id,valuenum: 389
  - missing_required:hadm_id,valueuom: 1

## d_labitems
- source: `/Users/prags/Desktop/AKI/DATA/Hospital Data/d_labitems.csv`
- processed: 1650
- passed: 1646
- failed: 4
- passed_pct: 99.75%
- failure_reasons:
  - missing_required:label: 4

## icustays
- source: `/Users/prags/Desktop/AKI/DATA/ICU DATA/icustays.csv`
- processed: 94458
- passed: 94444
- failed: 14
- passed_pct: 99.98%
- failure_reasons:
  - missing_required:outtime: 14

## outputevents
- source: `/Users/prags/Desktop/AKI/DATA/ICU DATA/outputevents.csv`
- processed: 5359395
- passed: 5359366
- failed: 29
- passed_pct: 99.99%
- failure_reasons:
  - out_of_range:urine_negative: 24
  - out_of_range:urine_too_large: 5

## d_items
- source: `/Users/prags/Desktop/AKI/DATA/ICU DATA/d_items.csv`
- processed: 4095
- passed: 4095
- failed: 0
- passed_pct: 100.0%


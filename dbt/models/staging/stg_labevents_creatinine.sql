-- Module 1 v1.2.0: Staging — creatinine labs from BRONZE.RAW_LABEVENTS
-- Owner: Nikhil
-- v1.2.0 BREAKING CHANGE: valuenum range updated from 0.1–20.0 → 0.3–15.0 mg/dL

SELECT
    subject_id,
    hadm_id,
    itemid,
    charttime,
    valuenum                          AS creatinine_mg_dl,
    valueuom,
    storetime,
    labevent_id
FROM {{ source('bronze', 'raw_labevents') }}
WHERE
    -- v1.2.0 creatinine item IDs (MIMIC-IV)
    itemid IN (50912, 51081, 52024, 52546)
    -- v1.2.0: range 0.3–15.0 mg/dL (was 0.1–20.0)
    AND valuenum BETWEEN 0.3 AND 15.0
    AND valueuom = 'mg/dL'
    -- Enforce 100% ID + timestamp coverage (v1.2.0 zero-null policy)
    AND subject_id IS NOT NULL
    AND charttime IS NOT NULL

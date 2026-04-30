-- Module 1 v1.2.0: Staging — urine output events from BRONZE.RAW_OUTPUTEVENTS
-- Owner: Nikhil
-- Dataset: MIMIC-IV ONLY (no eICU — v1.2.0)

SELECT
    subject_id,
    hadm_id,
    stay_id,
    itemid,
    charttime,
    storetime,
    value                             AS urine_output_ml,
    valueuom
FROM {{ source('bronze', 'raw_outputevents') }}
WHERE
    -- MIMIC-IV urine output item IDs
    itemid IN (226559, 226560, 226561, 226567, 226627, 226631, 227489)
    -- Urine output must be non-negative (GX Gate #1 enforced)
    AND value >= 0
    -- v1.2.0 zero-null policy
    AND subject_id IS NOT NULL
    AND charttime IS NOT NULL

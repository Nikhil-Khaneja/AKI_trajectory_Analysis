-- dbt_aki/models/silver/slv_patient_features.sql
-- This model calculates rolling creatinine and urine output

WITH raw_events AS (
    SELECT * FROM {{ ref('brz_clinical_events') }}
),

engineered AS (
    SELECT 
        patientunitstayid,
        hour_bucket,
        creatinine,
        -- Calculate creatinine ratio over baseline
        creatinine / MIN(creatinine) OVER (PARTITION BY patientunitstayid) as creatinine_ratio,
        -- Calculate rolling urine output (6h)
        SUM(urine_output_ml) OVER (
            PARTITION BY patientunitstayid 
            ORDER BY hour_bucket 
            ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
        ) as urine_ml_6h
    FROM raw_events
)

SELECT * FROM engineered

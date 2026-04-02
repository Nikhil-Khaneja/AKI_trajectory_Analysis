-- Module 1 v1.2.0: Silver — deduplicated, normalized creatinine events per patient-hour
-- Owner: Nikhil
-- Dedup strategy: keep earliest reading per subject_id + hour_bucket (ROW_NUMBER)
-- Bloom Filter pre-check is applied upstream (silver_bloom_filter.py) before INSERT.

{{ config(
    materialized = 'incremental',
    unique_key   = ['subject_id', 'hour_bucket'],
    on_schema_change = 'sync_all_columns'
) }}

WITH source AS (
    SELECT * FROM {{ ref('stg_labevents_creatinine') }}
),

deduped AS (
    SELECT
        subject_id,
        hadm_id,
        itemid,
        charttime,
        creatinine_mg_dl,
        valueuom,
        -- Assign row rank within each patient-hour bucket
        ROW_NUMBER() OVER (
            PARTITION BY subject_id, DATE_TRUNC('hour', charttime)
            ORDER BY charttime ASC
        ) AS rn
    FROM source
)

SELECT
    subject_id,
    hadm_id,
    itemid,
    charttime,
    DATE_TRUNC('hour', charttime)     AS hour_bucket,
    -- Normalized to mg/dL (already enforced in staging filter)
    creatinine_mg_dl                  AS creatinine,
    valueuom,
    -- Lineage metadata
    CURRENT_TIMESTAMP()               AS _silver_loaded_at,
    '1.2.0'                           AS _pipeline_version
FROM deduped
WHERE rn = 1

{% if is_incremental() %}
    -- Only process new rows since last run (incremental mode)
    AND charttime > (SELECT MAX(charttime) FROM {{ this }})
{% endif %}

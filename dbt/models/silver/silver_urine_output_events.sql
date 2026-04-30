-- Module 1 v1.2.0: Silver — urine output aggregated per patient-hour
-- Owner: Nikhil
-- Aggregates raw output events into mL per patient-hour for KDIGO UO threshold checks.

{{ config(
    materialized = 'incremental',
    unique_key   = ['subject_id', 'stay_id', 'hour_bucket'],
    on_schema_change = 'sync_all_columns'
) }}

WITH source AS (
    SELECT * FROM {{ ref('stg_outputevents_urine') }}
),

hourly AS (
    SELECT
        subject_id,
        hadm_id,
        stay_id,
        DATE_TRUNC('hour', charttime)       AS hour_bucket,
        -- Sum all urine output events within the hour
        SUM(urine_output_ml)                AS urine_output_ml,
        COUNT(*)                            AS event_count,
        MIN(charttime)                      AS first_event_ts,
        MAX(charttime)                      AS last_event_ts
    FROM source
    GROUP BY
        subject_id,
        hadm_id,
        stay_id,
        DATE_TRUNC('hour', charttime)
)

SELECT
    subject_id,
    hadm_id,
    stay_id,
    hour_bucket,
    urine_output_ml,
    event_count,
    first_event_ts,
    last_event_ts,
    CURRENT_TIMESTAMP()                     AS _silver_loaded_at,
    '1.2.0'                                 AS _pipeline_version
FROM hourly
WHERE urine_output_ml >= 0

{% if is_incremental() %}
    AND hour_bucket > (SELECT MAX(hour_bucket) FROM {{ this }})
{% endif %}

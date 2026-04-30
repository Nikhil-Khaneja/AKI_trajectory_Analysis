-- Module 3 v1.2.0: MART — MIMIC-IV training dataset (hourly)
-- Owner: Charvee
--
-- Output export name for handoff: mimic_iv_training_set.csv

WITH base AS (
    SELECT
        f.subject_id,
        f.hour_bucket,
        f.creatinine,
        f.min_cr_48h,
        f.creatinine_ratio,
        f.cr_delta_1h,
        f.cr_delta_12h,
        f.cr_delta_48h,
        f.urine_output_velocity_6h,
        f.urine_output_velocity_12h,
        f.urine_output_velocity_24h,
        k.kdigo_stage
    FROM {{ ref('gold_features_hourly') }} AS f
    INNER JOIN {{ ref('gold_kdigo_hourly') }} AS k
        ON f.subject_id = k.subject_id
       AND f.hour_bucket = k.hour_bucket
),

labeled AS (
    SELECT
        *,
        CASE
            WHEN MAX(kdigo_stage) OVER (
                PARTITION BY subject_id
                ORDER BY hour_bucket
                ROWS BETWEEN CURRENT ROW AND 48 FOLLOWING
            ) >= 3 THEN 1 ELSE 0
        END AS target_progress_to_stage3_48h
    FROM base
)

SELECT
    *,
    CURRENT_TIMESTAMP() AS _mart_loaded_at,
    '{{ var("pipeline_version") }}' AS _pipeline_version
FROM labeled

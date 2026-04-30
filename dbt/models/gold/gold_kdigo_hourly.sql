-- Module 3 v1.2.0: Gold — KDIGO stages (hourly)
-- Owner: Charvee

SELECT
    subject_id,
    hour_bucket,
    creatinine,
    min_cr_48h,
    creatinine_ratio,
    cr_delta_1h,
    cr_delta_12h,
    cr_delta_48h,
    urine_ml,
    urine_output_velocity_6h,
    urine_output_velocity_12h,
    urine_output_velocity_24h,
    CASE
        WHEN creatinine_ratio >= 3.0 OR creatinine >= 4.0 OR urine_output_velocity_24h < 0.3 THEN 3
        WHEN creatinine_ratio BETWEEN 2.0 AND 2.99 OR urine_output_velocity_12h < 0.5 THEN 2
        WHEN cr_delta_48h >= 0.3 OR creatinine_ratio BETWEEN 1.5 AND 1.99 OR urine_output_velocity_6h < 0.5 THEN 1
        ELSE 0
    END AS kdigo_stage,
    CURRENT_TIMESTAMP() AS _gold_loaded_at,
    '{{ var("pipeline_version") }}' AS _pipeline_version
FROM {{ ref('gold_features_hourly') }}

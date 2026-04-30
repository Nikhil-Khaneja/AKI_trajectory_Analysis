-- Module 3 v1.2.0: Gold — hourly engineered features (MIMIC-IV ONLY)
-- Owner: Charvee

WITH cr_with_min AS (
    SELECT
        subject_id,
        hour_bucket,
        creatinine,
        MIN(creatinine) OVER (
            PARTITION BY subject_id
            ORDER BY hour_bucket
            ROWS BETWEEN 47 PRECEDING AND CURRENT ROW
        ) AS min_cr_48h
    FROM {{ ref('silver_creatinine_events') }}
),

cr_features AS (
    SELECT
        subject_id,
        hour_bucket,
        creatinine,
        min_cr_48h,
        creatinine / NULLIF(min_cr_48h, 0) AS creatinine_ratio,
        creatinine - LAG(creatinine, 1) OVER (
            PARTITION BY subject_id
            ORDER BY hour_bucket
        ) AS cr_delta_1h,
        creatinine - LAG(creatinine, 12) OVER (
            PARTITION BY subject_id
            ORDER BY hour_bucket
        ) AS cr_delta_12h,
        creatinine - LAG(creatinine, 48) OVER (
            PARTITION BY subject_id
            ORDER BY hour_bucket
        ) AS cr_delta_48h
    FROM cr_with_min
),

urine_hourly AS (
    SELECT
        subject_id,
        hour_bucket,
        SUM(urine_output_ml) AS urine_ml
    FROM {{ ref('silver_urine_output_events') }}
    GROUP BY subject_id, hour_bucket
),

features AS (
    SELECT
        c.subject_id,
        c.hour_bucket,
        c.creatinine,
        c.min_cr_48h,
        c.creatinine_ratio,
        c.cr_delta_1h,
        c.cr_delta_12h,
        c.cr_delta_48h,
        u.urine_ml,
        AVG(u.urine_ml) OVER (
            PARTITION BY c.subject_id
            ORDER BY c.hour_bucket
            ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
        ) / (70.0 * 6.0) AS urine_output_velocity_6h,
        AVG(u.urine_ml) OVER (
            PARTITION BY c.subject_id
            ORDER BY c.hour_bucket
            ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
        ) / (70.0 * 12.0) AS urine_output_velocity_12h,
        AVG(u.urine_ml) OVER (
            PARTITION BY c.subject_id
            ORDER BY c.hour_bucket
            ROWS BETWEEN 23 PRECEDING AND CURRENT ROW
        ) / (70.0 * 24.0) AS urine_output_velocity_24h,
        CURRENT_TIMESTAMP() AS _gold_loaded_at,
        '{{ var("pipeline_version") }}' AS _pipeline_version
    FROM cr_features AS c
    LEFT JOIN urine_hourly AS u
        ON c.subject_id = u.subject_id
       AND c.hour_bucket = u.hour_bucket
)

SELECT * FROM features

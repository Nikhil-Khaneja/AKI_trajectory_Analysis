-- Module 1 v1.2.0: Silver — ICU cohort with stay windows + demographics
-- Owner: Nikhil
-- Joins icustays + admissions + patients into a single cohort table.
-- Used as the reference dimension for all per-patient feature windows.

{{ config(
    materialized = 'table',
    unique_key   = 'stay_id'
) }}

WITH stays AS (
    SELECT * FROM {{ ref('stg_icustays') }}
),

admissions AS (
    SELECT
        hadm_id,
        subject_id,
        admittime,
        dischtime,
        admission_type,
        insurance,
        language,
        marital_status,
        race
    FROM {{ source('bronze', 'raw_admissions') }}
    WHERE subject_id IS NOT NULL
      AND hadm_id   IS NOT NULL
),

patients AS (
    SELECT
        subject_id,
        gender,
        anchor_age,
        anchor_year,
        dod
    FROM {{ source('bronze', 'raw_patients') }}
    WHERE subject_id IS NOT NULL
),

cohort AS (
    SELECT
        s.subject_id,
        s.hadm_id,
        s.stay_id,
        s.first_careunit,
        s.last_careunit,
        s.intime                                    AS icu_intime,
        s.outtime                                   AS icu_outtime,
        s.los_days,
        s.los_hours,
        -- Hours elapsed from hospital admission to ICU admission
        DATEDIFF(
            'hour', a.admittime, s.intime
        )                                           AS hours_admit_to_icu,
        a.admittime,
        a.dischtime,
        a.admission_type,
        a.insurance,
        a.race,
        p.gender,
        p.anchor_age                                AS age,
        -- Age group for fairness audit (Module 4)
        CASE
            WHEN p.anchor_age < 65 THEN '<65'
            ELSE '>=65'
        END                                         AS age_group,
        p.dod                                       AS date_of_death,
        -- 30-day in-hospital mortality flag
        CASE
            WHEN p.dod IS NOT NULL
             AND p.dod <= DATEADD('day', 30, a.admittime)
            THEN 1 ELSE 0
        END                                         AS mortality_30d,
        CURRENT_TIMESTAMP()                         AS _silver_loaded_at,
        '1.2.0'                                     AS _pipeline_version
    FROM stays AS s
    LEFT JOIN admissions AS a
        ON s.hadm_id = a.hadm_id
    LEFT JOIN patients AS p
        ON s.subject_id = p.subject_id
)

SELECT * FROM cohort

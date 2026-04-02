-- Module 1 v1.2.0: Staging — ICU stay windows from BRONZE.RAW_ICUSTAYS
-- Owner: Nikhil
-- Provides the time boundaries for all per-patient KDIGO windows.

SELECT
    icu.subject_id,
    icu.hadm_id,
    icu.stay_id,
    icu.first_careunit,
    icu.last_careunit,
    icu.intime,
    icu.outtime,
    icu.los                               AS los_days,
    -- Convert LOS to hours for downstream KDIGO window joins
    ROUND(icu.los * 24, 2)               AS los_hours,
    -- Derived: ICU admission date only (for partition pruning)
    DATE(icu.intime)                     AS icu_admit_date
FROM {{ source('bronze', 'raw_icustays') }} AS icu
WHERE
    icu.subject_id IS NOT NULL
    AND icu.stay_id   IS NOT NULL
    AND icu.intime    IS NOT NULL
    -- Only include stays with a valid duration (> 0 hours)
    AND icu.los > 0

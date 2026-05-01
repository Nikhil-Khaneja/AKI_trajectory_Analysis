-- Canonical ICU cohort for downstream joins.
select
    subject_id,
    hadm_id,
    stay_id,
    first_careunit,
    last_careunit,
    intime,
    outtime,
    los
from {{ ref('stg_icustays') }}

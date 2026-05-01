-- ICU-window filtered urine output events.
with cohort as (
    select *
    from {{ ref('silver_icu_cohort') }}
),
urine as (
    select *
    from {{ ref('stg_outputevents_urine') }}
)
select
    c.subject_id,
    c.hadm_id,
    c.stay_id,
    u.charttime,
    u.urine_ml
from cohort c
join urine u
  on c.stay_id = u.stay_id
 and u.charttime between c.intime and c.outtime

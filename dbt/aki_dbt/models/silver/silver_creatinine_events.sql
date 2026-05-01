-- ICU-window filtered serum creatinine events.
with cohort as (
    select *
    from {{ ref('silver_icu_cohort') }}
),
creat as (
    select *
    from {{ ref('stg_labevents_creatinine') }}
)
select
    c.subject_id,
    c.hadm_id,
    c.stay_id,
    e.charttime,
    e.creatinine_mg_dl
from cohort c
join creat e
  on c.subject_id = e.subject_id
 and c.hadm_id = e.hadm_id
 and e.charttime between c.intime and c.outtime

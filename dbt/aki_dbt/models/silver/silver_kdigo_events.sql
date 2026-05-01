-- Restrict curated events to ICU windows to avoid non-ICU contamination.
with icu as (
    select
        subject_id,
        hadm_id,
        stay_id,
        intime,
        outtime
    from {{ ref('silver_icu_cohort') }}
),
creat as (
    select *
    from {{ ref('stg_labevents_creatinine') }}
),
urine as (
    select *
    from {{ ref('stg_outputevents_urine') }}
)
select
    i.subject_id,
    i.hadm_id,
    i.stay_id,
    c.charttime,
    c.creatinine_mg_dl,
    cast(null as double) as urine_ml,
    'creatinine' as event_type
from icu i
join creat c
  on i.subject_id = c.subject_id
 and i.hadm_id = c.hadm_id
 and c.charttime between i.intime and i.outtime

union all

select
    i.subject_id,
    i.hadm_id,
    i.stay_id,
    u.charttime,
    cast(null as double) as creatinine_mg_dl,
    u.urine_ml,
    'urine' as event_type
from icu i
join urine u
  on i.stay_id = u.stay_id
 and u.charttime between i.intime and i.outtime

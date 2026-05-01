with baseline as (
    select
        stay_id,
        min(creatinine_mg_dl) as baseline_creatinine
    from {{ ref('silver_creatinine_events') }}
    group by stay_id
),
creat_hourly as (
    select
        c.subject_id,
        c.hadm_id,
        c.stay_id,
        date_trunc('hour', c.charttime) as chart_hour,
        max(c.creatinine_mg_dl) as creatinine_mg_dl,
        b.baseline_creatinine
    from {{ ref('silver_creatinine_events') }} c
    join baseline b
      on c.stay_id = b.stay_id
    group by 1,2,3,4,6
),
creat_stage as (
    select
        *,
        case
            when baseline_creatinine is null or baseline_creatinine = 0 then 0
            when (creatinine_mg_dl / baseline_creatinine) >= 3.0 or creatinine_mg_dl >= 4.0 then 3
            when (creatinine_mg_dl / baseline_creatinine) >= 2.0 then 2
            when (creatinine_mg_dl / baseline_creatinine) >= 1.5 then 1
            else 0
        end as kdigo_creat_stage
    from creat_hourly
),
urine_hourly as (
    select
        subject_id,
        hadm_id,
        stay_id,
        date_trunc('hour', charttime) as chart_hour,
        sum(urine_ml) as urine_ml
    from {{ ref('silver_urine_output_events') }}
    group by 1,2,3,4
),
urine_stage as (
    select
        *,
        case
            when urine_ml < 30 then 2
            when urine_ml < 60 then 1
            else 0
        end as kdigo_urine_stage
    from urine_hourly
),
joined as (
    select
        coalesce(c.subject_id, u.subject_id) as subject_id,
        coalesce(c.hadm_id, u.hadm_id) as hadm_id,
        coalesce(c.stay_id, u.stay_id) as stay_id,
        coalesce(c.chart_hour, u.chart_hour) as charttime,
        c.creatinine_mg_dl,
        u.urine_ml,
        coalesce(c.kdigo_creat_stage, 0) as kdigo_creat_stage,
        coalesce(u.kdigo_urine_stage, 0) as kdigo_urine_stage
    from creat_stage c
    full outer join urine_stage u
      on c.stay_id = u.stay_id
     and c.chart_hour = u.chart_hour
)
select
    subject_id,
    hadm_id,
    stay_id,
    charttime,
    creatinine_mg_dl,
    urine_ml,
    kdigo_creat_stage,
    kdigo_urine_stage,
    greatest(kdigo_creat_stage, kdigo_urine_stage) as kdigo_stage,
    current_timestamp() as processed_ts
from joined

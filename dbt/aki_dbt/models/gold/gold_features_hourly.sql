with kdigo as (
    select *
    from {{ ref('gold_kdigo_hourly') }}
),
cohort as (
    select stay_id, intime
    from {{ ref('silver_icu_cohort') }}
),
final as (
    select
        k.subject_id,
        k.hadm_id,
        k.stay_id,
        k.charttime,
        k.creatinine_mg_dl,
        k.urine_ml,
        k.kdigo_stage,
        max(k.kdigo_stage) over (
            partition by k.stay_id
            order by k.charttime
            rows between unbounded preceding and current row
        ) as rolling_max_kdigo,
        datediff('hour', c.intime, k.charttime) as hours_since_admit
    from kdigo k
    left join cohort c
      on k.stay_id = c.stay_id
)
select * from final

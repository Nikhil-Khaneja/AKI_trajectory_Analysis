select
    subject_id,
    hadm_id,
    stay_id,
    charttime,
    creatinine_mg_dl,
    urine_ml,
    hours_since_admit,
    kdigo_stage,
    rolling_max_kdigo,
    case when rolling_max_kdigo >= 3 then 1 else 0 end as label_stage3_progression
from {{ ref('gold_features_hourly') }}
where creatinine_mg_dl is not null
  and urine_ml is not null

with typed as (
    select
        cast(subject_id as bigint) as subject_id,
        cast(hadm_id as bigint) as hadm_id,
        cast(stay_id as bigint) as stay_id,
        cast(first_careunit as string) as first_careunit,
        cast(last_careunit as string) as last_careunit,
        cast(intime as timestamp_ntz) as intime,
        cast(outtime as timestamp_ntz) as outtime,
        cast(los as double) as los
    from {{ source('bronze', 'icustays_raw') }}
)
select *
from typed
where subject_id is not null
  and hadm_id is not null
  and stay_id is not null
  and intime is not null
  and outtime is not null
  and intime < outtime

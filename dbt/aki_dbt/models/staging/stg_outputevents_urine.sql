-- Curated urine output events for KDIGO logic.
select
    try_to_number(subject_id)::bigint as subject_id,
    try_to_number(hadm_id)::bigint as hadm_id,
    try_to_number(stay_id)::bigint as stay_id,
    try_to_number(itemid)::bigint as itemid,
    try_to_timestamp_ntz(charttime) as charttime,
    try_to_double(value) as urine_ml
from {{ source('bronze', 'outputevents_raw') }}
where itemid in (226559, 226560, 226561, 226567, 226627, 226631, 227489)
  and valueuom = 'mL'
  and value is not null
  and try_to_double(value) is not null
  and try_to_double(value) >= 0
  and try_to_double(value) <= 10000

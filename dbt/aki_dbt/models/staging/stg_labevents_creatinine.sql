-- Curated serum creatinine events for KDIGO logic.
select
    try_to_number(subject_id)::bigint as subject_id,
    try_to_number(hadm_id)::bigint as hadm_id,
    try_to_number(itemid)::bigint as itemid,
    try_to_timestamp_ntz(charttime) as charttime,
    try_to_double(valuenum) as creatinine_mg_dl
from {{ source('bronze', 'labevents_raw') }}
where itemid in (50912, 51081, 52024, 52546)
  and valueuom = 'mg/dL'
  and valuenum is not null
  and try_to_double(valuenum) is not null
  and try_to_double(valuenum) between 0.1 and 20.0

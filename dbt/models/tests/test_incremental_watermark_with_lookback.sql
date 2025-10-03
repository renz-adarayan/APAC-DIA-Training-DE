-- models/test_incremental_watermark_with_lookback.sql

{{ config(materialized='table') }}

-- This test creates a scenario where we have new data and tests
-- if the incremental_watermark_with_lookback macro correctly filters based on existing data
with test_data as (
    select 1 as id, '2023-01-01'::timestamp as ts union all
    select 2 as id, '2023-01-03'::timestamp as ts union all
    select 3 as id, '2023-01-04'::timestamp as ts union all
    select 4 as id, '2023-01-05'::timestamp as ts union all
    select 5 as id, '2023-01-06'::timestamp as ts
)
select
    id,
    ts,
    'lookback_48_hours' as test_case
from test_data
{% if is_incremental() %}
-- This is where the macro would be used in a real incremental model
-- The macro will generate a WHERE clause like: ts >= (SELECT ... FROM this_table)
where {{ incremental_watermark_with_lookback('ts', 48) }}
{% endif %}

-- Expected behavior:
-- On first run (not incremental): All rows are inserted
-- On subsequent runs (incremental): Only rows with ts >= (max_existing_ts - 48 hours) are processed
-- This allows for late-arriving data within the 48-hour lookback window

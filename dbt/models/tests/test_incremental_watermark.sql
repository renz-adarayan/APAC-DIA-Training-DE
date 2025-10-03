-- models/test_incremental_watermark.sql

{{ config(materialized='table') }}

-- This test creates a scenario where we have new data and tests
-- if the incremental_watermark macro correctly filters based on existing data
with test_data as (
    select 1 as id, '2023-01-01'::timestamp as ts union all
    select 2 as id, '2023-01-03'::timestamp as ts union all
    select 3 as id, '2023-01-04'::timestamp as ts union all
    select 4 as id, '2023-01-05'::timestamp as ts
)
select
    id,
    ts,
    'test_scenario' as test_case
from test_data
{% if is_incremental() %}
-- This is where the macro would be used in a real incremental model
-- For testing purposes, we simulate having existing data with max ts = '2023-01-02'
where {{ incremental_watermark('ts') }}
{% endif %}

-- Expected result:
-- Only rows with ts > max existing ts (2023-01-02) should be included
-- id | ts                  | should_be_included
-- 1  | 2023-01-01 00:00:00 | 0
-- 2  | 2023-01-03 00:00:00 | 1
-- 3  | 2023-01-04 00:00:00 | 1

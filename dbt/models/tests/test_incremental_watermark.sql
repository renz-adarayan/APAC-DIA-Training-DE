-- models/test_incremental_watermark.sql

-- Mock existing table data (simulating {{ this }})
with existing_data as (
    select '2023-01-01'::timestamp as ts union all
    select '2023-01-02'::timestamp as ts
),

-- New incoming data
new_data as (
    select 1 as id, '2023-01-01'::timestamp as ts union all
    select 2 as id, '2023-01-03'::timestamp as ts union all
    select 3 as id, '2023-01-04'::timestamp as ts
),

-- Get max timestamp from existing data (simulating what the macro does)
max_existing_ts as (
    select coalesce(max(ts), '1970-01-01'::timestamp) as max_ts
    from existing_data
)

select
    id,
    ts,
    case when ts > (select max_ts from max_existing_ts) then 1 else 0 end as should_be_included
from new_data;

-- Expected result:
-- Only rows with ts > max existing ts (2023-01-02) should be included
-- id | ts                  | should_be_included
-- 1  | 2023-01-01 00:00:00 | 0
-- 2  | 2023-01-03 00:00:00 | 1
-- 3  | 2023-01-04 00:00:00 | 1

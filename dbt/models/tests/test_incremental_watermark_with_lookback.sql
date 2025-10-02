-- models/test_incremental_watermark_with_lookback.sql

-- Mock existing table data (simulating {{ this }})
with existing_data as (
    select '2023-01-01'::timestamp as ts union all
    select '2023-01-02'::timestamp as ts union all
    select '2023-01-05'::timestamp as ts
),

-- New incoming data
new_data as (
    select 1 as id, '2023-01-01'::timestamp as ts union all
    select 2 as id, '2023-01-03'::timestamp as ts union all
    select 3 as id, '2023-01-04'::timestamp as ts union all
    select 4 as id, '2023-01-05'::timestamp as ts union all
    select 5 as id, '2023-01-06'::timestamp as ts
),

-- Calculate lookback watermark (max ts - lookback_hours hours)
-- Using 48 hours (2 days) lookback
watermark as (
    select coalesce(
        dateadd('hour', -48, max(ts)),
        '1970-01-01'::timestamp
    ) as watermark_ts
    from existing_data
)

select
    id,
    ts,
    case when ts >= (select watermark_ts from watermark) then 1 else 0 end as should_be_included
from new_data;

-- Expected result:
-- Max existing ts is 2023-01-05, so watermark = 2023-01-05 - 48 hours = 2023-01-03
-- Rows with ts >= 2023-01-03 should be included
-- id | ts                  | should_be_included
-- 1  | 2023-01-01 00:00:00 | 0
-- 2  | 2023-01-03 00:00:00 | 1
-- 3  | 2023-01-04 00:00:00 | 1
-- 4  | 2023-01-05 00:00:00 | 1
-- 5  | 2023-01-06 00:00:00 | 1

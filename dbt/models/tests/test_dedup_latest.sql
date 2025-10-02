-- models/test_dedup_latest.sql

with test_data as (
    select 1 as id, 'A' as value, '2023-01-01' as ts union all
    select 1 as id, 'B' as value, '2023-01-02' as ts union all
    select 2 as id, 'C' as value, '2023-01-01' as ts
)
select
    id,
    value,
    ts
from test_data
where {{ dedup_latest(['id'], 'ts') }};

-- Expected result:
-- id | value | ts
-- 1  | B     | 2023-01-02
-- 2  | C     | 2023-01-01

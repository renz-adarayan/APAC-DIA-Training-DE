-- test_json_extract_text.sql
-- Test for json_extract_text macro

with test_data as (
    select '{"name": "Alice", "age": 30}' as json_col union all
    select '{"name": "Bob", "age": 25}' as json_col
)
select
    {{ json_extract_text('json_col', '$.name') }} as name,
    {{ json_extract_text('json_col', '$.age') }} as age
from test_data;

-- Expected result:
-- name  | age
-- Alice | 30
-- Bob   | 25

-- test_json_extract_text.sql
-- Test for json_extract_text macro

with test_data as (
    select '{"name": "Alice", "age": 30, "address": {"city": "NYC", "zip": "10001"}}' as json_col, 'complex_nested' as test_case union all
    select '{"name": "Bob", "age": 25}' as json_col, 'simple_object' as test_case union all
    select '{"users": [{"name": "Charlie"}, {"name": "David"}]}' as json_col, 'array_access' as test_case union all
    select '{}' as json_col, 'empty_object' as test_case union all
    select null as json_col, 'null_json' as test_case
)
select
    test_case,
    json_col,
    {{ json_extract_text('json_col', '$.name') }} as name,
    {{ json_extract_text('json_col', '$.age') }} as age,
    {{ json_extract_text('json_col', '$.address.city') }} as nested_city,
    {{ json_extract_text('json_col', '$.users[0].name') }} as first_user,
    {{ json_extract_text('json_col', '$.nonexistent') }} as missing_field
from test_data

-- Expected result:
-- name  | age
-- Alice | 30
-- Bob   | 25

-- models/tests/test_hash_columns.sql

-- Test 1: Verify hash consistency - same inputs produce same hashes
with consistency_test as (
    select
        'foo' as col1,
        'bar' as col2,
        {{ hash_columns(["'foo'", "'bar'"]) }} as hash_1,
        {{ hash_columns(["'foo'", "'bar'"]) }} as hash_2,
        case when {{ hash_columns(["'foo'", "'bar'"]) }} = {{ hash_columns(["'foo'", "'bar'"]) }} 
             then 'PASS' else 'FAIL' end as consistency_check
),

-- Test 2: Verify different inputs produce different hashes
different_inputs as (
    select
        {{ hash_columns(["'foo'", "'bar'"]) }} as hash_foobar,
        {{ hash_columns(["'bar'", "'foo'"]) }} as hash_barfoo,
        case when {{ hash_columns(["'foo'", "'bar'"]) }} != {{ hash_columns(["'bar'", "'foo'"]) }}
             then 'PASS' else 'FAIL' end as order_matters_check
),

-- Test 3: Single column vs array
single_vs_array as (
    select
        'test' as input_val,
        {{ hash_columns("'test'") }} as hash_single,
        {{ hash_columns(["'test'"]) }} as hash_array,
        case when {{ hash_columns("'test'") }} = {{ hash_columns(["'test'"]) }}
             then 'PASS' else 'FAIL' end as single_array_consistency
),

-- Test 4: Multiple columns
multi_column as (
    select
        'foo' as col1,
        'bar' as col2,
        'baz' as col3,
        {{ hash_columns(["'foo'", "'bar'", "'baz'"]) }} as hash_three_cols
),

-- Test 5: Null handling
null_handling as (
    select
        null as col1,
        {{ hash_columns("null") }} as hash_null,
        {{ hash_columns(["null", "'value'"]) }} as hash_null_with_value
)

select 'consistency_test' as test_name, hash_1 as hash_value, consistency_check as result from consistency_test
union all
select 'different_inputs' as test_name, hash_foobar as hash_value, order_matters_check as result from different_inputs
union all
select 'single_vs_array' as test_name, hash_single as hash_value, single_array_consistency as result from single_vs_array
union all
select 'multi_column' as test_name, hash_three_cols as hash_value, 'N/A' as result from multi_column
union all
select 'null_handling' as test_name, hash_null as hash_value, 'N/A' as result from null_handling

-- Expected results:
-- test_name          | hash_value                       | result
-- consistency_test   | (md5 hash of 'foo|bar')         | PASS
-- different_inputs   | (md5 hash of 'foo|bar')         | PASS (order matters: foo|bar != bar|foo)
-- single_vs_array    | (md5 hash of 'test')            | PASS (single column = array with 1 element)
-- multi_column       | (md5 hash of 'foo|bar|baz')     | N/A
-- null_handling      | (md5 hash of null)              | N/A

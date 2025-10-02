-- models/tests/test_hash_columns.sql

select
    'foo' as col1,
    'bar' as col2,
    {{ hash_columns(["'foo'", "'bar'"]) }} as hash_array,
    {{ hash_columns("'foo'") }} as hash_single,
    {{ hash_columns(["'foo'", "'bar'", "'baz'"]) }} as hash_three_cols
union all
select
    'baz' as col1,
    'qux' as col2,
    {{ hash_columns(["'baz'", "'qux'"]) }} as hash_array,
    {{ hash_columns("'baz'") }} as hash_single,
    {{ hash_columns(["'baz'", "'qux'", "'quux'"]) }} as hash_three_cols

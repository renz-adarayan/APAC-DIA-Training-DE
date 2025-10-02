-- models/test_safe_cast.sql

select
    '123' as input_val,
    {{ safe_cast("'123'", 'integer') }} as cast_to_int,
    {{ safe_cast("'123.45'", 'float') }} as cast_to_float,
    {{ safe_cast("'not_a_number'", 'integer') }} as cast_invalid_int,
    {{ safe_cast("null", 'integer') }} as cast_null_int
union all
select
    '456.78' as input_val,
    {{ safe_cast("'456.78'", 'float') }} as cast_to_float,
    {{ safe_cast("'456.78'", 'integer') }} as cast_to_int,
    {{ safe_cast("'abc'", 'float') }} as cast_invalid_float,
    {{ safe_cast("null", 'float') }} as cast_null_float

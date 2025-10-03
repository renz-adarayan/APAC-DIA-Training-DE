-- models/test_clean_string.sql

select
    '  Hello   World  ' as raw_string,
    {{ clean_string("'  Hello   World  '") }} as cleaned_default,
    {{ clean_string("'  Hello   World  '", lower=false) }} as cleaned_no_lower,
    {{ clean_string("'  Hello   World  '", squash_spaces=false) }} as cleaned_no_squash
union all
select
    '   ' as raw_string,
    {{ clean_string("'   '") }} as cleaned_default,
    {{ clean_string("'   '", lower=false) }} as cleaned_no_lower,
    {{ clean_string("'   '", squash_spaces=false) }} as cleaned_no_squash
union all
select
    '' as raw_string,
    {{ clean_string("''") }} as cleaned_default,
    {{ clean_string("''", lower=false) }} as cleaned_no_lower,
    {{ clean_string("''", squash_spaces=false) }} as cleaned_no_squash
union all
select
    'UPPERCASE' as raw_string,
    {{ clean_string("'UPPERCASE'") }} as cleaned_default,
    {{ clean_string("'UPPERCASE'", lower=false) }} as cleaned_no_lower,
    {{ clean_string("'UPPERCASE'", squash_spaces=false) }} as cleaned_no_squash
union all
select
    null as raw_string,
    {{ clean_string("null") }} as cleaned_default,
    {{ clean_string("null", lower=false) }} as cleaned_no_lower,
    {{ clean_string("null", squash_spaces=false) }} as cleaned_no_squash

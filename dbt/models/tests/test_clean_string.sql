-- models/test_clean_string.sql

select
    '  Hello   World  ' as raw_string,
    {{ clean_string("'  Hello   World  '") }} as cleaned_default,
    {{ clean_string("'  Hello   World  '", lower=false) }} as cleaned_no_lower,
    {{ clean_string("'  Hello   World  '", squash_spaces=false) }} as cleaned_no_squash

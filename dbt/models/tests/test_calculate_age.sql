-- models/tests/test_calculate_age.sql

select
    '2000-01-01' as birth_date,
    {{ calculate_age("'2000-01-01'", "'2025-10-02'") }} as age_as_of_2025_10_02,
    {{ calculate_age("'2000-01-01'") }} as age_as_of_now
union all
select
    '2010-10-02' as birth_date,
    {{ calculate_age("'2010-10-02'", "'2025-10-02'") }} as age_as_of_2025_10_02,
    {{ calculate_age("'2010-10-02'") }} as age_as_of_now
union all
select
    '2025-10-02' as birth_date,
    {{ calculate_age("'2025-10-02'", "'2025-10-02'") }} as age_zero,
    {{ calculate_age("'2025-10-02'") }} as age_zero_now

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
union all
select
    '2030-01-01' as birth_date,
    {{ calculate_age("'2030-01-01'", "'2025-10-02'") }} as age_future_birth,
    null as age_as_of_now
union all
select
    null as birth_date,
    {{ calculate_age("null", "'2025-10-02'") }} as age_null_birth,
    {{ calculate_age("null") }} as age_null_birth_now

-- Expected results:
-- birth_date | age_as_of_2025_10_02 | age_as_of_now
-- 2000-01-01 | 25                   | (current age)
-- 2010-10-02 | 15                   | (current age)
-- 2025-10-02 | 0                    | 0
-- 2030-01-01 | -5                   | null (negative age for future birth)
-- null       | null                 | null (handles null gracefully)

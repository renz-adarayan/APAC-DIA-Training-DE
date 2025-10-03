-- models/tests/test_normalize_timestamp.sql
select
    '2025-10-02 12:34:56' as input_ts,
    {{ normalize_timestamp("'2025-10-02 12:34:56'") }} as normalized_ts,
    'basic_timestamp' as test_case
union all
select
    '2025-03-15 08:00:00' as input_ts,
    {{ normalize_timestamp("'2025-03-15 08:00:00'") }} as normalized_ts,
    'morning_timestamp' as test_case
union all
select
    '2025-12-31 23:59:59' as input_ts,
    {{ normalize_timestamp("'2025-12-31 23:59:59'") }} as normalized_ts,
    'end_of_year' as test_case
union all
select
    '2025-01-01 00:00:00' as input_ts,
    {{ normalize_timestamp("'2025-01-01 00:00:00'") }} as normalized_ts,
    'start_of_year' as test_case
union all
select
    '2025-06-15T14:30:45' as input_ts,
    {{ normalize_timestamp("'2025-06-15T14:30:45'") }} as normalized_ts,
    'iso_format' as test_case
union all
select
    '2025-06-15 14:30:45.123' as input_ts,
    {{ normalize_timestamp("'2025-06-15 14:30:45.123'") }} as normalized_ts,
    'with_milliseconds' as test_case
union all
select
    null as input_ts,
    {{ normalize_timestamp("null") }} as normalized_ts,
    'null_input' as test_case

-- Expected results:
-- All timestamps should be cast to timestamp with UTC timezone
-- input_ts            | normalized_ts
-- 2025-10-02 12:34:56 | 2025-10-02 12:34:56+00
-- 2025-03-15 08:00:00 | 2025-03-15 08:00:00+00
-- 2025-12-31 23:59:59 | 2025-12-31 23:59:59+00 (boundary case)
-- 2025-01-01 00:00:00 | 2025-01-01 00:00:00+00 (boundary case)
-- null                | null (handles null gracefully)

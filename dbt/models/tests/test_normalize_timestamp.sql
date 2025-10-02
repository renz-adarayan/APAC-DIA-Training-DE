-- models/tests/test_normalize_timestamp.sql
select
    '2025-10-02 12:34:56' as input_ts,
    {{ normalize_timestamp("'2025-10-02 12:34:56'") }} as normalized_ts_1
union all
select
    '2025-03-15 08:00:00' as input_ts,
    {{ normalize_timestamp("'2025-03-15 08:00:00'") }} as normalized_ts_2

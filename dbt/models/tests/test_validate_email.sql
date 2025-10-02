-- models/tests/test_validate_email.sql

select
    'user@example.com' as email,
    {{ validate_email("'user@example.com'") }} as is_valid
union all
select
    'user.name+tag@sub.domain.co' as email,
    {{ validate_email("'user.name+tag@sub.domain.co'") }} as is_valid
union all
select
    'invalid-email' as email,
    {{ validate_email("'invalid-email'") }} as is_valid
union all
select
    'user@.com' as email,
    {{ validate_email("'user@.com'") }} as is_valid
union all
select
    'user@domain' as email,
    {{ validate_email("'user@domain'") }} as is_valid
union all
select
    'user@domain.c' as email,
    {{ validate_email("'user@domain.c'") }} as is_valid

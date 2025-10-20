{% macro get_holidays() %}
{#
    Returns a union of all holiday dates from seed files (AU, US, UK)
    Used by dim_date to mark holidays
#}
    select 
        holiday_date,
        holiday_name,
        country
    from {{ ref('au_holidays') }}
    
    union all
    
    select 
        holiday_date,
        holiday_name,
        country
    from {{ ref('us_holidays') }}
    
    union all
    
    select 
        holiday_date,
        holiday_name,
        country
    from {{ ref('uk_holidays') }}
{% endmacro %}

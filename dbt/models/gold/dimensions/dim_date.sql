{{
    config(
        materialized='table',
        schema='gold'
    )
}}

with date_spine as (
    {% set start_date = var('dim_date_start', '2014-01-01') %}
    {% set end_date = var('dim_date_end', '2025-12-31') %}
    
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('" ~ start_date ~ "' as date)",
        end_date="cast('" ~ end_date ~ "' as date)"
    ) }}
),

holidays as (
    {{ get_holidays() }}
),

date_dimension as (
    select
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['date_day']) }} as date_sk,
        
        -- Natural key
        date_day as date,
        
        -- Calendar attributes
        extract(year from date_day) as calendar_year,
        extract(month from date_day) as calendar_month,
        extract(day from date_day) as calendar_day,
        extract(quarter from date_day) as calendar_quarter,
        
        -- ISO week attributes
        extract(week from date_day) as iso_week_number,
        extract(year from date_day) as iso_year,
        
        -- Day attributes
        extract(dayofweek from date_day) as day_of_week,  -- 0=Sunday, 6=Saturday
        dayname(date_day) as day_name,
        extract(dayofyear from date_day) as day_of_year,
        
        -- Month attributes
        monthname(date_day) as month_name,
        date_trunc('month', date_day) as month_start_date,
        last_day(date_day) as month_end_date,
        
        -- Quarter attributes
        date_trunc('quarter', date_day) as quarter_start_date,
        last_day(date_trunc('quarter', date_day) + interval '2 months') as quarter_end_date,
        
        -- Year attributes
        date_trunc('year', date_day) as year_start_date,
        make_date(extract(year from date_day)::integer, 12, 31) as year_end_date,
        
        -- Fiscal year (Australian FY: July 1 - June 30)
        {{ get_fiscal_period('date_day', var('fiscal_year_start_month', 7), var('fiscal_year_start_day', 1)) }} as fiscal_year,
        
        -- Fiscal quarter (based on fiscal year)
        case 
            when extract(month from date_day) in (7, 8, 9) then 1
            when extract(month from date_day) in (10, 11, 12) then 2
            when extract(month from date_day) in (1, 2, 3) then 3
            when extract(month from date_day) in (4, 5, 6) then 4
        end as fiscal_quarter,
        
        -- Weekend and weekday flags
        case when extract(dayofweek from date_day) in (0, 6) then true else false end as is_weekend,
        case when extract(dayofweek from date_day) not in (0, 6) then true else false end as is_weekday,
        
        -- Holiday information (will be populated via left join)
        null::boolean as is_holiday,
        null::varchar as holiday_name,
        null::varchar as holiday_country
        
    from date_spine
),

final as (
    select
        d.date_sk,
        d.date,
        d.calendar_year,
        d.calendar_month,
        d.calendar_day,
        d.calendar_quarter,
        d.iso_week_number,
        d.iso_year,
        d.day_of_week,
        d.day_name,
        d.day_of_year,
        d.month_name,
        d.month_start_date,
        d.month_end_date,
        d.quarter_start_date,
        d.quarter_end_date,
        d.year_start_date,
        d.year_end_date,
        d.fiscal_year,
        d.fiscal_quarter,
        d.is_weekend,
        d.is_weekday,
        
        -- Holiday information from seed files
        case when h.holiday_date is not null then true else false end as is_holiday,
        h.holiday_name,
        h.country as holiday_country
        
    from date_dimension d
    left join holidays h
        on d.date = h.holiday_date
)

select * from final

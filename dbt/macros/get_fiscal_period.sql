{% macro get_fiscal_period(date_column, fiscal_year_start_month=7, fiscal_year_start_day=1) %}
{#
    Calculate fiscal year and quarter based on configurable fiscal year start.
    Default: Australian fiscal year starts July 1 (month=7, day=1)
    
    Returns: fiscal_year (integer)
#}
    case 
        when extract(month from {{ date_column }}) > {{ fiscal_year_start_month }} 
            or (extract(month from {{ date_column }}) = {{ fiscal_year_start_month }} 
                and extract(day from {{ date_column }}) >= {{ fiscal_year_start_day }})
        then extract(year from {{ date_column }}) + 1
        else extract(year from {{ date_column }})
    end
{% endmacro %}

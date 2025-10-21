{% macro get_carrier_sla_target(carrier_column) %}
    case 
        when lower({{ carrier_column }}) like '%express%' then 2
        when lower({{ carrier_column }}) like '%standard%' then 5
        when lower({{ carrier_column }}) like '%economy%' then 7
        else 5  -- default to standard if unknown
    end
{% endmacro %}

{% macro calculate_age(birth_date_column, as_of_ts='current_timestamp') %}
  {#- Calculate age in years from birth date to reference timestamp -#}
  {#- Returns floor year difference -#}
floor(date_diff('year', cast({{ birth_date_column }} as date), cast({{ as_of_ts }} as date)))
{% endmacro %}

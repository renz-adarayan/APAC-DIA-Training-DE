{% macro validate_email(column) %}
  {#- Validate email format using pragmatic regex pattern -#}
  {#- Returns boolean: true if valid, false if invalid -#}
  regexp_matches({{ column }}, '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$')
{% endmacro %}

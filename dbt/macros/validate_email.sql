{% macro validate_email(column) %}
  {#- Validate email format using comprehensive validation rules -#}
  {#- Returns boolean: true if valid, false if invalid -#}
  (
    {{ column }} is not null
    and regexp_matches({{ column }}, '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$')
    and {{ column }} not like '%..%'  -- No consecutive dots
    and {{ column }} not like '.%'    -- No leading dot
    and {{ column }} not like '%.'    -- No trailing dot
    and {{ column }} not like '%@.%'  -- No dot immediately after @
    and {{ column }} not like '%.@%'  -- No dot immediately before @
    and length({{ column }}) <= 254   -- RFC 5321 length limit
  )
{% endmacro %}

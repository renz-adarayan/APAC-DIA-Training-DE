{% macro normalize_timestamp(column) %}
  {#- Cast timestamp column and force UTC timezone -#}
  cast({{ column }} as timestamp) AT TIME ZONE 'UTC'
{% endmacro %}

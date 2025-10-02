{% macro json_extract_text(json_col, json_path) %}
  {#- Extract text value from JSON column using JSON path -#}
  {#- Wrapper for DuckDB JSON extraction -#}
  json_extract_string({{ json_col }}, '{{ json_path }}')
{% endmacro %}

{% macro safe_cast(expr, type_text) %}
  {#- Safely cast expression to type, returning null if cast fails -#}
  try_cast({{ expr }} as {{ type_text }})
{% endmacro %}

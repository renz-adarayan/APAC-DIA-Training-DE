{% macro clean_string(column, lower=true, squash_spaces=true) %}
  {#- Clean string by trimming whitespace and optionally lowercasing and collapsing internal spaces -#}
  {% set cleaned_column = "trim(" ~ column ~ ")" %}
  
  {% if squash_spaces %}
    {% set cleaned_column = "regexp_replace(" ~ cleaned_column ~ ", '\\s+', ' ', 'g')" %}
  {% endif %}
  
  {% if lower %}
    {% set cleaned_column = "lower(" ~ cleaned_column ~ ")" %}
  {% endif %}
  
  {{ cleaned_column }}
{% endmacro %}

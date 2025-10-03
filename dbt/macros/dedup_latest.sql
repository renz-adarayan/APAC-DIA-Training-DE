{% macro dedup_latest(pk_cols, order_col) %}
  {#- Generate row_number window function for deduplication -#}
  {#- pk_cols: primary key columns (string or array) -#}
  {#- order_col: column to order by for tie-breaking -#}
  {#- Returns the window function expression, not a condition -#}
  
  {% if pk_cols is string %}
    {% set partition_cols = [pk_cols] %}
  {% else %}
    {% set partition_cols = pk_cols %}
  {% endif %}
  
  row_number() over (
    partition by {{ partition_cols | join(', ') }}
    order by {{ order_col }} desc
  )
{% endmacro %}

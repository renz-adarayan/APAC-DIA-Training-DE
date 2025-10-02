{% macro dedup_latest(pk_cols, order_col) %}
  {#- Deduplicate rows using window function with row_number -#}
  {#- pk_cols: primary key columns (string or array) -#}
  {#- order_col: column to order by for tie-breaking -#}
  
  {% if pk_cols is string %}
    {% set partition_cols = [pk_cols] %}
  {% else %}
    {% set partition_cols = pk_cols %}
  {% endif %}
  
  row_number() over (
    partition by {{ partition_cols | join(', ') }}
    order by {{ order_col }} desc
  ) = 1
{% endmacro %}

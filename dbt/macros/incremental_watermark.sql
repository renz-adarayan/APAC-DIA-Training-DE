{% macro incremental_watermark(ts_col) %}
  {#- Generate WHERE clause for incremental processing using MAX timestamp -#}
  {#- ts_col: timestamp column name for watermarking -#}
  {{ ts_col }} > (select coalesce(max({{ ts_col }}), '1970-01-01'::timestamp) from {{ this }})
{% endmacro %}

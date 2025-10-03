{% macro incremental_watermark_with_lookback(ts_col, lookback_hours) %} 
  {#- Generate WHERE clause for incremental processing with lookback window for late-arriving data -#}
  {#- ts_col: timestamp column name for watermarking -#}
  {#- lookback_hours: number of hours to look back for late arrivals -#}
  {{ ts_col }} >= (
    select coalesce(
      max({{ ts_col }}) - INTERVAL '{{ lookback_hours }} hours', 
      '1970-01-01'::timestamp
    ) 
    from {{ this }}
  )
{% endmacro %}

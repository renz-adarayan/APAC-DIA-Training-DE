{% macro incremental_watermark_with_lookback_cte(ts_col, lookback_hours) %} 
  incremental_watermark as (
    select coalesce(
      max({{ ts_col }}) - INTERVAL '{{ lookback_hours }} hours', 
      '1970-01-01'::timestamp
    ) as watermark_value
    from {{ this }}
  )
{% endmacro %}

{% macro incremental_watermark_with_lookback(ts_col, lookback_hours) %} 
  {{ ts_col }} >= (select watermark_value from incremental_watermark)
{% endmacro %}

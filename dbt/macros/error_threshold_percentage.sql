{% macro get_table_row_count(table_ref) %}
  {#- Get the actual row count from a table -#}
  {% set query %}
    select count(*) as row_count from {{ table_ref }}
  {% endset %}
  
  {% if execute %}
    {% set results = run_query(query) %}
    {% set row_count = results.columns[0].values()[0] %}
    {{ return(row_count) }}
  {% else %}
    {#- Return default during parsing -#}
    {{ return(1000) }}
  {% endif %}
{% endmacro %}

{% macro dynamic_fk_threshold(parent_table_ref, error_pct=1.0, warn_pct=0.5) %}
  {#- Calculate FK violation thresholds based on actual parent table size -#}
  
  {% set actual_count = get_table_row_count(parent_table_ref) %}
  {% set error_threshold = (actual_count * error_pct / 100) | round | int %}
  {% set warn_threshold = (actual_count * warn_pct / 100) | round | int %}
  
  {#- Ensure minimum thresholds of at least 1 -#}
  {% set error_threshold = [error_threshold, 1] | max %}
  {% set warn_threshold = [warn_threshold, 1] | max %}
  
  {{ return({'error_if': '>= ' ~ error_threshold, 'warn_if': '>= ' ~ warn_threshold}) }}
{% endmacro %}

{% macro dynamic_fk_threshold_child_based(child_table_ref, error_pct=1.0, warn_pct=0.5) %}
  {#- Calculate FK violation thresholds based on actual child table size -#}
  {#- Use this when the child table size is more relevant for threshold calculation -#}
  
  {% set actual_count = get_table_row_count(child_table_ref) %}
  {% set error_threshold = (actual_count * error_pct / 100) | round | int %}
  {% set warn_threshold = (actual_count * warn_pct / 100) | round | int %}
  
  {#- Ensure minimum thresholds of at least 1 -#}
  {% set error_threshold = [error_threshold, 1] | max %}
  {% set warn_threshold = [warn_threshold, 1] | max %}
  
  {{ return({'error_if': '>= ' ~ error_threshold, 'warn_if': '>= ' ~ warn_threshold}) }}
{% endmacro %}
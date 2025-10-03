{% macro get_lake_path() %}
  {# 
    Dynamic lake path resolution using dbt context:
    Build absolute path dynamically from target.path
  #}
  
  {# Build absolute path dynamically from dbt context #}
  {% set current_target = target.path %}
  {% set db_dir = current_target.split('duckdb')[0] %}
  {% set lake_path = db_dir ~ 'lake/bronze' %}
  {{ return(lake_path.replace('\\', '/')) }}
{% endmacro %}
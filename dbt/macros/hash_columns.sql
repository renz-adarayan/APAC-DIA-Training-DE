{% macro hash_columns(columns) %}
  {#- Generate stable MD5 hash from array of columns -#}
  {#- columns should be an array of column names -#}
  {% if columns is string %}
    {#- Handle single column passed as string -#}
    md5(cast({{ columns }} as varchar))
  {% else %}
    {#- Handle array of columns -#}
    md5(concat({% for col in columns %}cast({{ col }} as varchar){% if not loop.last %}, '|', {% endif %}{% endfor %}))
  {% endif %}
{% endmacro %}

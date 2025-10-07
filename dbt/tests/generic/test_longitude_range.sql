{% test longitude_range(model, column_name) %}

-- Test that longitude is within valid range (-180 to 180)
select *
from {{ model }}
where {{ column_name }} is not null
  and (
    {{ column_name }} < -180
    or {{ column_name }} > 180
  )

{% endtest %}

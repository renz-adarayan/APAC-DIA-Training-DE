{% test latitude_range(model, column_name) %}

-- Test that latitude is within valid range (-90 to 90)
select *
from {{ model }}
where {{ column_name }} is not null
  and (
    {{ column_name }} < -90
    or {{ column_name }} > 90
  )

{% endtest %}

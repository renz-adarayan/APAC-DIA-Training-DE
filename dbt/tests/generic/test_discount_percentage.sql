{% test discount_percentage(model, column_name) %}

-- Test that discount percentage is within valid range (0-100)
select *
from {{ model }}
where {{ column_name }} is not null
  and (
    {{ column_name }} < 0
    or {{ column_name }} > 100
  )

{% endtest %}

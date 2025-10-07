{% test email_format(model, column_name) %}

-- Test that email addresses follow a basic valid format
-- Checks for: text@text.text pattern with basic validation rules
select *
from {{ model }}
where {{ column_name }} is not null
  and not (
    -- Basic email format validation
    {{ column_name }} ~ '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'
    -- Additional checks for common invalid patterns
    and {{ column_name }} not like '%..%'  -- No consecutive dots
    and {{ column_name }} not like '.%'    -- No leading dot
    and {{ column_name }} not like '%.'    -- No trailing dot
    and {{ column_name }} not like '%@.%'  -- No dot immediately after @
    and {{ column_name }} not like '%.@%'  -- No dot immediately before @
    and length({{ column_name }}) <= 254   -- RFC 5321 length limit
  )

{% endtest %}
{% test fk_violation_rate(model, column_name, to, field, max_violation_rate=1.5, error_threshold=2.0) %}

-- Test that foreign key violation rate is below the specified threshold
-- max_violation_rate: Warning threshold (logs but doesn't fail)
-- error_threshold: Error threshold (fails the test)
-- Returns records only if violation rate exceeds the error threshold

with total_records as (
  select count(*) as total_count
  from {{ model }}
  where {{ column_name }} is not null
),

violations as (
  select count(*) as violation_count
  from {{ model }} child
  left join {{ to }} parent on child.{{ column_name }} = parent.{{ field }}
  where child.{{ column_name }} is not null
    and parent.{{ field }} is null
),

violation_rate as (
  select 
    total_records.total_count,
    violations.violation_count,
    case 
      when total_records.total_count > 0 then 
        (violations.violation_count * 100.0 / total_records.total_count)
      else 0 
    end as violation_rate_pct
  from total_records
  cross join violations
)

select 
  violation_rate_pct,
  violation_count,
  total_count,
  {{ max_violation_rate }} as warning_threshold,
  {{ error_threshold }} as error_threshold,
  'FK violation rate ' || round(violation_rate_pct, 2) || '% exceeds error threshold of ' || {{ error_threshold }} || '%' as message
from violation_rate
where violation_rate_pct > {{ error_threshold }}
  and violation_count != 1  -- Pass if exactly 1 violation (even if rate > threshold)

{% endtest %}
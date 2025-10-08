{{
  config(
    materialized='incremental',
    unique_key='return_key',
    on_schema_change='merge',
    tags=['silver', 'fact']
  )
}}

-- Returns fact table with enriched business metrics
-- Grain: One row per returned item (return_id)
-- Incremental strategy: Filter on return_ts_utc for new/updated returns

with returns_base as (
  select * from {{ ref('stg_returns') }}
),

-- Get original order and sales information for context
-- Aggregate when same product appears on multiple lines in same order
sales_context as (
  select
    ol.order_id,
    ol.product_id,
    oh.customer_id,
    oh.store_id,
    oh.order_ts_utc as order_ts,
    oh.order_date_local,
    oh.channel,
    oh.payment_method,
    oh.currency,
    -- Aggregate metrics when product appears on multiple lines
    avg(ol.unit_price) as unit_price,
    sum(ol.quantity) as quantity,
    sum(ol.line_total_after_discount) as line_total_after_discount
  from {{ ref('stg_orders_lines') }} ol
  inner join {{ ref('stg_orders_header') }} oh
    on ol.order_id = oh.order_id
  group by 
    ol.order_id,
    ol.product_id,
    oh.customer_id,
    oh.store_id,
    oh.order_ts_utc,
    oh.order_date_local,
    oh.channel,
    oh.payment_method,
    oh.currency
),

enriched as (
  select
    -- Unique identifier
    {{ hash_columns(['r.return_id']) }} as return_key,
    
    -- Primary identifiers
    r.return_id,
    r.order_id,
    r.product_id,
    
    -- Foreign key dimensions (from original sale)
    s.customer_id,
    s.store_id,
    
    -- Return date/time dimensions
    r.return_ts_utc as return_ts,
    date(r.return_ts_utc) as return_date,
    date_trunc('month', r.return_ts_utc) as return_month,
    date_trunc('year', r.return_ts_utc) as return_year,
    extract(dayofweek from r.return_ts_utc) as return_day_of_week,
    extract(hour from r.return_ts_utc) as return_hour,
    
    -- Original order context
    s.order_ts,
    s.order_date_local,
    s.channel,
    s.payment_method,
    s.currency,
    
    -- Return details
    r.quantity_returned,
    r.reason,
    r.return_reason_code,
    
    -- Original sale context
    s.unit_price as original_unit_price,
    s.quantity as original_quantity_ordered,
    s.line_total_after_discount as original_line_total,
    
    -- Calculated metrics
    s.unit_price * r.quantity_returned as return_amount_gross,
    
    -- Return timing analysis
    case 
      when s.order_date_local is not null 
      then date_diff('day', s.order_date_local, date(r.return_ts_utc))
      else null
    end as days_to_return,
    
    case 
      when s.order_date_local is not null then
        case 
          when date_diff('day', s.order_date_local, date(r.return_ts_utc)) <= 7 then 'Quick Return (≤7 days)'
          when date_diff('day', s.order_date_local, date(r.return_ts_utc)) <= 30 then 'Standard Return (8-30 days)'
          when date_diff('day', s.order_date_local, date(r.return_ts_utc)) <= 90 then 'Late Return (31-90 days)'
          else 'Very Late Return (>90 days)'
        end
      else 'Unknown Timing'
    end as return_timing_category,
    
    -- Return quantity analysis
    case
      when s.quantity is not null then
        case
          when r.quantity_returned = s.quantity then 'Full Return'
          when r.quantity_returned < s.quantity then 'Partial Return'
          else 'Over Return' -- Edge case, might indicate data quality issue
        end
      else 'Unknown Completeness'
    end as return_completeness,
    
    -- Return percentage
    case 
      when s.quantity is not null and s.quantity > 0
      then round((r.quantity_returned::decimal / s.quantity) * 100, 2)
      else null
    end as return_percentage,
    
    -- Reason categorization
    case 
      when lower(r.reason) like '%defect%' or lower(r.reason) like '%broken%' or lower(r.reason) like '%damage%' then 'Product Defect'
      when lower(r.reason) like '%size%' or lower(r.reason) like '%fit%' then 'Size/Fit Issue'
      when lower(r.reason) like '%expectation%' or lower(r.reason) like '%description%' then 'Not as Expected'
      when lower(r.reason) like '%mind%' or lower(r.reason) like '%cancel%' then 'Changed Mind'
      when lower(r.reason) like '%duplicate%' then 'Duplicate Order'
      else 'Other'
    end as return_category,
    
    -- Weekend/weekday classification
    case
      when extract(dayofweek from r.return_ts_utc) in (1, 7) then 'Weekend'
      else 'Weekday'
    end as return_day_type,
    
    -- Audit fields
    r.ingestion_ts,
    current_timestamp as dbt_updated_at
    
  from returns_base r
  left join sales_context s
    on r.order_id = s.order_id 
    and r.product_id = s.product_id
    
  {% if is_incremental() %}
    -- Incremental filter: only process returns with timestamps greater than current max
    where r.return_ts_utc > (
      select coalesce(max(return_ts), '1970-01-01'::timestamp) 
      from {{ this }}
    )
  {% endif %}
),

final as (
  select
    *,
    -- Additional business flags
    case 
      when return_category = 'Product Defect' then true 
      else false 
    end as is_quality_issue,
    
    case 
      when days_to_return <= 1 then true 
      else false 
    end as is_immediate_return,
    
    case 
      when return_amount_gross >= 100 then 'High Value Return'
      when return_amount_gross >= 25 then 'Medium Value Return'
      else 'Low Value Return'
    end as return_value_tier
    
  from enriched
)

select * from final
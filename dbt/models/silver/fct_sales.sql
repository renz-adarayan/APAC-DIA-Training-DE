{{
  config(
    materialized='incremental',
    unique_key='sales_key',
    on_schema_change='merge',
    tags=['silver', 'fact']
  )
}}

-- Sales fact table combining order header and line details
-- Grain: One row per order line (order_id + line_number)
-- Incremental strategy: Filter on order_ts from header for new/updated orders

with order_lines as (
  select * from {{ ref('stg_orders_lines') }}
),

order_headers as (
  select * from {{ ref('stg_orders_header') }}
),

joined as (
  select
    -- Composite key for uniqueness
    {{ hash_columns(['ol.order_id', 'ol.line_number']) }} as sales_key,
    
    -- Primary identifiers
    ol.order_id,
    ol.line_number,
    
    -- Foreign key dimensions
    oh.customer_id,
    ol.product_id,
    oh.store_id,
    
    -- Date/time dimensions
    oh.order_ts_utc as order_ts,
    oh.order_date_local,
    date_trunc('month', oh.order_date_local) as order_month,
    date_trunc('year', oh.order_date_local) as order_year,
    extract(dayofweek from oh.order_date_local) as order_day_of_week,
    extract(hour from oh.order_ts_utc) as order_hour,
    
    -- Order attributes
    oh.channel,
    oh.payment_method,
    oh.coupon_code,
    oh.currency,
    
    -- Line-level measures
    ol.quantity,
    ol.unit_price,
    ol.line_discount_pct,
    ol.tax_pct,
    
    -- Calculated measures
    ol.line_total_before_discount,
    ol.line_total_after_discount,
    ol.line_total_after_discount - ol.line_total_before_discount as line_discount_amount,
    ol.line_total_after_discount * coalesce(ol.tax_pct, 0) / 100 as line_tax_amount,
    ol.line_total_after_discount + (ol.line_total_after_discount * coalesce(ol.tax_pct, 0) / 100) as line_total_with_tax,
    
    -- Order-level measures (allocated proportionally to line)
    case 
      when oh.total_items > 0 
      then oh.shipping_fee * (ol.quantity::decimal / oh.total_items::decimal)
      else 0 
    end as allocated_shipping_fee,
    
    -- Audit fields
    oh.ingestion_ts,
    current_timestamp as dbt_updated_at
    
  from order_lines ol
  inner join order_headers oh
    on ol.order_id = oh.order_id
    
  {% if is_incremental() %}
    -- Incremental filter: only process orders with timestamps greater than current max
    where oh.order_ts_utc > (
      select coalesce(max(order_ts), '1970-01-01'::timestamp) 
      from {{ this }}
    )
  {% endif %}
),

final as (
  select
    *,
    -- Additional business metrics
    case 
      when line_discount_amount > 0 then 'Discounted'
      else 'Full Price'
    end as discount_flag,
    
    case
      when quantity = 1 then 'Single Item'
      when quantity between 2 and 5 then 'Small Quantity'
      when quantity between 6 and 10 then 'Medium Quantity'
      else 'Bulk Order'
    end as quantity_tier,
    
    case
      when extract(dayofweek from order_date_local) in (1, 7) then 'Weekend'
      else 'Weekday'
    end as day_type
    
  from joined
)

select * from final
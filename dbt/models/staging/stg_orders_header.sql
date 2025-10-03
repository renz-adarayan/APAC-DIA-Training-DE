{{ config(materialized='table') }}

-- DEDUPLICATION STRATEGY:
-- When multiple records exist for the same order_id, we keep the most recent
-- record based on ingestion_ts to ensure data freshness and consistency.

-- Ensure bronze views are created first
{% set _ = ref('_sources') %}

with src as (
  select * from {{ source('bronze', 'orders_header') }}
),

-- Calculate order line totals from orders_lines table
line_totals as (
  select 
    order_id,
    sum(line_total_after_discount) as order_total,
    sum(line_total_before_discount) as order_subtotal,
    sum(line_total_before_discount - line_total_after_discount) as order_discount_amount,
    sum(line_total_after_discount * coalesce(tax_pct, 0) / 100) as order_tax_amount,
    sum(quantity) as total_items
  from {{ ref('stg_orders_lines') }}
  group by order_id
),

cleaned as (
  select
    -- Primary key
    {{ safe_cast('src.order_id', 'bigint') }} as order_id,
    
    -- Foreign keys
    {{ safe_cast('src.customer_id', 'bigint') }} as customer_id,
    {{ safe_cast('src.store_id', 'bigint') }} as store_id,
    
    -- Order information
    {{ normalize_timestamp('src.order_ts') }} as order_ts_utc,
    {{ safe_cast('src.order_dt_local', 'date') }} as order_date_local,
    {{ clean_string('src.channel') }} as channel,
    {{ clean_string('src.payment_method') }} as payment_method,
    {{ clean_string('src.coupon_code') }} as coupon_code,
    
    -- Financial information
    {{ safe_cast('src.shipping_fee', 'decimal(10,2)') }} as shipping_fee,
    {{ clean_string('src.currency') }} as currency,
    
    -- Business metrics (calculated from line items)
    coalesce(line_totals.order_subtotal, 0.00) as order_subtotal,
    coalesce(line_totals.order_discount_amount, 0.00) as order_discount_amount,
    coalesce(line_totals.order_tax_amount, 0.00) as order_tax_amount,
    coalesce(line_totals.order_total, 0.00) + coalesce({{ safe_cast('src.shipping_fee', 'decimal(10,2)') }}, 0.00) as order_final_total,
    coalesce(line_totals.total_items, 0) as total_items,
    
    -- Order classification
    case 
      when coalesce(line_totals.order_total, 0.00) + coalesce({{ safe_cast('src.shipping_fee', 'decimal(10,2)') }}, 0.00) >= 1000 then 'high_value'
      when coalesce(line_totals.order_total, 0.00) + coalesce({{ safe_cast('src.shipping_fee', 'decimal(10,2)') }}, 0.00) >= 100 then 'medium_value'
      else 'low_value'
    end as order_value_tier,
    
    -- Promotional analysis
    case 
      when {{ clean_string('src.coupon_code') }} is not null and {{ clean_string('src.coupon_code') }} != '' then true 
      else false 
    end as has_coupon,
    
    case 
      when coalesce(line_totals.order_discount_amount, 0.00) > 0 then 
        round((coalesce(line_totals.order_discount_amount, 0.00) / nullif(coalesce(line_totals.order_subtotal, 0.00), 0)) * 100, 2)
      else 0.00 
    end as overall_discount_percentage,
    
    -- Temporal analysis
    extract('hour' from {{ normalize_timestamp('src.order_ts') }}) as order_hour,
    extract('dow' from {{ normalize_timestamp('src.order_ts') }}) as order_day_of_week,
    
    case 
      when extract('hour' from {{ normalize_timestamp('src.order_ts') }}) between 6 and 11 then 'morning'
      when extract('hour' from {{ normalize_timestamp('src.order_ts') }}) between 12 and 17 then 'afternoon'
      when extract('hour' from {{ normalize_timestamp('src.order_ts') }}) between 18 and 21 then 'evening'
      else 'night'
    end as order_time_of_day,
    
    case 
      when extract('dow' from {{ normalize_timestamp('src.order_ts') }}) in (0, 6) then 'weekend'
      else 'weekday'
    end as order_day_type,
    
    -- Basket analysis
    case 
      when coalesce(line_totals.total_items, 0) = 1 then 'single_item'
      when coalesce(line_totals.total_items, 0) <= 5 then 'small_basket'
      when coalesce(line_totals.total_items, 0) <= 15 then 'medium_basket'
      else 'large_basket'
    end as basket_size_category,
    
    -- Audit columns
    src.src_filename,
    src.src_row_hash,
    {{ normalize_timestamp('src.ingestion_ts') }} as ingestion_ts
    
  from src
  left join line_totals on src.order_id = line_totals.order_id
),

deduped as (
  select *,
    {{ dedup_latest('order_id', 'ingestion_ts') }} as rn
  from cleaned
),

final as (
  select 
    order_id,
    customer_id,
    store_id,
    order_ts_utc,
    order_date_local,
    channel,
    payment_method,
    coupon_code,
    shipping_fee,
    currency,
    -- Business metrics
    order_subtotal,
    order_discount_amount,
    order_tax_amount,
    order_final_total,
    total_items,
    order_value_tier,
    has_coupon,
    overall_discount_percentage,
    order_hour,
    order_day_of_week,
    order_time_of_day,
    order_day_type,
    basket_size_category,
    -- Audit columns
    src_filename,
    src_row_hash,
    ingestion_ts
  from deduped
  where rn = 1
)

select * from final

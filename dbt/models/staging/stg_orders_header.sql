{{ config(materialized='table') }}

-- DEDUPLICATION STRATEGY:
-- When multiple records exist for the same order_id, we keep the most recent
-- record based on ingestion_ts to ensure data freshness and consistency.

-- Ensure bronze views are created first
{% set _ = ref('_sources') %}

with src as (
  select * from {{ source('bronze', 'orders_header') }}
),

cleaned as (
  select
    -- Primary key
    {{ safe_cast('order_id', 'bigint') }} as order_id,
    
    -- Foreign keys
    {{ safe_cast('customer_id', 'bigint') }} as customer_id,
    {{ safe_cast('store_id', 'bigint') }} as store_id,
    
    -- Order information
    {{ normalize_timestamp('order_ts') }} as order_ts_utc,
    {{ safe_cast('order_dt_local', 'date') }} as order_date_local,
    {{ clean_string('channel') }} as channel,
    {{ clean_string('payment_method') }} as payment_method,
    {{ clean_string('coupon_code') }} as coupon_code,
    
    -- Financial information
    {{ safe_cast('shipping_fee', 'decimal(10,2)') }} as shipping_fee,
    {{ clean_string('currency') }} as currency,
    
    -- Audit columns
    src_filename,
    src_row_hash,
    {{ normalize_timestamp('ingestion_ts') }} as ingestion_ts
    
  from src
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
    src_filename,
    src_row_hash,
    ingestion_ts
  from deduped
  where rn = 1
)

select * from final

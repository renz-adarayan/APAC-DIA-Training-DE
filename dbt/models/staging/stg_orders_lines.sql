{{ config(materialized='table') }}

-- DEDUPLICATION STRATEGY:
-- When multiple records exist for the same order line (composite key: order_id + line_number),
-- we keep the most recent record based on ingestion_ts to ensure data freshness and consistency.

-- Ensure bronze views are created first
{% set _ = ref('_sources') %}

with src as (
  select * from {{ source('bronze', 'orders_lines') }}
),

cleaned as (
  select
    -- Composite primary key
    {{ safe_cast('order_id', 'bigint') }} as order_id,
    {{ safe_cast('line_number', 'bigint') }} as line_number,
    
    -- Foreign keys
    {{ safe_cast('product_id', 'bigint') }} as product_id,
    
    -- Line item information
    {{ safe_cast('qty', 'integer') }} as quantity,
    {{ safe_cast('unit_price', 'decimal(10,2)') }} as unit_price,
    {{ safe_cast('line_discount_pct', 'decimal(5,2)') }} as line_discount_pct,
    {{ safe_cast('tax_pct', 'decimal(5,2)') }} as tax_pct,
    
    -- Calculated fields
    {{ safe_cast('unit_price', 'decimal(10,2)') }} * {{ safe_cast('qty', 'integer') }} as line_total_before_discount,
    ({{ safe_cast('unit_price', 'decimal(10,2)') }} * {{ safe_cast('qty', 'integer') }}) * 
    (1 - coalesce({{ safe_cast('line_discount_pct', 'decimal(5,2)') }}, 0) / 100) as line_total_after_discount,
    
    -- Business key for deduplication
    {{ hash_columns(['order_id', 'line_number']) }} as order_line_business_key,
    
    -- Audit columns
    src_filename,
    src_row_hash,
    {{ normalize_timestamp('ingestion_ts') }} as ingestion_ts
    
  from src
),

deduped as (
  select *,
    {{ dedup_latest('order_line_business_key', 'ingestion_ts') }} as rn
  from cleaned
),

final as (
  select 
    order_id,
    line_number,
    product_id,
    quantity,
    unit_price,
    line_discount_pct,
    tax_pct,
    line_total_before_discount,
    line_total_after_discount,
    order_line_business_key,
    src_filename,
    src_row_hash,
    ingestion_ts
  from deduped
  where rn = 1
)

select * from final

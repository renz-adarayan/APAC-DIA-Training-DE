{{ config(materialized='table') }}

-- DEDUPLICATION STRATEGY:
-- When multiple records exist for the same product_id, we keep the most recent
-- record based on ingestion_ts to ensure data freshness and consistency.

-- Ensure bronze views are created first
{% set _ = ref('_sources') %}

with src as (
  select * from {{ source('bronze', 'products') }}
),

cleaned as (
  select
    -- Primary keys
    {{ safe_cast('product_id', 'bigint') }} as product_id,
    {{ clean_string('sku') }} as sku,
    
    -- Product information
    {{ clean_string('name', lower=false) }} as product_name,
    {{ clean_string('category') }} as category,
    {{ clean_string('subcategory') }} as subcategory,
    
    -- Pricing information
    {{ safe_cast('current_price', 'decimal(10,2)') }} as current_price,
    {{ clean_string('currency') }} as currency,
    
    -- Product lifecycle dates
    {{ safe_cast('introduced_dt', 'date') }} as introduced_dt,
    {{ safe_cast('discontinued_dt', 'date') }} as discontinued_dt,
    {{ safe_cast('is_discontinued', 'boolean') }} as is_discontinued,
    
    -- Audit columns
    {{ clean_string('src_filename') }} as src_filename,
    {{ clean_string('src_row_hash') }} as src_row_hash,
    {{ normalize_timestamp('ingestion_ts') }} as ingestion_ts
    
  from src
),

deduped as (
  select *,
    {{ dedup_latest('product_id', 'ingestion_ts') }} as rn
  from cleaned
),

final as (
  select 
    product_id,
    sku,
    product_name,
    category,
    subcategory,
    current_price,
    currency,
    introduced_dt,
    discontinued_dt,
    is_discontinued,
    src_filename,
    src_row_hash,
    ingestion_ts
  from deduped
  where rn = 1
)

select * from final
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
    
    -- Pricing information (handle anomalies: 0.1-0.5% missing/invalid prices)
    case 
      when {{ safe_cast('current_price', 'decimal(10,2)') }} > 0 
      then {{ safe_cast('current_price', 'decimal(10,2)') }}
      else null 
    end as current_price,
    {{ clean_string('currency') }} as currency,
    
    -- Product lifecycle dates
    {{ safe_cast('introduced_dt', 'date') }} as introduced_dt,
    {{ safe_cast('discontinued_dt', 'date') }} as discontinued_dt,
    {{ safe_cast('is_discontinued', 'boolean') }} as is_discontinued,
    
    -- Product business metrics
    date_diff('day', {{ safe_cast('introduced_dt', 'date') }}, current_date) as product_age_days,
    
    case 
      when date_diff('day', {{ safe_cast('introduced_dt', 'date') }}, current_date) <= 30 then 'new'
      when date_diff('day', {{ safe_cast('introduced_dt', 'date') }}, current_date) <= 365 then 'recent'
      when date_diff('day', {{ safe_cast('introduced_dt', 'date') }}, current_date) <= 1095 then 'mature'  -- 3 years
      else 'legacy'
    end as product_lifecycle_stage,
    
    case 
      when {{ safe_cast('current_price', 'decimal(10,2)') }} is null or {{ safe_cast('current_price', 'decimal(10,2)') }} <= 0 then 'unknown'
      when {{ safe_cast('current_price', 'decimal(10,2)') }} <= 25.00 then 'budget'
      when {{ safe_cast('current_price', 'decimal(10,2)') }} <= 100.00 then 'mid_range'
      when {{ safe_cast('current_price', 'decimal(10,2)') }} <= 500.00 then 'premium'
      else 'luxury'
    end as price_tier,
    
    case 
      when {{ safe_cast('is_discontinued', 'boolean') }} = true then 'discontinued'
      when {{ safe_cast('discontinued_dt', 'date') }} is not null then 'end_of_life'
      when date_diff('day', {{ safe_cast('introduced_dt', 'date') }}, current_date) <= 90 then 'launch_phase'
      else 'active'
    end as product_status,
    
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
    product_age_days,
    product_lifecycle_stage,
    price_tier,
    product_status,
    src_filename,
    src_row_hash,
    ingestion_ts
  from deduped
  where rn = 1
)

select * from final
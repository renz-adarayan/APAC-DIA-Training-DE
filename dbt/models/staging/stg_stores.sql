{{ config(materialized='table') }}

-- DEDUPLICATION STRATEGY:
-- When multiple records exist for the same store_id, we keep the most recent
-- record based on ingestion_ts to ensure data freshness and consistency.

-- Ensure bronze views are created first
{% set _ = ref('_sources') %}

with src as (
  select * from {{ source('bronze', 'stores') }}
),

cleaned as (
  select
    -- Primary keys
    {{ safe_cast('store_id', 'bigint') }} as store_id,
    {{ clean_string('store_code') }} as store_code,
    
    -- Store information
    {{ clean_string('name', lower=false) }} as store_name,
    {{ clean_string('channel') }} as channel,
    {{ clean_string('region') }} as region,
    {{ clean_string('state') }} as state_region,
    
    -- Geographic coordinates with validation
    {{ safe_cast('latitude', 'double') }} as latitude,
    {{ safe_cast('longitude', 'double') }} as longitude,
    
    -- Dates
    {{ safe_cast('open_dt', 'date') }} as opened_date,
    {{ safe_cast('close_dt', 'date') }} as closed_date,
    
    -- Audit columns
    src_filename,
    src_row_hash,
    {{ normalize_timestamp('ingestion_ts') }} as ingestion_ts

  from src
),

deduped as (
  select *,
    {{ dedup_latest('store_id', 'ingestion_ts') }} as rn
  from cleaned
),

final as (
  select 
    store_id,
    store_code,
    store_name,
    channel,
    region,
    state_region,
    latitude,
    longitude,
    opened_date,
    closed_date,
    src_filename,
    src_row_hash,
    ingestion_ts
  from deduped
  where rn = 1
)

select * from final

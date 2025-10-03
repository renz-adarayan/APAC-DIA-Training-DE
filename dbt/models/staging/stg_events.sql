{{ config(materialized='table') }}

-- DEDUPLICATION STRATEGY:
-- When multiple records exist for the same event_id, we keep the most recent
-- record based on ingestion_ts to ensure data freshness and consistency.

-- Ensure bronze views are created first
{% set _ = ref('_sources') %}

with src as (
  select * from {{ source('bronze', 'events') }}
),

parsed as (
  select
    -- Primary key
    {{ safe_cast('event_id', 'bigint') }} as event_id,
    
    -- Core event fields
    {{ normalize_timestamp('event_ts') }} as event_ts_utc,
    {{ clean_string('event_type') }} as event_type,
    {{ safe_cast('user_id', 'bigint') }} as user_id,
    {{ clean_string('session_id') }} as session_id,
    
    -- Extract additional fields from JSON payload if present
    {{ json_extract_text('payload', '$.product_id') }} as product_id_raw,
    {{ json_extract_text('payload', '$.page_url') }} as page_url,
    {{ json_extract_text('payload', '$.device_type') }} as device_type,
    {{ json_extract_text('payload', '$.browser') }} as browser,
    {{ json_extract_text('payload', '$.ip_address') }} as ip_address,
    
    -- Audit columns
    src_filename,
    src_row_hash,
    {{ normalize_timestamp('ingestion_ts') }} as ingestion_ts
    
  from src
),

cleaned as (
  select
    event_id,
    event_ts_utc,
    event_type,
    user_id,
    session_id,
    
    -- Type conversions for extracted fields
    {{ safe_cast('product_id_raw', 'bigint') }} as product_id,
    {{ clean_string('page_url') }} as page_url,
    {{ clean_string('device_type') }} as device_type,
    {{ clean_string('browser') }} as browser,
    {{ clean_string('ip_address') }} as ip_address,
    
    -- Audit columns
    src_filename,
    src_row_hash,
    ingestion_ts
    
  from parsed
),

deduped as (
  select *,
    {{ dedup_latest('event_id', 'ingestion_ts') }} as rn
  from cleaned
),

final as (
  select 
    event_id,
    event_ts_utc,
    event_type,
    user_id,
    session_id,
    product_id,
    page_url,
    device_type,
    browser,
    ip_address,
    src_filename,
    src_row_hash,
    ingestion_ts
  from deduped
  where rn = 1
)

select * from final

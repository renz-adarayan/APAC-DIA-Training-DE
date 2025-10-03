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
    -- Extract primary key from JSON
    {{ json_extract_text('json', '$.event_id') }} as event_id_raw,
    
    -- Extract core event fields from JSON
    {{ json_extract_text('json', '$.event_ts') }} as event_ts_raw,
    {{ json_extract_text('json', '$.event_type') }} as event_type,
    {{ json_extract_text('json', '$.user_id') }} as user_id_raw,
    {{ json_extract_text('json', '$.session_id') }} as session_id,
    
    -- Extract payload fields from JSON
    {{ json_extract_text('json', '$.payload.product_id') }} as product_id_raw,
    {{ json_extract_text('json', '$.payload.page_url') }} as page_url,
    {{ json_extract_text('json', '$.payload.device_type') }} as device_type,
    {{ json_extract_text('json', '$.payload.browser') }} as browser,
    {{ json_extract_text('json', '$.payload.ip_address') }} as ip_address,
    
    -- Extract audit columns from _audit object
    {{ json_extract_text('json', '$._audit.src_filename') }} as src_filename,
    {{ json_extract_text('json', '$._audit.src_row_hash') }} as src_row_hash,
    {{ json_extract_text('json', '$._audit.ingestion_ts') }} as ingestion_ts_raw
    
  from src
),

cleaned as (
  select
    -- Type conversions for extracted fields (event_id is UUID string)
    {{ clean_string('event_id_raw') }} as event_id,
    {{ normalize_timestamp('event_ts_raw') }} as event_ts_utc,
    {{ clean_string('event_type') }} as event_type,
    {{ safe_cast('user_id_raw', 'bigint') }} as user_id,
    {{ clean_string('session_id') }} as session_id,
    
    -- Type conversions for payload fields
    {{ safe_cast('product_id_raw', 'bigint') }} as product_id,
    {{ clean_string('page_url') }} as page_url,
    {{ clean_string('device_type') }} as device_type,
    {{ clean_string('browser') }} as browser,
    {{ clean_string('ip_address') }} as ip_address,
    
    -- Clean audit columns
    {{ clean_string('src_filename') }} as src_filename,
    {{ clean_string('src_row_hash') }} as src_row_hash,
    {{ normalize_timestamp('ingestion_ts_raw') }} as ingestion_ts
    
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

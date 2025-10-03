{{ config(materialized='table') }}

-- DEDUPLICATION STRATEGY:
-- When multiple records exist for the same return_id, we keep the most recent
-- record based on ingestion_ts to ensure data freshness and consistency.

with src as (
  select * from {{ source('bronze', 'returns') }}
),

cleaned as (
  select
    -- Primary key
    {{ safe_cast('return_id', 'bigint') }} as return_id,
    
    -- Foreign keys
    {{ safe_cast('order_id', 'bigint') }} as order_id,
    {{ safe_cast('product_id', 'bigint') }} as product_id,
    
    -- Return information
    {{ normalize_timestamp('return_ts') }} as return_ts_utc,
    {{ safe_cast('qty', 'integer') }} as quantity_returned,
    {{ clean_string('reason') }} as reason,
    {{ clean_string('return_reason_code') }} as return_reason_code,
    
    -- Audit columns
    src_filename,
    src_row_hash,
    {{ normalize_timestamp('ingestion_ts') }} as ingestion_ts
    
  from src
),

deduped as (
  select *,
    {{ dedup_latest('return_id', 'ingestion_ts') }} as rn
  from cleaned
),

final as (
  select 
    return_id,
    order_id,
    product_id,
    return_ts_utc,
    quantity_returned,
    reason,
    return_reason_code,
    src_filename,
    src_row_hash,
    ingestion_ts
  from deduped
  where rn = 1
)

select * from final

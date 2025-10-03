{{ config(materialized='table') }}

-- DEDUPLICATION STRATEGY:
-- When multiple records exist for the same exchange rate (composite key: rate_date + currency_code),
-- we keep the most recent record based on ingestion_ts to ensure data freshness and consistency.

-- Ensure bronze views are created first
{% set _ = ref('_sources') %}

with src as (
  select * from {{ source('bronze', 'exchange_rates') }}
),

cleaned as (
  select
    -- Composite primary key
    {{ safe_cast('date', 'date') }} as rate_date,
    {{ clean_string('currency') }} as currency_code,
    
    -- Rate information
    {{ safe_cast('rate_to_aud', 'decimal(12,6)') }} as rate_to_aud,
    
    -- Business key for deduplication
    {{ hash_columns(['date', 'currency']) }} as exchange_rate_key,
    
    -- Audit columns
    src_filename,
    src_row_hash,
    {{ normalize_timestamp('ingestion_ts') }} as ingestion_ts
    
  from src
),

deduped as (
  select *,
    {{ dedup_latest('exchange_rate_key', 'ingestion_ts') }} as rn
  from cleaned
),

final as (
  select 
    rate_date,
    currency_code,
    rate_to_aud,
    exchange_rate_key,
    src_filename,
    src_row_hash,
    ingestion_ts
  from deduped
  where rn = 1
)

select * from final

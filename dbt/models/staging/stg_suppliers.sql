{{ config(materialized='table') }}

-- DEDUPLICATION STRATEGY:
-- When multiple records exist for the same supplier_id, we keep the most recent
-- record based on ingestion_ts to ensure data freshness and consistency.

-- Ensure bronze views are created first
{% set _ = ref('_sources') %}

with src as (
  select * from {{ source('bronze', 'suppliers') }}
),

cleaned as (
  select
    -- Primary keys
    {{ safe_cast('supplier_id', 'bigint') }} as supplier_id,
    {{ clean_string('supplier_code') }} as supplier_code,
    
    -- Supplier information
    {{ clean_string('name', lower=false) }} as supplier_name,
    {{ clean_string('country_code') }} as country_code,
    
    -- Business attributes
    {{ safe_cast('lead_time_days', 'integer') }} as lead_time_days,
    {{ safe_cast('preferred', 'boolean') }} as is_preferred,
    
    -- Audit columns
    src_filename,
    src_row_hash,
    {{ normalize_timestamp('ingestion_ts') }} as ingestion_ts

  from src
),

deduped as (
  select *,
    {{ dedup_latest('supplier_id', 'ingestion_ts') }} as rn
  from cleaned
),

final as (
  select 
    supplier_id,
    supplier_code,
    supplier_name,
    country_code,
    lead_time_days,
    is_preferred,
    src_filename,
    src_row_hash,
    ingestion_ts
  from deduped
  where rn = 1
)

select * from final

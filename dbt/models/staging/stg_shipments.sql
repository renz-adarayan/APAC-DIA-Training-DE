{{ config(materialized='table') }}

-- DEDUPLICATION STRATEGY:
-- When multiple records exist for the same shipment_id, we keep the most recent
-- record based on ingestion_ts to ensure data freshness and consistency.

with src as (
  select * from {{ source('bronze', 'shipments') }}
),

cleaned as (
  select
    -- Primary key
    {{ safe_cast('shipment_id', 'bigint') }} as shipment_id,
    
    -- Foreign key
    {{ safe_cast('order_id', 'bigint') }} as order_id,
    
    -- Shipment information
    {{ clean_string('carrier') }} as carrier,
    
    -- Timestamps
    {{ normalize_timestamp('shipped_at') }} as shipped_at_utc,
    {{ normalize_timestamp('delivered_at') }} as delivered_at_utc,
    
    -- Costs
    {{ safe_cast('ship_cost', 'decimal(10,2)') }} as ship_cost,
    
    -- Audit columns
    src_filename,
    src_row_hash,
    {{ normalize_timestamp('ingestion_ts') }} as ingestion_ts
    
  from src
),

deduped as (
  select *,
    {{ dedup_latest('shipment_id', 'ingestion_ts') }} as rn
  from cleaned
),

final as (
  select 
    shipment_id,
    order_id,
    carrier,
    shipped_at_utc,
    delivered_at_utc,
    ship_cost,
    src_filename,
    src_row_hash,
    ingestion_ts
  from deduped
  where rn = 1
)

select * from final

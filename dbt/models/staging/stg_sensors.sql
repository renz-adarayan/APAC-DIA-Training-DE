{{ config(materialized='table') }}

-- DEDUPLICATION STRATEGY:
-- When multiple records exist for the same sensor reading (composite key: sensor_ts + store_id + shelf_id),
-- we keep the most recent record based on ingestion_ts to ensure data freshness and consistency.

-- Ensure bronze views are created first
{% set _ = ref('_sources') %}

with src as (
  select * from {{ source('bronze', 'sensors') }}
),

cleaned as (
  select
    -- Time dimension
    {{ normalize_timestamp('sensor_ts') }} as sensor_ts_utc,
    
    -- Location keys
    {{ safe_cast('store_id', 'bigint') }} as store_id,
    {{ clean_string('shelf_id') }} as shelf_id,
    
    -- Sensor measurements
    {{ safe_cast('temperature_c', 'double') }} as temperature_celsius,
    {{ safe_cast('humidity_pct', 'double') }} as humidity_percent,
    {{ safe_cast('battery_mv', 'integer') }} as battery_millivolts,
    
    -- Business key for deduplication
    {{ hash_columns(['sensor_ts', 'store_id', 'shelf_id']) }} as sensor_reading_key,
    
    -- Audit columns
    src_filename,
    src_row_hash,
    {{ normalize_timestamp('ingestion_ts') }} as ingestion_ts
    
  from src
),

deduped as (
  select *,
    {{ dedup_latest('sensor_reading_key', 'ingestion_ts') }} as rn
  from cleaned
),

final as (
  select 
    sensor_ts_utc,
    store_id,
    shelf_id,
    temperature_celsius,
    humidity_percent,
    battery_millivolts,
    sensor_reading_key,
    src_filename,
    src_row_hash,
    ingestion_ts
  from deduped
  where rn = 1
)

select * from final

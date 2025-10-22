{{ config(
    materialized='incremental',
    unique_key='sensor_hour_key',
    on_schema_change='append_new_columns',
    tags=['gold', 'fact']
) }}

-- Gold fct_sensor_readings: Sensor readings fact with hourly aggregation
-- Grain: One row per hour per store per shelf
-- Source: stg_sensors
-- Key transformation: Aggregate to hourly granularity and add surrogate keys

with sensor_readings as (
    select * from {{ ref('stg_sensors') }}
    {% if is_incremental() %}
    where ingestion_ts > (select max(ingestion_ts) from {{ this }})
    {% endif %}
),

dim_store as (
    select store_sk, store_id
    from {{ ref('dim_store') }}
),

dim_date as (
    select date_sk, date
    from {{ ref('dim_date') }}
),

-- Aggregate to hourly granularity per store/shelf
hourly_aggregated as (
    select
        -- Time dimension (truncate to hour)
        date_trunc('hour', sensor_ts_utc) as reading_hour,
        
        -- Location keys
        store_id,
        shelf_id,
        
        -- Aggregated measurements
        MIN(temperature_celsius) as temperature_min,
        MAX(temperature_celsius) as temperature_max,
        ROUND(AVG(temperature_celsius), 2) as temperature_avg,
        
        MIN(humidity_percent) as humidity_min,
        MAX(humidity_percent) as humidity_max,
        ROUND(AVG(humidity_percent), 2) as humidity_avg,
        
        MIN(battery_millivolts) as battery_min,
        MAX(battery_millivolts) as battery_max,
        ROUND(AVG(battery_millivolts), 1) as battery_avg,
        
        -- Count of readings and anomalies
        COUNT(*) as reading_count,
        SUM(CASE WHEN has_sensor_anomaly THEN 1 ELSE 0 END) as anomaly_count,
        
        -- Audit
        MAX(ingestion_ts) as ingestion_ts
        
    from sensor_readings
    group by 
        date_trunc('hour', sensor_ts_utc),
        store_id,
        shelf_id
),


fact_with_surrogate_keys as (
    select
        -- Unique key (composite grain)
        {{ hash_columns(['sr.reading_hour', 'sr.store_id', 'sr.shelf_id']) }} as sensor_hour_key,
        
        -- Surrogate keys
        s.store_sk,
        d.date_sk,
        
        -- Natural keys (keep for validation)
        sr.store_id,
        sr.shelf_id,
        
        -- Time dimension
        sr.reading_hour,
        d.date as reading_date,
        EXTRACT(hour FROM sr.reading_hour) as reading_hour_of_day,
        EXTRACT(dayofweek FROM sr.reading_hour) as reading_day_of_week,
        
        -- Hourly aggregated measurements
        sr.temperature_min,
        sr.temperature_max,
        sr.temperature_avg,
        sr.humidity_min,
        sr.humidity_max,
        sr.humidity_avg,
        sr.battery_min,
        sr.battery_max,
        sr.battery_avg,
        
        -- Reading counts
        sr.reading_count,
        sr.anomaly_count,
        ROUND((sr.anomaly_count::DECIMAL / NULLIF(sr.reading_count, 0)) * 100, 2) as anomaly_rate_pct,
        
        -- Business flags
        CASE 
            WHEN sr.anomaly_count > 0 THEN true 
            ELSE false 
        END as has_anomalies_this_hour,
        
        CASE 
            WHEN sr.temperature_avg < 0 THEN 'Below Freezing'
            WHEN sr.temperature_avg BETWEEN 0 AND 10 THEN 'Cold'
            WHEN sr.temperature_avg BETWEEN 10 AND 25 THEN 'Normal'
            WHEN sr.temperature_avg > 25 THEN 'Warm'
            ELSE 'Unknown'
        END as temperature_category,
        
        CASE 
            WHEN sr.battery_avg < 2000 THEN 'Critical'
            WHEN sr.battery_avg BETWEEN 2000 AND 2500 THEN 'Low'
            WHEN sr.battery_avg > 2500 THEN 'Normal'
            ELSE 'Unknown'
        END as battery_status,
        
        -- Audit
        sr.ingestion_ts,
        current_timestamp as dbt_updated_at
        
    from hourly_aggregated sr
    
    -- Join to dimension tables to get surrogate keys
    left join dim_store s
        on sr.store_id = s.store_id
    left join dim_date d
        on CAST(sr.reading_hour AS DATE) = d.date
)

select * from fact_with_surrogate_keys

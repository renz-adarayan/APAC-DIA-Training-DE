{{ config(
    materialized='table',
    tags=['gold', 'fact']
) }}

-- Gold fct_ingestion_audit: Pipeline monitoring fact for ingestion health
-- Grain: One row per file/partition processed
-- Source: manifest.processed_files
-- Key transformation: Add derived metrics and KPIs for pipeline monitoring

with manifest_files as (
    select * from {{ source('manifest', 'processed_files') }}
),

fact_with_metrics as (
    select
        -- Unique key (file path is primary key in manifest)
        {{ hash_columns(['src_path', 'processed_at']) }} as ingestion_audit_key,
        
        -- File identification
        src_path as file_path,
        file_hash,
        file_size_bytes,
        
        -- Processing timestamps
        processed_at as load_ts,
        CAST(processed_at AS DATE) as load_date,
        EXTRACT(hour FROM processed_at) as load_hour,
        EXTRACT(dayofweek FROM processed_at) as load_day_of_week,
        
        -- Processing metrics
        row_count as rows_processed,
        reject_count as rows_rejected,
        
        -- Convert processing duration from milliseconds to seconds
        ROUND(processing_duration_ms / 1000.0, 2) as processing_time_seconds,
        
        -- Calculate rejection rate
        ROUND((reject_count::DECIMAL / NULLIF(row_count, 0)) * 100, 2) as rejection_rate_pct,
        
        -- Status and errors
        status,
        error_message,
        
        -- Success flag
        CASE 
            WHEN status = 'SUCCESS' THEN true 
            ELSE false 
        END as is_successful,
        
        -- Performance categorization
        CASE 
            WHEN processing_duration_ms < 1000 THEN 'Fast (<1s)'
            WHEN processing_duration_ms BETWEEN 1000 AND 5000 THEN 'Normal (1-5s)'
            WHEN processing_duration_ms BETWEEN 5000 AND 30000 THEN 'Slow (5-30s)'
            WHEN processing_duration_ms > 30000 THEN 'Very Slow (>30s)'
            ELSE 'Unknown'
        END as processing_speed_category,
        
        -- Data quality categorization
        CASE 
            WHEN reject_count = 0 THEN 'Perfect Quality'
            WHEN (reject_count::DECIMAL / NULLIF(row_count, 0)) < 0.01 THEN 'High Quality (<1% rejects)'
            WHEN (reject_count::DECIMAL / NULLIF(row_count, 0)) < 0.05 THEN 'Good Quality (1-5% rejects)'
            WHEN (reject_count::DECIMAL / NULLIF(row_count, 0)) < 0.10 THEN 'Fair Quality (5-10% rejects)'
            ELSE 'Poor Quality (>10% rejects)'
        END as data_quality_category,
        
        -- File size categorization
        CASE 
            WHEN file_size_bytes < 1024 THEN 'Tiny (<1KB)'
            WHEN file_size_bytes < 1048576 THEN 'Small (1KB-1MB)'
            WHEN file_size_bytes < 10485760 THEN 'Medium (1-10MB)'
            WHEN file_size_bytes < 104857600 THEN 'Large (10-100MB)'
            WHEN file_size_bytes >= 104857600 THEN 'Very Large (>100MB)'
            ELSE 'Unknown'
        END as file_size_category,
        
        -- Extract source system from path (assuming path format like 'bronze/customers/...')
        CASE 
            WHEN src_path LIKE '%customers%' THEN 'customers'
            WHEN src_path LIKE '%products%' THEN 'products'
            WHEN src_path LIKE '%orders%' THEN 'orders'
            WHEN src_path LIKE '%stores%' THEN 'stores'
            WHEN src_path LIKE '%suppliers%' THEN 'suppliers'
            WHEN src_path LIKE '%shipments%' THEN 'shipments'
            WHEN src_path LIKE '%returns%' THEN 'returns'
            WHEN src_path LIKE '%sensors%' THEN 'sensors'
            WHEN src_path LIKE '%events%' THEN 'events'
            WHEN src_path LIKE '%exchange_rates%' THEN 'exchange_rates'
            ELSE 'unknown'
        END as source_system,
        
        -- Audit
        current_timestamp as dbt_updated_at
        
    from manifest_files
)

select * from fact_with_metrics

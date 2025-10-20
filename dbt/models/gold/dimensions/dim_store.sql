{{ config(
    materialized='table',
    tags=['gold', 'dimension']
) }}

-- dim_store: Store dimension with size categorization
-- Grain: One row per store (store_id)
-- Dependencies: staging.stg_stores

with source as (
    select * from {{ ref('stg_stores') }}
),

enhanced as (
    select
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['store_id']) }} as store_sk,
        
        -- Natural key
        store_id,
        store_code,
        
        -- Store information
        store_name,
        channel,
        region,
        state_region,
        
        -- Geographic coordinates
        latitude,
        longitude,
        
        -- Store lifecycle
        opened_date,
        closed_date,
        store_age_days,
        store_maturity,
        store_status,
        store_type_category,
        
        -- Size categorization based on channel and maturity
        case
            -- Physical stores (POS)
            when store_type_category = 'physical' and store_maturity = 'mature_store' then 'Large Flagship'
            when store_type_category = 'physical' and store_maturity = 'established' then 'Standard Store'
            when store_type_category = 'physical' and store_maturity = 'new_store' then 'Small Startup'
            
            -- Digital stores (WEB)
            when store_type_category = 'digital' and store_maturity = 'mature_store' then 'Enterprise Platform'
            when store_type_category = 'digital' and store_maturity = 'established' then 'Regional Platform'
            when store_type_category = 'digital' and store_maturity = 'new_store' then 'Emerging Platform'
            
            -- Other/unknown
            else 'Uncategorized'
        end as store_size_category,
        
        -- Operational flags
        case
            when store_status = 'operational' then true
            else false
        end as is_operational,
        
        case
            when store_status = 'closed' then true
            else false
        end as is_closed,
        
        case
            when store_status = 'opening_phase' then true
            else false
        end as is_opening,
        
        -- Store performance indicators
        case
            when store_type_category = 'physical' then true
            else false
        end as is_physical_store,
        
        case
            when store_type_category = 'digital' then true
            else false
        end as is_digital_store,
        
        -- Regional classification
        case
            when region in ('NSW', 'VIC', 'QLD') then 'Major Markets'
            when region in ('SA', 'WA', 'TAS', 'ACT', 'NT') then 'Regional Markets'
            else 'Other'
        end as market_tier,
        
        -- Audit
        ingestion_ts as dbt_updated_at
        
    from source
)

select * from enhanced

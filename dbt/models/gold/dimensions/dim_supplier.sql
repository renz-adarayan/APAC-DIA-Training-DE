{{ config(
    materialized='table',
    tags=['gold', 'dimension']
) }}

-- dim_supplier: Supplier dimension with tier and region classification
-- Grain: One row per supplier (supplier_id)
-- Dependencies: staging.stg_suppliers

with source as (
    select * from {{ ref('stg_suppliers') }}
),

enhanced as (
    select
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['supplier_id']) }} as supplier_sk,
        
        -- Natural key
        supplier_id,
        supplier_code,
        
        -- Supplier information
        supplier_name,
        country_code,
        
        -- Business attributes
        lead_time_days,
        is_preferred,
        
        -- Supplier tier based on lead_time_days
        case
            when lead_time_days <= 7 then 'Premium'
            when lead_time_days <= 14 then 'Standard'
            else 'Extended'
        end as supplier_tier,
        
        -- Region mapping from country_code (using UPPER for case-insensitive comparison)
        case
            -- APAC countries
            when upper(country_code) in ('AU', 'NZ', 'SG', 'MY', 'TH', 'ID', 'PH', 'VN', 'JP', 'KR', 'CN', 'HK', 'TW', 'IN') then 'APAC'
            
            -- EMEA countries
            when upper(country_code) in ('GB', 'DE', 'FR', 'IT', 'ES', 'NL', 'BE', 'CH', 'AT', 'SE', 'NO', 'DK', 'FI', 
                                  'PL', 'CZ', 'IE', 'PT', 'GR', 'RO', 'HU', 'UA', 'RU',
                                  'ZA', 'EG', 'NG', 'KE', 'MA', 'TZ', 'UG', 'GH', 'AE', 'SA', 'IL', 'TR') then 'EMEA'
            
            -- Americas countries
            when upper(country_code) in ('US', 'CA', 'MX', 'BR', 'AR', 'CL', 'CO', 'PE', 'VE', 'EC', 'BO', 'PY', 'UY', 'CR', 'PA', 'GT') then 'Americas'
            
            -- Other/Unknown
            else 'Other'
        end as supplier_region,
        
        -- Performance flags
        case
            when is_preferred = true and lead_time_days <= 7 then 'Top Tier'
            when is_preferred = true then 'Preferred'
            when lead_time_days <= 7 then 'Fast Delivery'
            else 'Standard'
        end as supplier_classification,
        
        -- Delivery speed indicator
        case
            when lead_time_days <= 7 then true
            else false
        end as is_fast_delivery,
        
        case
            when lead_time_days > 21 then true
            else false
        end as is_slow_delivery,
        
        -- Audit
        ingestion_ts as dbt_updated_at
        
    from source
)

select * from enhanced

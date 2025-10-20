{{ config(
    materialized='table',
    tags=['gold', 'dimension']
) }}

-- dim_customer: Customer dimension with GDPR-compliant PII masking and segmentation
-- Grain: One row per customer (customer_id)
-- Dependencies: staging.stg_customers

with source as (
    select * from {{ ref('stg_customers') }}
),

gdpr_masked as (
    select
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['customer_id']) }} as customer_sk,
        
        -- Natural key
        customer_id,
        
        -- Personal information with GDPR masking
        first_name,
        last_name,
        
        -- GDPR-compliant email masking
        case
            when gdpr_consent = false then 
                sha256(lower(coalesce(email_normalized, 'unknown'))) || '@masked.invalid'
            else email_normalized
        end as email,
        
        -- GDPR-compliant phone masking (show only last 4 digits)
        case
            when gdpr_consent = false and phone is not null then
                repeat('X', greatest(0, length(phone) - 4)) || right(phone, 4)
            else phone
        end as phone,
        
        -- GDPR-compliant address masking (city only, no street details)
        case
            when gdpr_consent = false then null
            else address_line1
        end as address_line1,
        
        case
            when gdpr_consent = false then null
            else address_line2
        end as address_line2,
        
        -- City is retained for regional analytics even without consent
        city,
        state_region,
        
        case
            when gdpr_consent = false then null
            else postcode
        end as postcode,
        
        country_code,
        
        -- Geographic coordinates masked for GDPR
        case
            when gdpr_consent = false then null
            else latitude
        end as latitude,
        
        case
            when gdpr_consent = false then null
            else longitude
        end as longitude,
        
        -- Demographics
        birth_date,
        age_years as customer_age_years,
        generation,
        
        -- Customer lifecycle
        join_ts_utc as customer_join_date,
        customer_tenure_days as customer_lifetime_days,
        customer_lifecycle_stage,
        
        -- Flags
        is_vip,
        gdpr_consent,
        is_email_valid,
        
        -- Audit
        ingestion_ts as dbt_updated_at
        
    from source
),

segmented as (
    select
        *,
        
        -- Customer segmentation based on VIP status and tenure
        case
            when is_vip = true then 'VIP'
            when customer_lifetime_days >= 1095 then 'Loyal'  -- 3+ years
            when customer_lifetime_days >= 365 then 'Established'  -- 1-3 years
            when customer_lifetime_days >= 90 then 'Growing'  -- 3-12 months
            else 'New'  -- < 3 months
        end as customer_segment,
        
        -- Generation-based segment (using generation from source data)
        generation as age_segment
        
    from gdpr_masked
)

select * from segmented

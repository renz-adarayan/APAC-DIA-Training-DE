{{ config(materialized='table') }}

with src as (
  select * from {{ source('bronze', 'customers') }}
),

cleaned as (
  select
    -- Primary keys
    {{ safe_cast('customer_id', 'bigint') }} as customer_id,
    natural_key,
    
    -- Personal information with normalization
    {{ clean_string('first_name', lower=false) }} as first_name,
    {{ clean_string('last_name', lower=false) }} as last_name,
    {{ clean_string('email', lower=true) }} as email_normalized,
    {{ clean_string('phone') }} as phone,
    
    -- Address information  
    {{ clean_string('address_line1') }} as address_line1,
    {{ clean_string('address_line2') }} as address_line2,
    {{ clean_string('city') }} as city,
    {{ clean_string('state_region') }} as state_region,
    {{ clean_string('postcode') }} as postcode,
    {{ clean_string('country_code') }} as country_code,
    
    -- Geographic coordinates with validation
    {{ safe_cast('latitude', 'double') }} as latitude,
    {{ safe_cast('longitude', 'double') }} as longitude,
    
    -- Dates and derived columns
    {{ safe_cast('birth_date', 'date') }} as birth_date,
    {{ calculate_age('birth_date') }} as age_years,
    {{ normalize_timestamp('join_ts') }} as join_ts_utc,
    
    -- Flags
    {{ safe_cast('is_vip', 'boolean') }} as is_vip,
    {{ safe_cast('gdpr_consent', 'boolean') }} as gdpr_consent,
    
    -- Email validation
    {{ validate_email('email') }} as is_email_valid,
    
    -- Metadata
    {{ normalize_timestamp('updated_at') }} as updated_at,
    {{ normalize_timestamp('ingestion_ts') }} as ingestion_ts

  from src
),

deduped as (
  select *
  from cleaned
  where {{ dedup_latest('email_normalized', 'coalesce(updated_at, ingestion_ts)') }}
)

select * from deduped

{{ config(materialized='table') }}

-- DEDUPLICATION STRATEGY:
-- This model handles duplicate customers by natural_key as per requirements.
-- When multiple records exist for the same natural_key, we keep the most recent
-- record based on ingestion_ts to ensure data freshness and consistency.

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
    {{ normalize_timestamp('ingestion_ts') }} as ingestion_ts

  from src
),

deduped as (
  select *,
    row_number() over (
      partition by natural_key
      order by ingestion_ts desc
    ) as rn
  from cleaned
),

final as (
  select 
    customer_id,
    natural_key,
    first_name,
    last_name,
    email_normalized,
    phone,
    address_line1,
    address_line2,
    city,
    state_region,
    postcode,
    country_code,
    latitude,
    longitude,
    birth_date,
    age_years,
    join_ts_utc,
    is_vip,
    gdpr_consent,
    is_email_valid,
    ingestion_ts
  from deduped
  where rn = 1
)

select * from final

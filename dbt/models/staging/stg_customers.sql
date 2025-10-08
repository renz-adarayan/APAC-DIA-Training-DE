{{ config(materialized='table') }}

-- DEDUPLICATION STRATEGY:
-- When multiple records exist for the same natural_key, we keep the most recent
-- record based on ingestion_ts to ensure data freshness and consistency.

-- Ensure bronze views are created first
{% set _ = ref('_sources') %}

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
    -- Only normalize valid emails, set invalid ones to NULL
    case 
      when {{ validate_email('email') }} then {{ clean_string('email', lower=true) }}
      else null 
    end as email_normalized,
    {{ clean_string('phone') }} as phone,
    
    -- Address information  
    {{ clean_string('address_line1') }} as address_line1,
    {{ clean_string('address_line2') }} as address_line2,
    {{ clean_string('city') }} as city,
    {{ clean_string('state_region') }} as state_region,
    {{ clean_string('postcode') }} as postcode,
    {{ clean_string('country_code') }} as country_code,
    
    -- Geographic coordinates with validation (handle anomalies)
    case 
      when {{ safe_cast('latitude', 'double') }} between -90 and 90 
      then {{ safe_cast('latitude', 'double') }}
      else null 
    end as latitude,
    case 
      when {{ safe_cast('longitude', 'double') }} between -180 and 180 
      then {{ safe_cast('longitude', 'double') }}
      else null 
    end as longitude,
    
    -- Dates and derived columns
    {{ safe_cast('birth_date', 'date') }} as birth_date,
    {{ calculate_age('birth_date') }} as age_years,
    {{ normalize_timestamp('join_ts') }} as join_ts_utc,
    
    -- Customer business metrics
    date_diff('day', {{ safe_cast('join_ts', 'date') }}, current_date) as customer_tenure_days,
    case 
      when {{ calculate_age('birth_date') }} <= 27 then 'gen_z'        -- Born 1997-2012 (ages 13-27 in 2024)
      when {{ calculate_age('birth_date') }} <= 43 then 'millennial'   -- Born 1981-1996 (ages 28-43 in 2024)
      when {{ calculate_age('birth_date') }} <= 59 then 'gen_x'        -- Born 1965-1980 (ages 44-59 in 2024)
      when {{ calculate_age('birth_date') }} <= 78 then 'boomer'       -- Born 1946-1964 (ages 60-78 in 2024)
      else 'silent_generation'                                         -- Born before 1946 (79+ in 2024)
    end as generation,
    case 
      when date_diff('day', {{ safe_cast('join_ts', 'date') }}, current_date) < 30 then 'new'
      when date_diff('day', {{ safe_cast('join_ts', 'date') }}, current_date) < 365 then 'recent'
      else 'established'
    end as customer_lifecycle_stage,
    
    -- Flags
    {{ safe_cast('is_vip', 'boolean') }} as is_vip,
    {{ safe_cast('gdpr_consent', 'boolean') }} as gdpr_consent,
    
    -- Email validation
    {{ validate_email('email') }} as is_email_valid,
    
    -- Audit columns
    src_filename,
    src_row_hash,
    {{ normalize_timestamp('ingestion_ts') }} as ingestion_ts

  from src
),

deduped as (
  select *,
    {{ dedup_latest('customer_id', 'ingestion_ts') }} as rn
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
    customer_tenure_days,
    generation,
    customer_lifecycle_stage,
    is_vip,
    gdpr_consent,
    is_email_valid,
    src_filename,
    src_row_hash,
    ingestion_ts
  from deduped
  where rn = 1
)

select * from final

{{ config(
    materialized='table',
    tags=['gold', 'dimension']
) }}

-- dim_product_scd: Product dimension with Slowly Changing Dimension Type 2
-- Grain: One row per product version (product_id + dbt_scd_id)
-- Source: snapshots.products_snapshot with historical price tracking

with snapshot_products as (
    select 
        -- Natural keys
        product_id,
        sku,
        
        -- Product attributes
        name as product_name,
        category,
        subcategory,
        current_price,
        currency,
        introduced_dt,
        discontinued_dt,
        is_discontinued,
        
        -- SCD Type 2 metadata from dbt snapshot
        dbt_scd_id,
        dbt_updated_at,
        dbt_valid_from,
        dbt_valid_to
        
    from {{ ref('products_snapshot') }}
),

enriched as (
    select
        -- Surrogate key combining product_id and version
        {{ dbt_utils.generate_surrogate_key(['product_id', 'dbt_scd_id']) }} as product_sk,
        
        -- Natural keys
        product_id,
        sku,
        
        -- Product attributes
        product_name,
        category,
        subcategory,
        current_price,
        currency,
        introduced_dt,
        discontinued_dt,
        is_discontinued,
        
        -- SCD Type 2 fields
        dbt_scd_id,
        dbt_updated_at,
        dbt_valid_from,
        dbt_valid_to,
        
        -- Is this the current version?
        case 
            when dbt_valid_to is null then true 
            else false 
        end as is_current,
        
        -- Calculate product age and lifecycle
        date_diff('day', introduced_dt, current_date) as product_age_days,
        
        case 
            when date_diff('day', introduced_dt, current_date) <= 30 then 'new'
            when date_diff('day', introduced_dt, current_date) <= 365 then 'recent'
            when date_diff('day', introduced_dt, current_date) <= 1095 then 'mature'  -- 3 years
            else 'legacy'
        end as product_lifecycle_stage,
        
        case 
            when current_price is null or current_price <= 0 then 'unknown'
            when current_price <= 25.00 then 'budget'
            when current_price <= 100.00 then 'mid_range'
            when current_price <= 500.00 then 'premium'
            else 'luxury'
        end as price_tier,
        
        case 
            when is_discontinued = true then 'discontinued'
            when discontinued_dt is not null then 'end_of_life'
            when date_diff('day', introduced_dt, current_date) <= 90 then 'launch_phase'
            else 'active'
        end as product_status,
        
        -- Price change tracking using LAG window function
        lag(current_price) over (
            partition by product_id 
            order by dbt_valid_from
        ) as previous_price
        
    from snapshot_products
),

final as (
    select
        product_sk,
        product_id,
        sku,
        product_name,
        category,
        subcategory,
        current_price,
        currency,
        introduced_dt,
        discontinued_dt,
        is_discontinued,
        product_age_days,
        product_lifecycle_stage,
        price_tier,
        product_status,
        
        -- SCD Type 2 fields
        dbt_scd_id,
        dbt_updated_at,
        dbt_valid_from,
        dbt_valid_to,
        is_current,
        
        -- Price change metrics
        previous_price,
        case 
            when previous_price is null then null
            when current_price is null then null
            else current_price - previous_price
        end as price_change_from_previous,
        
        -- Price trend categorization
        case 
            when previous_price is null then 'initial'
            when current_price is null or previous_price is null then 'unknown'
            when current_price > previous_price then 'increasing'
            when current_price < previous_price then 'decreasing'
            else 'stable'
        end as price_trend
        
    from enriched
)

select * from final

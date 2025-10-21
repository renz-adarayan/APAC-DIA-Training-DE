{{ config(
    materialized='incremental',
    unique_key='sales_key',
    on_schema_change='append_new_columns',
    tags=['gold', 'fact']
) }}

-- Gold fct_sales: Sales fact with dimensional model integration
-- Grain: One row per order line (order_id + line_number)
-- Source: silver.fct_sales
-- Key transformation: Replace natural keys with surrogate keys from dimensions

with silver_sales as (
    select * from {{ ref('fct_sales') }}
    {% if is_incremental() %}
    where dbt_updated_at > (select max(dbt_updated_at) from {{ this }})
    {% endif %}
),

dim_customer as (
    select customer_sk, customer_id
    from {{ ref('dim_customer') }}
),

dim_product_scd as (
    select 
        product_sk, 
        product_id, 
        dbt_valid_from, 
        dbt_valid_to,
        is_current
    from {{ ref('dim_product_scd') }}
),

dim_store as (
    select store_sk, store_id
    from {{ ref('dim_store') }}
),

dim_date as (
    -- Get one date_sk per date (dim_date has duplicates due to multi-country holidays)
    select 
        MIN(date_sk) as date_sk,
        date
    from {{ ref('dim_date') }}
    group by date
),

fact_with_surrogate_keys as (
    select
        -- Unique key from Silver
        sf.sales_key,
        
        -- Surrogate keys (replace natural keys)
        c.customer_sk,
        p.product_sk,
        s.store_sk,
        d.date_sk as order_date_sk,
        
        -- Natural keys (keep for validation and debugging)
        sf.customer_id,
        sf.product_id,
        sf.store_id,
        
        -- Degenerate dimensions (keep as-is)
        sf.order_id,
        sf.line_number,
        
        -- Date/time dimensions
        sf.order_ts,
        sf.order_date_local,
        sf.order_month,
        sf.order_year,
        sf.order_day_of_week,
        sf.order_hour,
        
        -- Order attributes
        sf.channel,
        sf.payment_method,
        sf.coupon_code,
        sf.currency,
        
        -- Line-level measures
        sf.quantity,
        sf.unit_price,
        sf.line_discount_pct,
        sf.tax_pct,
        
        -- Calculated measures
        sf.line_total_before_discount,
        sf.line_total_after_discount,
        sf.line_discount_amount,
        sf.line_tax_amount,
        sf.line_total_with_tax as net_amount,
        
        -- Order-level allocated measures
        sf.allocated_shipping_fee,
        
        -- Business metrics
        sf.discount_flag,
        sf.quantity_tier,
        sf.day_type,
        
        -- Audit
        sf.ingestion_ts,
        sf.dbt_updated_at
        
    from silver_sales sf
    
    -- Join to dimension tables to get surrogate keys
    left join dim_customer c
        on sf.customer_id = c.customer_id
    
    -- SCD Type 2 join: Use current version for now 
    -- have no matching product versions. Using is_current flag for initial load.
    -- Future sales will properly utilize SCD Type 2 date-based logic.
    left join dim_product_scd p
        on sf.product_id = p.product_id
        and p.is_current = true
    
    left join dim_store s
        on sf.store_id = s.store_id
    
    left join dim_date d
        on sf.order_date_local = d.date
)

select * from fact_with_surrogate_keys

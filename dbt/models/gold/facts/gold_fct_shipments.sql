{{ config(
    materialized='incremental',
    unique_key='shipment_key',
    on_schema_change='append_new_columns',
    tags=['gold', 'fact']
) }}

-- Gold fct_shipments: Shipments fact with dimensional model integration and SLA tracking
-- Grain: One row per shipment (shipment_id)
-- Source: silver.fct_shipments
-- Key transformation: Replace natural keys with surrogate keys and add SLA compliance metrics

with silver_shipments as (
    select * from {{ ref('fct_shipments') }}
    {% if is_incremental() %}
    where dbt_updated_at > (select max(dbt_updated_at) from {{ this }})
    {% endif %}
),

dim_customer as (
    select customer_sk, customer_id
    from {{ ref('dim_customer') }}
),

dim_store as (
    select store_sk, store_id
    from {{ ref('dim_store') }}
),

dim_date_shipped as (
    -- Get one date_sk per date (dim_date has duplicates due to multi-country holidays)
    select 
        MIN(date_sk) as date_sk,
        date
    from {{ ref('dim_date') }}
    group by date
),

dim_date_delivered as (
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
        sh.shipment_key,
        
        -- Surrogate keys (replace natural keys)
        c.customer_sk,
        st.store_sk,
        ds.date_sk as shipped_date_sk,
        dd.date_sk as delivered_date_sk,
        
        -- Natural keys (keep for validation and debugging)
        sh.customer_id,
        sh.store_id,
        
        -- Degenerate dimensions
        sh.shipment_id,
        sh.order_id,
        
        -- Shipment date/time dimensions
        sh.shipped_at,
        sh.shipped_date,
        sh.shipped_month,
        sh.shipped_year,
        sh.shipped_day_of_week,
        sh.shipped_hour,
        
        -- Delivery date/time dimensions
        sh.delivered_at,
        sh.delivered_date,
        sh.delivered_month,
        sh.delivered_day_of_week,
        sh.delivered_hour,
        
        -- Original order context
        sh.order_ts,
        sh.order_date_local,
        sh.channel,
        sh.payment_method,
        sh.currency,
        sh.shipping_fee,
        sh.order_final_total,
        
        -- Shipment details
        sh.carrier,
        sh.ship_cost,
        
        -- Performance metrics from Silver
        sh.delivery_days,
        sh.delivery_hours,
        sh.days_to_ship,
        sh.hours_to_ship,
        sh.total_fulfillment_days,
        
        -- SLA compliance metrics
        {{ get_carrier_sla_target('sh.carrier') }} as target_delivery_days,
        
        case 
            when sh.delivery_days is not null and sh.delivery_days <= {{ get_carrier_sla_target('sh.carrier') }}
            then true
            else false
        end as is_on_time,
        
        case 
            when sh.delivery_days is not null
            then sh.delivery_days - {{ get_carrier_sla_target('sh.carrier') }}
            else null
        end as sla_variance_days,
        
        -- Status and categorization
        sh.delivery_status,
        sh.delivery_speed_category,
        sh.shipping_speed_category,
        sh.shipped_day_type,
        sh.delivered_day_type,
        
        -- Cost analysis
        sh.shipping_cost_variance,
        sh.shipping_cost_percentage,
        sh.shipping_cost_tier,
        
        -- Business flags
        sh.is_express_delivery,
        sh.is_same_day_ship,
        sh.is_potentially_lost,
        
        -- Audit
        sh.ingestion_ts,
        sh.dbt_updated_at
        
    from silver_shipments sh
    
    -- Join to dimension tables to get surrogate keys
    left join dim_customer c
        on sh.customer_id = c.customer_id
    
    left join dim_store st
        on sh.store_id = st.store_id
    
    left join dim_date_shipped ds
        on sh.shipped_date = ds.date
    
    left join dim_date_delivered dd
        on sh.delivered_date = dd.date
)

select * from fact_with_surrogate_keys

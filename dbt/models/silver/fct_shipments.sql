{{
  config(
    materialized='incremental',
    unique_key='shipment_key',
    on_schema_change='merge',
    tags=['silver', 'fact']
  )
}}

-- Shipments fact table with logistics and delivery performance metrics
-- Grain: One row per shipment (shipment_id)
-- Incremental strategy: Filter on shipped_at_utc for new/updated shipments

with shipments_base as (
  select * from {{ ref('stg_shipments') }}
),

-- Get original order context for enrichment
order_context as (
  select
    order_id,
    customer_id,
    store_id,
    order_ts_utc,
    order_date_local,
    channel,
    payment_method,
    currency,
    shipping_fee,
    order_final_total
  from {{ ref('stg_orders_header') }}
),

enriched as (
  select
    -- Unique identifier
    {{ hash_columns(['s.shipment_id']) }} as shipment_key,
    
    -- Primary identifiers
    s.shipment_id,
    s.order_id,
    
    -- Foreign key dimensions (from original order)
    o.customer_id,
    o.store_id,
    
    -- Shipment date/time dimensions
    s.shipped_at_utc as shipped_at,
    date(s.shipped_at_utc) as shipped_date,
    date_trunc('month', s.shipped_at_utc) as shipped_month,
    date_trunc('year', s.shipped_at_utc) as shipped_year,
    extract(dayofweek from s.shipped_at_utc) as shipped_day_of_week,
    extract(hour from s.shipped_at_utc) as shipped_hour,
    
    -- Delivery date/time dimensions
    s.delivered_at_utc as delivered_at,
    date(s.delivered_at_utc) as delivered_date,
    date_trunc('month', s.delivered_at_utc) as delivered_month,
    extract(dayofweek from s.delivered_at_utc) as delivered_day_of_week,
    extract(hour from s.delivered_at_utc) as delivered_hour,
    
    -- Original order context
    o.order_ts_utc as order_ts,
    o.order_date_local,
    o.channel,
    o.payment_method,
    o.currency,
    o.shipping_fee,
    o.order_final_total,
    
    -- Shipment details
    s.carrier,
    s.ship_cost,
    
    -- Delivery performance metrics
    case 
      when s.delivered_at_utc is not null and s.shipped_at_utc is not null
      then date_diff('day', date(s.shipped_at_utc), date(s.delivered_at_utc))
      else null
    end as delivery_days,
    
    case 
      when s.delivered_at_utc is not null and s.shipped_at_utc is not null
      then date_diff('hour', s.shipped_at_utc, s.delivered_at_utc)
      else null
    end as delivery_hours,
    
    -- Order to shipment timing
    case 
      when o.order_ts_utc is not null and s.shipped_at_utc is not null
      then date_diff('day', date(o.order_ts_utc), date(s.shipped_at_utc))
      else null
    end as days_to_ship,
    
    case 
      when o.order_ts_utc is not null and s.shipped_at_utc is not null
      then date_diff('hour', o.order_ts_utc, s.shipped_at_utc)
      else null
    end as hours_to_ship,
    
    -- Total order to delivery time
    case 
      when o.order_ts_utc is not null and s.delivered_at_utc is not null
      then date_diff('day', date(o.order_ts_utc), date(s.delivered_at_utc))
      else null
    end as total_fulfillment_days,
    
    -- Delivery status
    case 
      when s.delivered_at_utc is not null then 'Delivered'
      when s.shipped_at_utc is not null then 'In Transit'
      else 'Not Shipped'
    end as delivery_status,
    
    -- Performance categorization
    case 
      when s.delivered_at_utc is not null and s.shipped_at_utc is not null then
        case 
          when date_diff('day', date(s.shipped_at_utc), date(s.delivered_at_utc)) <= 1 then 'Same/Next Day'
          when date_diff('day', date(s.shipped_at_utc), date(s.delivered_at_utc)) <= 3 then 'Fast (2-3 days)'
          when date_diff('day', date(s.shipped_at_utc), date(s.delivered_at_utc)) <= 7 then 'Standard (4-7 days)'
          when date_diff('day', date(s.shipped_at_utc), date(s.delivered_at_utc)) <= 14 then 'Slow (8-14 days)'
          else 'Very Slow (>14 days)'
        end
      else 'Unknown'
    end as delivery_speed_category,
    
    -- Shipping performance
    case 
      when o.order_ts_utc is not null and s.shipped_at_utc is not null then
        case 
          when date_diff('day', date(o.order_ts_utc), date(s.shipped_at_utc)) = 0 then 'Same Day Ship'
          when date_diff('day', date(o.order_ts_utc), date(s.shipped_at_utc)) = 1 then 'Next Day Ship'
          when date_diff('day', date(o.order_ts_utc), date(s.shipped_at_utc)) <= 3 then 'Fast Ship (2-3 days)'
          when date_diff('day', date(o.order_ts_utc), date(s.shipped_at_utc)) <= 7 then 'Standard Ship (4-7 days)'
          else 'Slow Ship (>7 days)'
        end
      else 'Unknown'
    end as shipping_speed_category,
    
    -- Weekend/weekday patterns
    case
      when extract(dayofweek from s.shipped_at_utc) in (1, 7) then 'Weekend'
      else 'Weekday'
    end as shipped_day_type,
    
    case
      when s.delivered_at_utc is not null then
        case
          when extract(dayofweek from s.delivered_at_utc) in (1, 7) then 'Weekend'
          else 'Weekday'
        end
      else null
    end as delivered_day_type,
    
    -- Cost analysis
    case 
      when s.ship_cost is not null and o.shipping_fee is not null
      then s.ship_cost - o.shipping_fee
      else null
    end as shipping_cost_variance,
    
    case 
      when s.ship_cost is not null and o.order_final_total is not null and o.order_final_total > 0
      then round((s.ship_cost / o.order_final_total) * 100, 2)
      else null
    end as shipping_cost_percentage,
    
    -- Audit fields
    s.ingestion_ts,
    current_timestamp as dbt_updated_at
    
  from shipments_base s
  left join order_context o
    on s.order_id = o.order_id
    
  {% if is_incremental() %}
    -- Incremental filter: only process shipments with timestamps greater than current max
    where s.shipped_at_utc > (
      select coalesce(max(shipped_at), '1970-01-01'::timestamp) 
      from {{ this }}
    )
  {% endif %}
),

final as (
  select
    *,
    -- Additional business flags
    case 
      when delivery_days is not null and delivery_days <= 2 then true 
      else false 
    end as is_express_delivery,
    
    case 
      when days_to_ship is not null and days_to_ship = 0 then true 
      else false 
    end as is_same_day_ship,
    
    case 
      when delivered_at is null and date_diff('day', shipped_date, current_date) > 14 then true 
      else false 
    end as is_potentially_lost,
    
    case 
      when ship_cost is not null then
        case 
          when ship_cost >= 50 then 'High Cost Shipping'
          when ship_cost >= 20 then 'Medium Cost Shipping'
          when ship_cost > 0 then 'Low Cost Shipping'
          else 'Free Shipping'
        end
      else 'Unknown Cost'
    end as shipping_cost_tier
    
  from enriched
)

select * from final
"""Shipments data generator module."""
import random
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
from decimal import Decimal
import pyarrow as pa
import pyarrow.parquet as pq

from utils.data_utils import apply_scale_to_targets
from utils.constants import TARGET_ROWS


def generate_shipments_data(schema: pa.Schema, scale: float, output_path: Path) -> int:
    """Generate shipments data and write to Parquet file.
    
    Args:
        schema: PyArrow schema for shipments
        scale: Scaling factor for number of records
        output_path: Path to write the Parquet file
    """
    num_shipments = apply_scale_to_targets(TARGET_ROWS['shipments'], scale)
    
    # Define carriers with realistic market share and characteristics
    carriers = {
        'AUSPOST': {'weight': 0.40, 'cost_base': 8.99, 'cost_range': (4.99, 25.99), 'transit_days': (2, 7)},
        'DHL': {'weight': 0.15, 'cost_base': 15.99, 'cost_range': (12.99, 89.99), 'transit_days': (1, 4)},
        'FEDEX': {'weight': 0.15, 'cost_base': 14.99, 'cost_range': (9.99, 79.99), 'transit_days': (1, 5)},
        'UPS': {'weight': 0.10, 'cost_base': 13.99, 'cost_range': (8.99, 69.99), 'transit_days': (2, 6)},
        'TNT': {'weight': 0.08, 'cost_base': 16.99, 'cost_range': (11.99, 85.99), 'transit_days': (2, 5)},
        'TOLL': {'weight': 0.07, 'cost_base': 12.99, 'cost_range': (7.99, 45.99), 'transit_days': (3, 8)},
        'FASTWAY': {'weight': 0.05, 'cost_base': 9.99, 'cost_range': (6.99, 35.99), 'transit_days': (2, 9)},
    }
    
    # Calculate in-transit and late delivery anomaly counts
    in_transit_count = max(1, int(num_shipments * 0.075))  # 7.5% in-transit
    late_delivery_count = max(1, int(num_shipments * 0.025))  # 2.5% late deliveries
    
    # Generate base date range (last 6 months of shipping activity)
    base_date = datetime(2024, 1, 1)
    end_date = datetime(2024, 6, 30)
    date_range_days = (end_date - base_date).days
    
    # Pre-generate carrier assignments
    carrier_names = list(carriers.keys())
    carrier_weights = [carriers[name]['weight'] for name in carrier_names]
    selected_carriers = random.choices(carrier_names, weights=carrier_weights, k=num_shipments)
    
    # Generate shipment data
    shipment_ids = list(range(1, num_shipments + 1))
    order_ids = list(range(1, num_shipments + 1))  # 1:1 relationship for simplicity
    
    # Generate shipped dates
    shipped_dates = []
    for i in range(num_shipments):
        # Add some business day bias (less shipping on weekends)
        days_offset = random.randint(0, date_range_days)
        shipped_date = base_date + timedelta(days=days_offset)
        
        # Adjust time for business hours bias
        if shipped_date.weekday() < 5:  # Weekday
            hour = random.choices(range(24), weights=[0.5]*8 + [3]*8 + [1]*8)[0]  # Business hours weighted
        else:  # Weekend
            hour = random.choices(range(24), weights=[1]*24)[0]  # Less activity
            
        shipped_date = shipped_date.replace(
            hour=hour,
            minute=random.randint(0, 59),
            second=random.randint(0, 59),
            microsecond=random.randint(0, 999999)
        )
        shipped_dates.append(shipped_date)
    
    # Generate delivery dates and costs
    delivered_dates = []
    ship_costs = []
    
    # Mark which shipments will be in-transit (random selection)
    in_transit_indices = set(random.sample(range(num_shipments), in_transit_count))
    
    # Mark which delivered shipments will be late (excluding in-transit)
    available_for_late = [i for i in range(num_shipments) if i not in in_transit_indices]
    late_delivery_indices = set(random.sample(available_for_late, min(late_delivery_count, len(available_for_late))))
    
    for i in range(num_shipments):
        carrier_name = selected_carriers[i]
        carrier_info = carriers[carrier_name]
        shipped_at = shipped_dates[i]
        
        # Generate shipping cost using beta distribution for realistic spread
        cost_min, cost_max = carrier_info['cost_range']
        beta_sample = np.random.beta(2, 5)  # Skewed toward lower costs
        cost = cost_min + beta_sample * (cost_max - cost_min)
        
        # Add some premium/express shipping (higher cost, faster delivery)
        if random.random() < 0.15:  # 15% express shipping
            cost *= random.uniform(1.5, 2.5)
            transit_min, transit_max = carrier_info['transit_days']
            transit_days = max(1, transit_min - 1)  # Express is faster
        else:
            transit_min, transit_max = carrier_info['transit_days']
            transit_days = random.randint(transit_min, transit_max)
        
        ship_costs.append(Decimal(str(round(cost, 2))))
        
        # Handle delivery dates
        if i in in_transit_indices:
            # In-transit shipment - no delivery date
            delivered_dates.append(None)
        else:
            # Calculate delivery date
            if i in late_delivery_indices:
                # Late delivery - exceed SLA (add extra 2-10 days)
                extra_days = random.randint(2, 10)
                delivery_date = shipped_at + timedelta(days=transit_days + extra_days)
            else:
                # Normal delivery
                delivery_date = shipped_at + timedelta(days=transit_days)
            
            # Add some time variation for delivery (business hours for delivery)
            if delivery_date.weekday() < 5:  # Weekday delivery
                delivery_hour = random.choices(range(8, 18), weights=[1]*10)[0]  # Business hours
            else:  # Weekend delivery (some carriers)
                if random.random() < 0.3:  # 30% weekend delivery
                    delivery_hour = random.choices(range(9, 15), weights=[1]*6)[0]
                else:
                    # Push to next Monday
                    days_to_monday = 7 - delivery_date.weekday()
                    delivery_date += timedelta(days=days_to_monday)
                    delivery_hour = random.choices(range(8, 18), weights=[1]*10)[0]
            
            delivery_date = delivery_date.replace(
                hour=delivery_hour,
                minute=random.randint(0, 59),
                second=random.randint(0, 59),
                microsecond=random.randint(0, 999999)
            )
            delivered_dates.append(delivery_date)
    
    # Create PyArrow table
    table_data = {
        'shipment_id': pa.array(shipment_ids, type=pa.int64()),
        'order_id': pa.array(order_ids, type=pa.int64()),
        'carrier': pa.array(selected_carriers, type=pa.string()),
        'shipped_at': pa.array(shipped_dates, type=pa.timestamp('us')),
        'delivered_at': pa.array(delivered_dates, type=pa.timestamp('us')),
        'ship_cost': pa.array(ship_costs, type=pa.decimal128(12, 2)),
    }
    
    table = pa.table(table_data, schema=schema)
    
    # Ensure output directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Write to Parquet with compression
    pq.write_table(table, output_path, compression='snappy')
    
    return num_shipments

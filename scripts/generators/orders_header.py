"""Orders header data generator module."""
import csv
import random
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Dict, List
import pyarrow as pa

from utils.data_utils import (
    apply_scale_to_targets, 
    generate_date_range, 
    generate_business_hours_timestamp,
    inject_foreign_key_violations,
    create_partitioned_path,
    ensure_dir
)
from utils.schema_utils import get_column_names
from utils.constants import (
    TARGET_ROWS, 
    CHANNELS, 
    CHANNEL_WEIGHTS,
    PAYMENT_METHODS, 
    PAYMENT_WEIGHTS,
    CURRENCIES, 
    CURRENCY_WEIGHTS
)


def generate_orders_header_data(schema: pa.Schema, scale: float, output_path: Path, 
                               num_customers: int, num_stores: int) -> tuple[int, dict, date, int, list[date]]:
    """Generate orders header data with daily partitioning and write to CSV files.
    
    Args:
        schema: PyArrow schema for orders_header
        scale: Scaling factor for number of records
        output_path: Base path to write the partitioned CSV files
        num_customers: Number of customers available for foreign key references
        num_stores: Number of stores available for foreign key references
        
    Returns:
        tuple: (orders_count, orders_per_date, start_date, num_orders, order_dates)
    """
    num_orders = apply_scale_to_targets(TARGET_ROWS['orders_header'], scale)
    orders_header_columns = get_column_names(schema)
    
    # Date range for orders: last 12 months from today
    end_date = date.today()
    start_date = end_date - timedelta(days=365)
    order_dates = generate_date_range(start_date, end_date)
    
    # Distribute orders across dates with some variation (weekdays busier)
    orders_per_date: Dict[date, int] = {}
    remaining_orders = num_orders
    for order_date in order_dates:
        # Weight weekdays (Mon-Fri) higher than weekends
        if order_date.weekday() < 5:  # Monday = 0, Sunday = 6
            daily_weight = 1.4
        else:
            daily_weight = 0.8
        
        # Distribute orders with some randomness
        base_orders = max(1, int(num_orders / len(order_dates) * daily_weight))
        variation = int(base_orders * 0.3)  # ±30% variation
        daily_orders = max(1, base_orders + random.randint(-variation, variation))
        daily_orders = min(daily_orders, remaining_orders)
        
        orders_per_date[order_date] = daily_orders
        remaining_orders -= daily_orders
        
        if remaining_orders <= 0:
            break
    
    # Get valid foreign key ranges for realistic references (99%) and violations (1%)
    valid_customer_ids = list(range(1, num_customers + 1))
    valid_store_ids = list(range(1, num_stores + 1))
    
    order_id = 1
    duplicate_order_ids = set()  # Track duplicates for 0.05% anomaly
    total_orders_generated = 0
    
    for order_date, daily_orders in orders_per_date.items():
        if daily_orders == 0:
            continue
            
        # Create partitioned directory structure
        partition_path = create_partitioned_path(output_path / 'orders', ['order_dt'], [order_date.isoformat()])
        ensure_dir(partition_path)
        orders_header_file = partition_path / 'orders_header.csv'
        
        # Generate foreign keys with controlled violations
        daily_customer_ids = [random.choice(valid_customer_ids) for _ in range(daily_orders)]
        daily_store_ids = [random.choice(valid_store_ids) for _ in range(daily_orders)]
        
        # Inject 1% foreign key violations
        daily_customer_ids = inject_foreign_key_violations(daily_customer_ids, 0.01)
        daily_store_ids = inject_foreign_key_violations(daily_store_ids, 0.01)
        
        with orders_header_file.open('w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
            writer.writerow(orders_header_columns)
            
            for i in range(daily_orders):
                # Generate order timestamp with business hours weighting
                order_ts = generate_business_hours_timestamp(order_date)
                
                # 0.05% duplicate order_ids across partitions (anomaly)
                current_order_id = order_id
                if random.random() < 0.0005 and duplicate_order_ids:
                    current_order_id = random.choice(list(duplicate_order_ids))
                else:
                    duplicate_order_ids.add(order_id)
                
                # Channel selection affects other attributes
                channel = random.choices(CHANNELS, weights=CHANNEL_WEIGHTS)[0]
                payment_method = random.choices(PAYMENT_METHODS, weights=PAYMENT_WEIGHTS)[0]
                
                # Cash only available for POS channel
                if channel == 'pos' and payment_method == 'cash':
                    payment_method = 'cash'
                elif channel == 'web' and payment_method == 'cash':
                    # Web orders can't use cash, switch to credit card
                    payment_method = 'credit_card'
                
                # Coupon codes: 15% of orders have them
                coupon_code = ''
                if random.random() < 0.15:
                    coupon_code = f"SAVE{random.randint(5, 25)}"
                
                # Shipping fee logic: $0 for pickup/pos, $5-25 for delivery
                if channel == 'pos' or random.random() < 0.3:  # 30% pickup even for web
                    shipping_fee = Decimal('0.00')
                else:
                    shipping_fee = Decimal(f"{random.uniform(4.99, 24.99):.2f}")
                
                currency = random.choices(CURRENCIES, weights=CURRENCY_WEIGHTS)[0]
                
                # Write row using CSV writer for proper escaping
                writer.writerow([
                    current_order_id,                    # order_id
                    order_ts.isoformat(),               # order_ts
                    order_date.isoformat(),             # order_dt_local
                    daily_customer_ids[i],              # customer_id
                    daily_store_ids[i],                 # store_id
                    channel,                            # channel
                    payment_method,                     # payment_method
                    coupon_code,                        # coupon_code
                    f"{shipping_fee:.2f}",              # shipping_fee
                    currency                            # currency
                ])
                
                order_id += 1
                total_orders_generated += 1
    
    return total_orders_generated, orders_per_date, start_date, num_orders, order_dates

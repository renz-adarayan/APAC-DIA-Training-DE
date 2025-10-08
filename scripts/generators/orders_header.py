"""Orders header data generator module."""
import csv
import math
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
    CURRENCY_WEIGHTS,
)


def _read_ids_from_csv(file_path: Path, id_column: str) -> List[int]:
    """Read actual IDs from a CSV file.
    
    Args:
        file_path: Path to the CSV file
        id_column: Name of the ID column to read
        
    Returns:
        List of actual IDs from the file
    """
    ids = []
    with file_path.open('r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            ids.append(int(row[id_column]))
    
    return ids


def generate_orders_header_data(schema: pa.Schema, scale: float, output_path: Path, 
                               customers_file_path: Path, stores_file_path: Path,
                               start_date: date = None, end_date: date = None) -> tuple[int, dict, date, int, list[date]]:
    """Generate orders header data with daily partitioning and write to CSV files.
    
    Args:
        schema: PyArrow schema for orders_header
        scale: Scaling factor for number of records
        output_path: Base path to write the partitioned CSV files
        customers_file_path: Path to customers CSV file to read actual customer IDs
        stores_file_path: Path to stores CSV file to read actual store IDs
        start_date: Start date for data generation (defaults to 2024-01-01)
        end_date: End date for data generation (defaults to 2024-12-31)
        
    Returns:
        tuple: (orders_count, orders_per_date, start_date, num_orders, order_dates)
    """
    num_orders = apply_scale_to_targets(TARGET_ROWS['orders_header'], scale)
    orders_header_columns = get_column_names(schema)
    
    # Use provided dates or defaults
    if start_date is None:
        start_date = date(2024, 1, 1)
    if end_date is None:
        end_date = date(2024, 12, 31)
        
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
    
    # Read actual customer and store IDs from generated CSV files
    valid_customer_ids = _read_ids_from_csv(customers_file_path, 'customer_id')
    valid_store_ids = _read_ids_from_csv(stores_file_path, 'store_id')
    
    print(f"Loaded {len(valid_customer_ids)} customer IDs and {len(valid_store_ids)} store IDs from CSV files")
    
    # Pre-calculate FK violations for exactly 1% across entire dataset
    total_customer_violations = max(1, int(num_orders * 0.01))  # Exactly 1% of total orders
    total_store_violations = max(1, int(num_orders * 0.01))     # Exactly 1% of total orders
    
    # Distribute violations across days proportionally
    customer_violations_per_date = {}
    store_violations_per_date = {}
    remaining_customer_violations = total_customer_violations
    remaining_store_violations = total_store_violations
    
    for order_date in order_dates:
        if order_date not in orders_per_date or orders_per_date[order_date] == 0:
            customer_violations_per_date[order_date] = 0
            store_violations_per_date[order_date] = 0
            continue
            
        # Proportional allocation based on daily order count
        daily_orders = orders_per_date[order_date]
        proportion = daily_orders / num_orders
        
        # Customer violations for this day
        daily_customer_violations = int(total_customer_violations * proportion)
        if remaining_customer_violations > 0:
            daily_customer_violations = min(daily_customer_violations, remaining_customer_violations)
            customer_violations_per_date[order_date] = daily_customer_violations
            remaining_customer_violations -= daily_customer_violations
        else:
            customer_violations_per_date[order_date] = 0
            
        # Store violations for this day  
        daily_store_violations = int(total_store_violations * proportion)
        if remaining_store_violations > 0:
            daily_store_violations = min(daily_store_violations, remaining_store_violations)
            store_violations_per_date[order_date] = daily_store_violations
            remaining_store_violations -= daily_store_violations
        else:
            store_violations_per_date[order_date] = 0
    
    # Distribute any remaining violations to days with orders
    order_dates_with_orders = [d for d in order_dates if orders_per_date.get(d, 0) > 0]
    while remaining_customer_violations > 0 and order_dates_with_orders:
        date_to_add = random.choice(order_dates_with_orders)
        customer_violations_per_date[date_to_add] += 1
        remaining_customer_violations -= 1
        
    while remaining_store_violations > 0 and order_dates_with_orders:
        date_to_add = random.choice(order_dates_with_orders)
        store_violations_per_date[date_to_add] += 1
        remaining_store_violations -= 1
    
    print(f"Planned FK violations: {total_customer_violations} customer violations, {total_store_violations} store violations across {len(order_dates)} days")
    
    # Track order IDs for duplicate injection (0.05% exact rate)
    generated_order_ids: List[int] = []
    duplicate_injected_keys: List[int] = []  # records each duplicate occurrence
    
    # Calculate number of duplicates to inject (0.05% exact rate) - always round up
    num_duplicates: int = max(1, math.ceil(num_orders * 0.0005))
    dupes_created: int = 0  # how many duplicate rows emitted so far
    duplicated_keys = set()  # track which order IDs already received one duplicate
    
    order_id = 1
    total_orders_generated = 0
    
    for order_date, daily_orders in orders_per_date.items():
        if daily_orders == 0:
            continue
            
        # Create partitioned directory structure
        partition_path = create_partitioned_path(output_path / 'orders', ['order_dt'], [order_date.isoformat()])
        ensure_dir(partition_path)
        orders_header_file = partition_path / 'orders_header.csv'
        
        # Generate foreign keys with controlled violations using pre-calculated amounts
        daily_customer_ids = [random.choice(valid_customer_ids) for _ in range(daily_orders)]
        daily_store_ids = [random.choice(valid_store_ids) for _ in range(daily_orders)]
        
        # Apply exact number of violations for this day (instead of percentage-based)
        daily_customer_violations = customer_violations_per_date.get(order_date, 0)
        daily_store_violations = store_violations_per_date.get(order_date, 0)
        
        # Inject exact number of customer violations for this day
        if daily_customer_violations > 0:
            violation_indices = random.sample(range(len(daily_customer_ids)), 
                                            min(daily_customer_violations, len(daily_customer_ids)))
            max_customer_id = max(valid_customer_ids)
            for idx in violation_indices:
                # Generate invalid customer ID above the valid range
                daily_customer_ids[idx] = random.randint(max_customer_id + 1, 999999)
        
        # Inject exact number of store violations for this day  
        if daily_store_violations > 0:
            violation_indices = random.sample(range(len(daily_store_ids)), 
                                            min(daily_store_violations, len(daily_store_ids)))
            max_store_id = max(valid_store_ids)
            for idx in violation_indices:
                # Generate invalid store ID above the valid range
                daily_store_ids[idx] = random.randint(max_store_id + 1, 999999)
        
        with orders_header_file.open('w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
            writer.writerow(orders_header_columns)
            
            for i in range(daily_orders):
                # Generate order timestamp with business hours weighting
                order_ts = generate_business_hours_timestamp(order_date)
                
                # Sophisticated duplicate injection logic for order_ids
                if order_id == 1:
                    # Always start with a unique key
                    current_order_id = order_id
                    generated_order_ids.append(order_id)
                else:
                    duplicates_remaining = num_duplicates - dupes_created
                    orders_remaining = num_orders - order_id + 1
                    # We can inject if we still have duplicate quota AND there exists a key not yet duplicated
                    available_for_dup = [k for k in generated_order_ids if k not in duplicated_keys]
                    can_inject = duplicates_remaining > 0 and len(available_for_dup) > 0
                    # Must inject if we are running out of orders (ensure quota fulfillment)
                    must_inject = can_inject and orders_remaining == duplicates_remaining
                    # Probabilistic early injection to spread duplicates; adjust probability if needed
                    should_inject = can_inject and (must_inject or random.random() < 0.35)

                    if should_inject:
                        current_order_id = random.choice(available_for_dup)
                        duplicate_injected_keys.append(current_order_id)
                        duplicated_keys.add(current_order_id)
                        dupes_created += 1
                    else:
                        # Generate a new unique order ID
                        current_order_id = order_id
                        generated_order_ids.append(order_id)
                
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
    
    # Post-generation summary
    if duplicate_injected_keys:
        unique_dupes = sorted(set(duplicate_injected_keys))
        print(
            f"Summary: Injected {len(duplicate_injected_keys)} duplicate occurrences across "
            f"{len(unique_dupes)} unique order_ids: {', '.join(map(str, unique_dupes))}"
        )
    else:
        print("Summary: No duplicate order_ids were injected (unexpected).")
    
    # FK violations summary
    total_planned_customer_violations = sum(customer_violations_per_date.values())
    total_planned_store_violations = sum(store_violations_per_date.values())
    customer_violation_pct = (total_planned_customer_violations / total_orders_generated) * 100
    store_violation_pct = (total_planned_store_violations / total_orders_generated) * 100
    
    print(f"FK Violations Summary:")
    print(f"  Customer violations: {total_planned_customer_violations} ({customer_violation_pct:.2f}%)")
    print(f"  Store violations: {total_planned_store_violations} ({store_violation_pct:.2f}%)")

    return total_orders_generated, orders_per_date, start_date, num_orders, order_dates

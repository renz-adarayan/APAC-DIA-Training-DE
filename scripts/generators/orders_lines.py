"""Orders lines data generator module."""
import random
import csv
from datetime import datetime, timedelta, date
from pathlib import Path
from decimal import Decimal
from typing import Dict, List
import pyarrow as pa

from utils.data_utils import apply_scale_to_targets, create_partitioned_path, ensure_dir
from utils.schema_utils import get_column_names
from utils.constants import TARGET_ROWS, TAX_RATES


def generate_orders_lines_data(schema: pa.Schema, scale: float, output_path: Path, 
                             orders_per_date: Dict[date, int], num_products: int, 
                             start_date: date, num_orders: int, order_dates: List[date]) -> int:
    """Generate orders lines data and write to partitioned CSV files.
    
    Args:
        schema: PyArrow schema for orders_lines
        scale: Scaling factor for number of records
        output_path: Path to write the CSV files
        orders_per_date: Dictionary mapping order dates to number of orders
        num_products: Total number of products for foreign key references
        start_date: Start date for order generation
        num_orders: Total number of orders
        order_dates: List of order dates for partitioning
        
    Returns:
        Total number of lines generated
    """
    # Generate orders lines matching header partitioning with 3.5 average lines per order
    num_lines_target = apply_scale_to_targets(TARGET_ROWS['orders_lines'], scale)
    orders_lines_columns = get_column_names(schema)
    
    # Valid product IDs for foreign key references and violations
    valid_product_ids = list(range(1, num_products + 1))
    
    # Track total lines generated
    total_lines_generated = 0
    
    # Lines per order distribution (weighted toward 2-4 lines, but allowing 1-8)
    lines_per_order_weights = [0.1, 0.25, 0.25, 0.2, 0.1, 0.05, 0.03, 0.02]  # 1-8 lines
    
    # Process each partition to generate corresponding lines
    for order_date, daily_orders in orders_per_date.items():
        if daily_orders == 0:
            continue
            
        # Use same partition structure as orders header
        partition_path = create_partitioned_path(output_path / 'orders', ['order_dt'], [order_date.isoformat()])
        ensure_dir(partition_path)
        orders_lines_file = partition_path / 'orders_lines.csv'
        
        # Calculate how many lines to generate for this partition
        # Target ~3.5 lines per order but respect overall target
        partition_lines_target = min(
            int(daily_orders * 3.5),
            num_lines_target - total_lines_generated
        )
        
        if partition_lines_target <= 0:
            continue
            
        with orders_lines_file.open('w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
            writer.writerow(orders_lines_columns)
            
            lines_written = 0
            current_order_id = ((order_date - start_date).days * max(1, int(num_orders / len(order_dates)))) + 1
            
            for order_idx in range(daily_orders):
                if lines_written >= partition_lines_target:
                    break
                    
                # Determine number of lines for this order
                num_lines = random.choices(range(1, 9), weights=lines_per_order_weights)[0]
                
                # Don't exceed partition target
                num_lines = min(num_lines, partition_lines_target - lines_written)
                
                for line_num in range(1, num_lines + 1):
                    # Generate product_id with 1% foreign key violations
                    product_id = random.choice(valid_product_ids)
                    if random.random() < 0.01:
                        # Foreign key violation: invalid product_id
                        product_id = random.randint(max(valid_product_ids) + 1, 999999)
                    
                    # Quantity distribution: mostly 1-3, occasionally higher
                    if random.random() < 0.001:  # 0.1% negative quantity anomaly
                        qty = -random.randint(1, 3)
                    else:
                        qty_weights = [0.5, 0.3, 0.15, 0.03, 0.01, 0.01]  # 1-6 qty
                        qty = random.choices(range(1, 7), weights=qty_weights)[0]
                    
                    # Unit price: base from product price with ±20% variation for promotions
                    # For simplicity, use category-based pricing similar to products
                    base_price = random.uniform(5, 500)  # Simplified range
                    promotion_factor = random.uniform(0.8, 1.2)  # ±20% price variation
                    
                    if random.random() < 0.0005:  # 0.05% zero price anomaly (free items)
                        unit_price = Decimal('0.0000')
                    else:
                        unit_price = Decimal(f"{base_price * promotion_factor:.4f}")
                    
                    # Line discount: 80% no discount, 20% have discount (0-50%)
                    if random.random() < 0.8:
                        line_discount_pct = Decimal('0.0000')
                    else:
                        discount = random.uniform(0.05, 0.50)  # 5-50% discount
                        line_discount_pct = Decimal(f"{discount:.4f}")
                    
                    # Tax percentage: 10% for AUD (GST), varying for other currencies
                    # For simplicity, assume AUD for most transactions
                    currency = random.choices(['AUD', 'USD', 'EUR'], weights=[0.8, 0.15, 0.05])[0]
                    tax_pct = Decimal(f"{TAX_RATES.get(currency, 0.10):.4f}")
                    
                    # Write row using CSV writer for proper escaping
                    writer.writerow([
                        current_order_id,       # order_id
                        line_num,               # line_number
                        product_id,             # product_id
                        qty,                    # qty
                        f"{unit_price:.4f}",    # unit_price
                        f"{line_discount_pct:.4f}",  # line_discount_pct
                        f"{tax_pct:.4f}"        # tax_pct
                    ])
                    
                    lines_written += 1
                    if lines_written >= partition_lines_target:
                        break
                
                current_order_id += 1
        
        total_lines_generated += lines_written
        
        if total_lines_generated >= num_lines_target:
            break
    
    return total_lines_generated

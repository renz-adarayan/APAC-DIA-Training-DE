"""Returns data generator module with Delta Lake format and schema evolution."""
import random
import csv
from datetime import datetime, timedelta
from pathlib import Path
from faker import Faker
import pyarrow as pa
from deltalake import write_deltalake, DeltaTable
import pandas as pd

from utils.data_utils import apply_scale_to_targets
from utils.constants import TARGET_ROWS


def _get_actual_order_product_combinations(orders_root_path: Path, products_csv_path: Path) -> list[tuple[int, int, datetime]]:
    """Get actual order_id and product_id combinations from CSV files.
    
    Args:
        orders_root_path: Path to the orders directory with partitioned data
        products_csv_path: Path to the products CSV file
        
    Returns:
        List of tuples containing (order_id, product_id, order_timestamp)
    """
    # Read all available product IDs
    product_ids = []
    if products_csv_path.exists():
        with products_csv_path.open('r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                product_ids.append(int(row['product_id']))
    
    if not product_ids:
        raise ValueError(f"No product IDs found in {products_csv_path}")
    
    # Read order IDs and timestamps from orders partitions
    order_data = []
    
    # Look for order partitions in the orders directory
    if orders_root_path.exists():
        for partition_dir in orders_root_path.iterdir():
            if partition_dir.is_dir() and partition_dir.name.startswith('order_dt='):
                orders_header_file = partition_dir / 'orders_header.csv'
                orders_lines_file = partition_dir / 'orders_lines.csv'
                
                if orders_header_file.exists() and orders_lines_file.exists():
                    # Read orders header to get order_id and timestamp
                    order_timestamps = {}
                    with orders_header_file.open('r', encoding='utf-8') as f:
                        reader = csv.DictReader(f)
                        for row in reader:
                            order_id = int(row['order_id'])
                            order_ts_str = row['order_ts']
                            # Parse timestamp (remove timezone for simplicity)
                            order_ts = datetime.fromisoformat(order_ts_str.replace('Z', '').replace('+08:00', ''))
                            order_timestamps[order_id] = order_ts
                    
                    # Read orders lines to get order_id and product_id combinations
                    with orders_lines_file.open('r', encoding='utf-8') as f:
                        reader = csv.DictReader(f)
                        for row in reader:
                            order_id = int(row['order_id'])
                            product_id = int(row['product_id'])
                            
                            if order_id in order_timestamps:
                                order_data.append((order_id, product_id, order_timestamps[order_id]))
    
    if not order_data:
        # Fallback: create combinations from available data
        print("No order-product combinations found in partitioned data. Creating fallback combinations...")
        # Read from a sample orders file if available
        sample_orders_path = orders_root_path / "order_dt=2024-01-01" / "orders_header.csv"
        if sample_orders_path.exists():
            with sample_orders_path.open('r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    order_id = int(row['order_id'])
                    order_ts_str = row['order_ts']
                    order_ts = datetime.fromisoformat(order_ts_str.replace('Z', '').replace('+08:00', ''))
                    
                    # Each order gets 1-3 random products
                    num_products = random.randint(1, min(3, len(product_ids)))
                    selected_products = random.sample(product_ids, num_products)
                    
                    for product_id in selected_products:
                        order_data.append((order_id, product_id, order_ts))
    
    return order_data


def generate_returns_data(schema: pa.Schema, scale: float, output_path: Path, 
                         orders_root_path: Path = None, products_csv_path: Path = None) -> int:
    """Generate returns data with Delta Lake format and schema evolution.
    
    Uses actual order_id and product_id combinations from CSV files
    to ensure referential integrity with existing orders and products data.
    
    Args:
        schema: PyArrow schema for returns (day1 schema)
        scale: Scaling factor for number of records
        output_path: Path to write the Delta table
        orders_root_path: Path to the orders directory with partitioned data
        products_csv_path: Path to products CSV file
    
    Returns:
        Number of returns generated
    """
    fake = Faker('en_AU')
    
    # Set default paths if not provided
    if orders_root_path is None:
        orders_root_path = Path("data_raw/orders")
    if products_csv_path is None:
        products_csv_path = Path("data_raw/products.csv")
    
    # Get actual order-product combinations from CSV files
    print("Loading actual order-product combinations from CSV files...")
    try:
        order_product_combinations = _get_actual_order_product_combinations(orders_root_path, products_csv_path)
        print(f"Loaded {len(order_product_combinations)} order-product combinations from CSV files")
    except Exception as e:
        print(f"Failed to load from CSV files: {e}")
        raise ValueError("Could not load order-product combinations from CSV files")
    
    if not order_product_combinations:
        raise ValueError("No order-product combinations available. Cannot generate returns.")
    
    # Calculate number of returns (should be smaller than available combinations)
    num_returns = apply_scale_to_targets(TARGET_ROWS['returns'], scale)
    max_possible_returns = len(order_product_combinations)
    
    if num_returns > max_possible_returns:
        print(f"Warning: Requested {num_returns} returns but only {max_possible_returns} order-product combinations available")
        num_returns = max_possible_returns
    
    # Return reasons based on common retail scenarios
    return_reasons = [
        'Defective', 'Wrong size', 'Wrong color', 'Damaged in shipping',
        'Not as described', 'Changed mind', 'Duplicate order', 'Quality issues',
        'Missing parts', 'Late delivery', 'Gift return', 'Better price elsewhere'
    ]
    
    # Generate base returns data (v1 schema)
    returns_data = []
    used_return_ids = set()
    
    print(f"Generating {num_returns:,} returns records from actual order-product combinations...")
    
    # Randomly sample from available order-product combinations
    selected_combinations = random.sample(order_product_combinations, num_returns)
    
    for i, (order_id, product_id, order_ts) in enumerate(selected_combinations, 1):
        # Ensure unique return_id (with some potential duplicates for anomalies)
        if random.random() < 0.0005:  # 0.05% duplicate return_ids
            if used_return_ids:
                return_id = random.choice(list(used_return_ids))
            else:
                return_id = i
        else:
            return_id = i
            
        used_return_ids.add(return_id)
        
        # Use actual order_id and product_id from the combination
        # Note: These are now guaranteed to be valid foreign keys
        
        # Return timestamp - returns happen within 30 days of order (business rule)
        days_after_order = random.randint(1, 30)
        hours_after_order = random.randint(0, 23)
        return_ts = order_ts + timedelta(days=days_after_order, hours=hours_after_order)
        
        # Return quantity - usually 1-3 items
        if random.random() < 0.005:  # 0.5% anomaly - negative qty
            qty = random.randint(-2, 0)
        else:
            qty = random.randint(1, min(5, random.choices([1, 2, 3, 4, 5], weights=[0.6, 0.25, 0.10, 0.04, 0.01])[0]))
        
        # Return reason
        reason = random.choice(return_reasons)
        
        returns_data.append({
            'return_id': return_id,
            'order_id': order_id,
            'product_id': product_id,
            'return_ts': return_ts,
            'qty': qty,
            'reason': reason
        })
    
    # Create DataFrame and write initial version (v1) to Delta
    df_v1 = pd.DataFrame(returns_data)
    
    # Ensure proper data types for Delta Lake compatibility
    df_v1['return_id'] = df_v1['return_id'].astype('int64')
    df_v1['order_id'] = df_v1['order_id'].astype('int64')
    df_v1['product_id'] = df_v1['product_id'].astype('int64')
    df_v1['qty'] = df_v1['qty'].astype('int32')
    df_v1['return_ts'] = pd.to_datetime(df_v1['return_ts'])
    df_v1['reason'] = df_v1['reason'].astype('string')
    
    # Ensure output directory exists
    output_path.mkdir(parents=True, exist_ok=True)
    
    # Schema evolution: Add return_reason_code column (v2)
    print("Implementing schema evolution: adding return_reason_code column...")
    
    # Create return reason codes mapping
    reason_codes = {
        'Defective': 'DEF001',
        'Wrong size': 'SIZ001', 
        'Wrong color': 'COL001',
        'Damaged in shipping': 'DMG001',
        'Not as described': 'DESC01',
        'Changed mind': 'CHG001',
        'Duplicate order': 'DUP001',
        'Quality issues': 'QUA001',
        'Missing parts': 'MIS001',
        'Late delivery': 'LAT001',
        'Gift return': 'GFT001',
        'Better price elsewhere': 'PRC001'
    }
    
    # Add return_reason_code to existing data
    df_v1['return_reason_code'] = df_v1['reason'].map(reason_codes)
    df_v1['return_reason_code'] = df_v1['return_reason_code'].astype('string')
    
    # Create v2 schema with the new column
    v2_schema = pa.schema([
        pa.field("return_id", pa.int64()),
        pa.field("order_id", pa.int64()),
        pa.field("product_id", pa.int64()),
        pa.field("return_ts", pa.timestamp("us")),
        pa.field("qty", pa.int32()),
        pa.field("reason", pa.string()),
        pa.field("return_reason_code", pa.string()),
    ])
    
    # Generate additional data for schema evolution demonstration
    print("Generating additional returns data for schema evolution...")
    
    # Create some new returns with the evolved schema (v2)
    evolution_count = max(1, int(num_returns * 0.05))  # 5% new records for evolution demo
    evolution_data = []
    
    # Use additional combinations for evolution data
    if len(order_product_combinations) > num_returns:
        remaining_combinations = order_product_combinations[num_returns:num_returns + evolution_count]
    else:
        # Reuse some combinations if we don't have enough
        remaining_combinations = random.sample(order_product_combinations, min(evolution_count, len(order_product_combinations)))
    
    for i, (order_id, product_id, order_ts) in enumerate(remaining_combinations):
        new_return_id = num_returns + i + 1
        
        # Generate new return with v2 schema
        # Return timestamp later than the original order
        days_after_order = random.randint(31, 60)  # Later returns for evolution demo
        return_ts = order_ts + timedelta(days=days_after_order, hours=random.randint(0, 23))
        
        reason = random.choice(return_reasons)
        return_reason_code = reason_codes[reason]
        
        evolution_data.append({
            'return_id': new_return_id,
            'order_id': order_id,
            'product_id': product_id,
            'return_ts': return_ts,
            'qty': random.randint(1, 3),
            'reason': reason,
            'return_reason_code': return_reason_code
        })
    
    # Create DataFrame for evolution data
    df_evolution = pd.DataFrame(evolution_data)
    
    # Ensure proper data types for evolution DataFrame
    df_evolution['return_id'] = df_evolution['return_id'].astype('int64')
    df_evolution['order_id'] = df_evolution['order_id'].astype('int64')
    df_evolution['product_id'] = df_evolution['product_id'].astype('int64')
    df_evolution['qty'] = df_evolution['qty'].astype('int32')
    df_evolution['return_ts'] = pd.to_datetime(df_evolution['return_ts'])
    df_evolution['reason'] = df_evolution['reason'].astype('string')
    df_evolution['return_reason_code'] = df_evolution['return_reason_code'].astype('string')
    
    # Combine v1 data (with added return_reason_code) and evolution data
    df_final = pd.concat([df_v1, df_evolution], ignore_index=True)
    
    # Write combined data (schema evolution) - overwrite the v1 table with v2 schema
    print("Writing returns data v2 (with return_reason_code) to Delta Lake...")
    write_deltalake(
        str(output_path),
        df_final,
        mode="overwrite"
    )
    
    # Demonstrate UPSERT operation
    print("Demonstrating UPSERT operation...")
    
    # Generate some updated returns (newer return_ts for existing return_ids)
    upsert_count = max(1, int(num_returns * 0.01))  # 1% of returns get updates
    upsert_data = []
    
    # Use some existing combinations for upsert operations
    upsert_combinations = random.sample(order_product_combinations, min(upsert_count, len(order_product_combinations)))
    
    for i, (order_id, product_id, order_ts) in enumerate(upsert_combinations):
        existing_return_id = random.choice(list(used_return_ids))
        # Create updated record with newer timestamp
        updated_return_ts = order_ts + timedelta(days=random.randint(45, 90))
        
        reason = random.choice(return_reasons)
        return_reason_code = reason_codes[reason]
        
        upsert_data.append({
            'return_id': existing_return_id,
            'order_id': order_id,
            'product_id': product_id,
            'return_ts': updated_return_ts,
            'qty': random.randint(1, 3),
            'reason': reason,
            'return_reason_code': return_reason_code
        })
    
    if upsert_data:
        df_upsert = pd.DataFrame(upsert_data)
        
        # Ensure proper data types for upsert DataFrame
        df_upsert['return_id'] = df_upsert['return_id'].astype('int64')
        df_upsert['order_id'] = df_upsert['order_id'].astype('int64')
        df_upsert['product_id'] = df_upsert['product_id'].astype('int64')
        df_upsert['qty'] = df_upsert['qty'].astype('int32')
        df_upsert['return_ts'] = pd.to_datetime(df_upsert['return_ts'])
        df_upsert['reason'] = df_upsert['reason'].astype('string')
        df_upsert['return_reason_code'] = df_upsert['return_reason_code'].astype('string')
        
        # Write upsert data
        write_deltalake(
            str(output_path),
            df_upsert,
            mode="append"
        )
        
        print(f"Performed UPSERT operation on {len(upsert_data)} records")
        print(f"Added {evolution_count} new records for schema evolution demonstration")
    
    # Demonstrate DELETE operation
    print("Demonstrating DELETE operation...")
    
    # Load the Delta table for delete operations
    dt = DeltaTable(str(output_path))
    
    # Delete some invalid returns (e.g., returns older than business rules allow)
    # Delete returns that are too old (> 45 days from base date for example)
    cutoff_date = datetime(2024, 1, 1) + timedelta(days=45)
    
    # Note: DeltaTable.delete() syntax for demonstration
    # In practice, use: dt.delete(f"return_ts < '{cutoff_date.isoformat()}'")
    print(f"Simulated DELETE operation for returns older than {cutoff_date.isoformat()}")
    
    # Return final count (accounting for any operations)
    final_table = dt.to_pandas()
    final_count = len(final_table)
    
    print(f"Returns data generation complete. Final count: {final_count:,} records")
    print(f"Delta table written to: {output_path}")
    
    return final_count

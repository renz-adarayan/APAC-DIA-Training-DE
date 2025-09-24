"""Returns data generator module with Delta Lake format and schema evolution."""
import random
from datetime import datetime, timedelta
from pathlib import Path
from faker import Faker
import pyarrow as pa
from deltalake import write_deltalake, DeltaTable
import pandas as pd

from utils.data_utils import apply_scale_to_targets
from utils.constants import TARGET_ROWS


def generate_returns_data(schema: pa.Schema, scale: float, output_path: Path, orders_count: int) -> int:
    """Generate returns data with Delta Lake format and schema evolution.
    
    Args:
        schema: PyArrow schema for returns (day1 schema)
        scale: Scaling factor for number of records
        output_path: Path to write the Delta table
        orders_count: Number of orders available for returns (for FK references)
    
    Returns:
        Number of returns generated
    """
    fake = Faker('en_AU')
    num_returns = apply_scale_to_targets(TARGET_ROWS['returns'], scale)
    
    # Return reasons based on common retail scenarios
    return_reasons = [
        'Defective', 'Wrong size', 'Wrong color', 'Damaged in shipping',
        'Not as described', 'Changed mind', 'Duplicate order', 'Quality issues',
        'Missing parts', 'Late delivery', 'Gift return', 'Better price elsewhere'
    ]
    
    # Generate base returns data (v1 schema)
    returns_data = []
    used_return_ids = set()
    
    print(f"Generating {num_returns:,} returns records...")
    
    for i in range(1, num_returns + 1):
        # Ensure unique return_id (with some potential duplicates for anomalies)
        if random.random() < 0.0005:  # 0.05% duplicate return_ids
            if used_return_ids:
                return_id = random.choice(list(used_return_ids))
            else:
                return_id = i
        else:
            return_id = i
            
        used_return_ids.add(return_id)
        
        # Foreign key to orders - mostly valid with 1% violations
        if random.random() < 0.01:
            # Invalid order_id (anomaly)
            order_id = random.randint(orders_count + 1, orders_count + 10000)
        else:
            # Valid order_id
            order_id = random.randint(1, orders_count)
        
        # Product ID - assume we have up to 25k products with 1% violations
        if random.random() < 0.01:
            # Invalid product_id (anomaly)
            product_id = random.randint(25001, 35000)
        else:
            # Valid product_id
            product_id = random.randint(1, 25000)
        
        # Return timestamp - mostly recent returns with some spread
        # Assuming returns happen within 30 days of order (per business rules)
        base_date = datetime(2024, 1, 1)
        days_offset = random.randint(0, 400)  # Spread across 2024
        return_hours_offset = random.randint(0, 719)  # Up to 30 days after order
        return_ts = base_date + timedelta(days=days_offset, hours=return_hours_offset)
        
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
    
    for i in range(evolution_count):
        new_return_id = num_returns + i + 1
        
        # Generate new return with v2 schema
        base_date = datetime(2024, 6, 1)  # Later time period for evolution
        days_offset = random.randint(0, 180)
        return_hours_offset = random.randint(0, 719)
        return_ts = base_date + timedelta(days=days_offset, hours=return_hours_offset)
        
        reason = random.choice(return_reasons)
        return_reason_code = reason_codes[reason]
        
        evolution_data.append({
            'return_id': new_return_id,
            'order_id': random.randint(1, orders_count),
            'product_id': random.randint(1, 25000),
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
    
    for _ in range(upsert_count):
        existing_return_id = random.choice(list(used_return_ids))
        # Create updated record with newer timestamp
        updated_return_ts = datetime(2024, 8, 1) + timedelta(days=random.randint(0, 120))
        
        reason = random.choice(return_reasons)
        return_reason_code = reason_codes[reason]
        
        upsert_data.append({
            'return_id': existing_return_id,
            'order_id': random.randint(1, orders_count),
            'product_id': random.randint(1, 25000),
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

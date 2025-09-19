# Generate synthetic raw data locally with controlled edge cases.
# Usage: python scripts/generate_data.py --seed 42 --out data_raw
import argparse, os, pathlib, random, sys
from datetime import datetime, timedelta, date
from decimal import Decimal
import numpy as np
from faker import Faker
from mimesis import Person, Address
import rstr
import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq
import xlsxwriter
import string

# --- Package import bootstrap -------------------------------------------------
# Ensure project root is on sys.path so that `schemas` package resolves even if
# the script is invoked directly via `python scripts/generate_data.py`.
PROJECT_ROOT = pathlib.Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

try:
    from schemas import (
        customers_schema, products_schema, stores_schema, suppliers_schema,
        orders_header_schema, orders_lines_schema, events_schema, sensors_schema,
        exchange_rates_schema, shipments_schema, returns_day1_schema,
    )
except ModuleNotFoundError as e:
    # Provide clearer diagnostic information before failing hard.
    raise ModuleNotFoundError(
        "Failed to import 'schemas'. Confirm that 'schemas/__init__.py' exists and that you are running the script from the project root. "
        f"sys.path (first 5 entries): {sys.path[:5]} | Project root: {PROJECT_ROOT}"
    ) from e

TARGET_ROWS = {
    'customers': 80_000,
    'products': 25_000, 
    'stores': 5_000,
    'suppliers': 8_000,
    'orders_header': 1_000_000,
    'orders_lines': 3_500_000,  # 3-4M average
    'events': 2_000_000,
    'sensors': 7_500_000,  # 5-10M range
    'exchange_rates': 1_100,  # ~3 years daily
    'shipments': 1_000_000,
    'returns': 100_000
}

# Schema utility functions
def get_schema_columns(schema):
    """Extract column names and types from PyArrow schema"""
    return [(field.name, field.type) for field in schema]

def get_column_names(schema):
    """Get just the column names from a schema"""
    return [field.name for field in schema]

def apply_scale_to_targets(base_count, scale):
    """Apply scaling factor to target row counts"""
    return max(1, int(base_count * scale))

def validate_data_against_schema(data_dict, schema):
    """Validate that generated data matches schema expectations"""
    try:
        table = pa.table(data_dict)
        casted = table.cast(schema)
        return True, None
    except Exception as e:
        return False, str(e)

def parse_args():
    ap = argparse.ArgumentParser(
        description='Generate synthetic retail data with controlled anomalies',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    ap.add_argument('--seed', type=int, default=42,
                    help='Random seed for reproducible data generation')
    ap.add_argument('--out', type=str, default='data_raw',
                    help='Output directory for generated data')
    ap.add_argument('--scale', type=float, default=1.0,
                    help='Scaling factor for data volumes (e.g., 0.01 for 1%% of target rows)')
    return ap.parse_args()

def ensure_dir(p): pathlib.Path(p).mkdir(parents=True, exist_ok=True)

def main():
    args = parse_args()
    random.seed(args.seed); np.random.seed(args.seed)
    out = pathlib.Path(args.out); ensure_dir(out)

    # Minimal sample generation (expand to full volumes per docs)
    ######## 1.CUSTOMERS ########
    fake = Faker('en_AU')
    num_customers = apply_scale_to_targets(TARGET_ROWS['customers'], args.scale)
    customers_path = out/'customers.csv'
    column_names = get_column_names(customers_schema)
    
    with customers_path.open('w', encoding='utf-8') as f:
        f.write(','.join(column_names) + '\n')
        for i in range(1, num_customers + 1):
            # Natural key must follow pattern CUST-[A-Z0-9]{8}. Using explicit uppercase alphanumerics
            characters = string.ascii_uppercase + string.digits
            random_suffix = ''.join(random.choices(characters, k=8))
            nk = f"CUST-{random_suffix}"
            # Inject anomalies: 0.5-1% malformed emails
            email = fake.email() if random.random() > 0.008 else 'bad_email'
            lat = -44 + random.random()*10; lon = 112 + random.random()*40
            birth = date(1960,1,1) + timedelta(days=random.randint(0, 20000))
            join_ts = datetime(2024,1,1) + timedelta(days=random.randint(0, 400), seconds=random.randint(0, 86399))
            f.write(f"{i},{nk},{fake.first_name()},{fake.last_name()},{email},{fake.phone_number().replace(',',' ')},{fake.street_address().replace(',',' ')},,{fake.city().replace(',',' ')},{fake.state_abbr()},{fake.postcode()},AU,{lat:.6f},{lon:.6f},{birth.isoformat()},{join_ts.isoformat()},{str(random.random()<0.15)},{str(random.random()>0.05)}\n")

    ######## 2.PRODUCTS ########
    ######## 3.STORES ########
    ######## 4.SUPPLIERS ########
    ######## 5.ORDERS HEADER ########
    ######## 6.ORDERS LINES ########
    ######## 7.EVENTS ########
    ######## 8.SENSORS ########
    ######## 9.EXCHANGE RATES ########
    ######## 10.SHIPMENTS ########
    # Shipments parquet sample with schema integration and weighted random shipping costs
    num_shipments = apply_scale_to_targets(TARGET_ROWS['shipments'], args.scale)
    
    # Generate shipping costs with beta distribution (weighted toward lower costs)
    weights = np.random.beta(2, 5, num_shipments)  # Shape parameters favor lower values
    costs = 4.99 + weights * (49.99 - 4.99)  # Scale to $4.99-$49.99 range
    
    tbl = pa.table({
        'shipment_id': pa.array(range(1, num_shipments + 1), type=pa.int64()),
        'order_id': pa.array(range(1, num_shipments + 1), type=pa.int64()),
        'carrier': pa.array(['AUSPOST'] * num_shipments, type=pa.string()),
        'shipped_at': pa.array([datetime(2024,1,1)+timedelta(days=i%90) for i in range(num_shipments)], type=pa.timestamp('us')),
        'delivered_at': pa.array([datetime(2024,1,2)+timedelta(days=i%90) for i in range(num_shipments)], type=pa.timestamp('us')),
        'ship_cost': pa.array([Decimal(f"{cost:.2f}") for cost in costs], type=pa.decimal128(12,2)),
    })
    pq.write_table(tbl, out/'shipments.parquet', compression='snappy')


    # Schema validation for generated data
    print(f"🔍 Validating generated data against schemas...")
    
    # Validate customers data
    try:
        customers_table = pacsv.read_csv(customers_path)
        is_valid, error = validate_data_against_schema(customers_table.to_pydict(), customers_schema)
        if is_valid:
            print(f"✅ Customers data ({num_customers:,} rows) validates against schema")
        else:
            print(f"❌ Customers validation failed: {error}")
    except Exception as e:
        print(f"❌ Customers validation error: {e}")
    
    # Validate shipments data
    try:
        shipments_table = pq.read_table(out/'shipments.parquet')
        is_valid, error = validate_data_against_schema(shipments_table.to_pydict(), shipments_schema)
        if is_valid:
            print(f"✅ Shipments data ({num_shipments:,} rows) validates against schema")
        else:
            print(f"❌ Shipments validation failed: {error}")
    except Exception as e:
        print(f"❌ Shipments validation error: {e}")
    
    print(f"✅ Sample raw written to {out}. Expand to required volumes per /docs.")
if __name__ == '__main__':
    main()

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

def validate_csv(path: pathlib.Path, schema: pa.Schema):
    """Read a CSV file at path and validate against provided PyArrow schema.

    Returns (is_valid, error_message, row_count).
    """
    try:
        table = pacsv.read_csv(path)
        is_valid, error = validate_data_against_schema(table.to_pydict(), schema)
        return is_valid, error, table.num_rows
    except Exception as e:
        return False, str(e), 0

def validate_parquet(path: pathlib.Path, schema: pa.Schema):
    """Read a Parquet file at path and validate against provided PyArrow schema.

    Returns (is_valid, error_message, row_count).
    """
    try:
        table = pq.read_table(path)
        is_valid, error = validate_data_against_schema(table.to_pydict(), schema)
        return is_valid, error, table.num_rows
    except Exception as e:
        return False, str(e), 0

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
    ap.add_argument('--validate', action='store_true', 
                    help='Run schema validations after data generation (off by default)')
    return ap.parse_args()

def ensure_dir(p): pathlib.Path(p).mkdir(parents=True, exist_ok=True)

def main():
    args = parse_args()
    random.seed(args.seed); np.random.seed(args.seed)
    out = pathlib.Path(args.out); ensure_dir(out)

    # Centralize all raw output paths
    paths = {
        'customers': out / 'customers.csv',
        'products': out / 'products.csv',
        'stores': out / 'stores.csv',
        'suppliers': out / 'suppliers.csv',
        'shipments': out / 'shipments.parquet',
        # Future datasets placeholders (uncomment/extend when implemented):
        # 'orders_header': out / 'orders_header.csv',
        # 'orders_lines': out / 'orders_lines.csv',
        # 'events': out / 'events.csv',
        # 'sensors': out / 'sensors.csv',
        # 'exchange_rates': out / 'exchange_rates.csv',
        # 'returns': out / 'returns_day1.csv',
    }

    # Minimal sample generation (expand to full volumes per docs)
    ######## 1.CUSTOMERS ########
    fake = Faker('en_AU')
    num_customers = apply_scale_to_targets(TARGET_ROWS['customers'], args.scale)
    customers_path = paths['customers']
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
    products_path = paths['products']
    num_products = apply_scale_to_targets(TARGET_ROWS['products'], args.scale)
    product_cols = get_column_names(products_schema)
    ensure_dir(products_path.parent)

    # Category/Subcategory pools (simple hierarchy)
    category_hierarchy = {
        'Electronics': ['Phones', 'Laptops', 'Audio', 'Gaming'],
        'Home': ['Kitchen', 'Furniture', 'Decor'],
        'Apparel': ['Mens', 'Womens', 'Kids'],
        'Sports': ['Outdoor', 'Fitness', 'Team'],
        'Beauty': ['Skincare', 'Makeup', 'Hair']
    }
    categories = list(category_hierarchy.keys())
    characters = string.ascii_uppercase + string.digits

    price_anomaly_rate = 0.005
    num_price_anomalies = max(1, int(num_products * price_anomaly_rate))
    anomaly_indices = set(random.sample(range(1, num_products + 1), num_price_anomalies))

    with products_path.open('w', encoding='utf-8') as f:
        f.write(','.join(product_cols) + '\n')
        for pid in range(1, num_products + 1):
            sku = 'SKU-' + ''.join(random.choices(characters, k=6))
            cat = random.choice(categories)
            subcat = random.choice(category_hierarchy[cat])
            name = f"{cat} {subcat} Item {pid}"
            introduced = date.today() - timedelta(days=random.randint(0, 365 * 5))
            # 10% discontinued products
            is_disc = random.random() < 0.10
            if is_disc:
                # 20% of discontinued missing discontinued_dt (anomaly for business rule)
                if random.random() < 0.20:
                    discontinued_dt = ''
                else:
                    discontinued_dt = introduced + timedelta(days=random.randint(30, 365*2))
                    # Guard future date overshoot
                    if discontinued_dt > date.today():
                        discontinued_dt = date.today() - timedelta(days=random.randint(0,30))
            else:
                discontinued_dt = ''

            # Base price distribution by category (rough ranges)
            base_ranges = {
                'Electronics': (49, 1999),
                'Home': (9, 799),
                'Apparel': (5, 299),
                'Sports': (10, 499),
                'Beauty': (3, 249)
            }
            low, high = base_ranges[cat]
            price_val = random.uniform(low, high)
            # Currency skew toward AUD with some USD/EUR
            currency = random.choices(['AUD','USD','EUR'], weights=[0.8,0.15,0.05])[0]

            # Inject price anomalies (missing or invalid). If pid in anomaly_indices:
            if pid in anomaly_indices:
                if random.random() < 0.5:
                    # Missing (empty string) -> will break strict decimal parse; accepted as anomaly
                    price_str = ''
                else:
                    # Invalid numeric (negative or too many decimals)
                    if random.random() < 0.5:
                        price_str = f"-{price_val:.4f}"  # negative
                    else:
                        price_str = f"{price_val:.6f}"   # too many decimals for scale=4
            else:
                price_str = f"{price_val:.4f}"  # Valid scale 4

            f.write(
                f"{pid},{sku},{name},{cat},{subcat},{price_str},{currency},{str(is_disc)},{introduced.isoformat()},{discontinued_dt}\n"
            )

    ######## 3.STORES ########
    stores_path = paths['stores']
    num_stores = apply_scale_to_targets(TARGET_ROWS['stores'], args.scale)
    store_columns = get_column_names(stores_schema)
    ensure_dir(stores_path.parent)

    # Define pools
    channels = ['web', 'pos']
    # Australian states & regions
    au_states = ['NSW','VIC','QLD','WA','SA','TAS','ACT','NT']
    state_to_region = {
        'NSW': 'East',
        'VIC': 'South-East',
        'QLD': 'North-East',
        'WA': 'West',
        'SA': 'South',
        'TAS': 'South',
        'ACT': 'East',
        'NT': 'North'
    }

    # Store code pattern: STR-XXXXX
    characters = string.ascii_uppercase + string.digits

    with stores_path.open('w', encoding='utf-8') as f:
        f.write(','.join(store_columns) + '\n')
        for sid in range(1, num_stores + 1):
            store_code = 'STR-' + ''.join(random.choices(characters, k=5))
            name = f"Store {sid}"
            channel = random.choice(channels)
            state = random.choice(au_states)
            region = state_to_region[state]
            # Normal plausible lat/lon centered roughly around Australia
            lat = -44 + random.random()*10  # -44 to -34 (approx southern AU)
            lon = 112 + random.random()*40  # 112 to 152 (AU span)
            # Inject impossible lat/lon for ~0.3% of rows
            if random.random() < 0.003:
                if random.random() < 0.5:
                    lat = 123.456  # Impossible latitude
                else:
                    lon = 987.654  # Impossible longitude
            # Open date spread over a decade
            open_dt = date(2015,1,1) + timedelta(days=random.randint(0, 365*10))
            # ~10% closed stores with valid close date after open date
            if random.random() < 0.10:
                close_dt = open_dt + timedelta(days=random.randint(30, 365*5))
            else:
                close_dt = ''  # Active store (nullable)
            f.write(f"{sid},{store_code},{name},{channel},{region},{state},{lat:.6f},{lon:.6f},{open_dt.isoformat()},{close_dt}\n")

    ######## 4.SUPPLIERS ########
    suppliers_path = paths['suppliers']
    num_suppliers = apply_scale_to_targets(TARGET_ROWS['suppliers'], args.scale)
    supplier_columns = get_column_names(suppliers_schema)
    country_pool = ['AU','AU','AU','US','US','CN','DE','JP','NZ','IN','SG']  # Weighted toward AU/US
    characters = string.ascii_uppercase + string.digits

    with suppliers_path.open('w', encoding='utf-8') as f:
        f.write(','.join(supplier_columns) + '\n')
        for sid in range(1, num_suppliers + 1):
            # Supplier code pattern SUP-XXXXXX (alphanumeric uppercase)
            code = 'SUP-' + ''.join(random.choices(characters, k=6))
            name = f"{fake.company().replace(',', ' ')}"
            country = random.choice(country_pool)
            # Lead time: normal-ish distribution
            base_lt = int(np.clip(np.random.normal(25, 12), 1, 90))
            preferred_flag = random.random() < 0.30
            preferred_val = str(preferred_flag)
            f.write(f"{sid},{code},{name},{country},{base_lt},{preferred_val}\n")

    ######## 5.ORDERS HEADER ########
    ######## 6.ORDERS LINES ########
    ######## 7.EVENTS ########
    ######## 8.SENSORS ########
    ######## 9.EXCHANGE RATES ########
    ######## 10.SHIPMENTS ########
    # Shipments parquet sample with schema integration and weighted random shipping costs
    num_shipments = apply_scale_to_targets(TARGET_ROWS['shipments'], args.scale)
    # Centralize shipments path to avoid hardcoding in multiple places
    shipments_path = paths['shipments']
    
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
    pq.write_table(tbl, shipments_path, compression='snappy')


    if args.validate:
        # Schema validation for generated data (CSV & Parquet separated)
        print("🔍 Validating generated data against schemas...")

        validations = [
            ("Customers", paths['customers'], customers_schema, 'csv'),
            ("Products", paths['products'], products_schema, 'csv'),
            ("Stores", paths['stores'], stores_schema, 'csv'),
            ("Suppliers", paths['suppliers'], suppliers_schema, 'csv'),
            ("Shipments", paths['shipments'], shipments_schema, 'parquet'),
        ]

        for name, path, schema, fmt in validations:
            if fmt == 'csv':
                is_valid, error, rows = validate_csv(path, schema)
            else:
                is_valid, error, rows = validate_parquet(path, schema)

            if is_valid:
                print(f"✅ {name} data ({rows:,} rows) validates against schema")
            else:
                print(f"❌ {name} validation failed: {error}")
    else:
        print("⚠️  Skipping schema validation (default; pass --validate to enable).")

    print(f"✅ Sample raw written to {out}. Expand to required volumes per /docs.")
if __name__ == '__main__':
    main()

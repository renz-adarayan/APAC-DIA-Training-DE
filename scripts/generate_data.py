# Generate synthetic raw data locally with controlled edge cases.
# Usage: python scripts/generate_data.py --seed 42 --out data_raw
import argparse
import pathlib
import random
import sys
import numpy as np

# Package import bootstrap
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
    raise ModuleNotFoundError(
        "Failed to import 'schemas'. Confirm that 'schemas/__init__.py' exists and that you are running the script from the project root. "
        f"sys.path (first 5 entries): {sys.path[:5]} | Project root: {PROJECT_ROOT}"
    ) from e


from utils.data_utils import ensure_dir
from utils.schema_utils import validate_csv, validate_parquet
from generators.customers import generate_customers_data
from generators.products import generate_products_data
from generators.stores import generate_stores_data
from generators.suppliers import generate_suppliers_data
from generators.orders_header import generate_orders_header_data
from generators.orders_lines import generate_orders_lines_data
from generators.events import generate_events_data
from generators.sensors import generate_sensors_data
from generators.exchange_rates import generate_exchange_rates_data
from generators.shipments import generate_shipments_data
from generators.returns import generate_returns_data


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



def main():
    args = parse_args()
    random.seed(args.seed)
    np.random.seed(args.seed)
    out = pathlib.Path(args.out)
    ensure_dir(out)

    # Centralize all raw output paths
    paths = {
        'customers': out / 'customers.csv',
        'products': out / 'products.csv',
        'stores': out / 'stores.csv',
        'suppliers': out / 'suppliers.csv',
        'sensors': out / 'sensors',
        'exchange_rates': out / 'exchange_rates.xlsx',
        'shipments': out / 'shipments.parquet',
        'returns': out / 'returns_delta',
    }

    print("Starting data generation...")

    # Generate data using modular generators
    results = {}
    
    print("Generating customers data...")
    results['customers'] = generate_customers_data(customers_schema, args.scale, paths['customers'])
    
    print("Generating products data...")
    results['products'] = generate_products_data(products_schema, args.scale, paths['products'])
    
    print("Generating stores data...")
    results['stores'] = generate_stores_data(stores_schema, args.scale, paths['stores'])
    
    print("Generating suppliers data...")
    results['suppliers'] = generate_suppliers_data(suppliers_schema, args.scale, paths['suppliers'])
    
    print("Generating orders header data...")
    orders_header_count, orders_per_date, start_date, num_orders, order_dates = generate_orders_header_data(
        orders_header_schema, args.scale, out, paths['customers'], paths['stores']
    )
    results['orders_header'] = orders_header_count
    
    print("Generating orders lines data...")
    orders_lines_count = generate_orders_lines_data(
        orders_lines_schema, 
        args.scale, 
        out, 
        orders_per_date, 
        results['products'], 
        start_date, 
        num_orders, 
        order_dates,
        paths['products']
    )
    results['orders_lines'] = orders_lines_count

    print("Generating events data...")
    results['events'] = generate_events_data(events_schema, args.scale, out)

    print("Generating sensors data...")
    results['sensors'] = generate_sensors_data(sensors_schema, args.scale, paths['sensors'])

    print("Generating exchange rates data...")
    results['exchange_rates'] = generate_exchange_rates_data(exchange_rates_schema, args.scale, paths['exchange_rates'])

    print("Generating shipments data...")
    results['shipments'] = generate_shipments_data(shipments_schema, args.scale, paths['shipments'], results['orders_header'])

    print("Generating returns data...")
    # Pass orders root directory and products CSV path for actual FK references
    orders_root_path = out / 'orders'  # Orders are generated to out/orders/<partitions>
    products_csv_path = paths['products']
    results['returns'] = generate_returns_data(returns_day1_schema, args.scale, paths['returns'], 
                                              orders_root_path=orders_root_path, products_csv_path=products_csv_path)

    print(f"Generated data summary:")
    for dataset, count in results.items():
        print(f"  - {dataset}: {count:,} records")

    if args.validate:
        print("🔍 Validating generated data against schemas...")

        validations = [
            ("Customers", paths['customers'], customers_schema, 'csv'),
            ("Products", paths['products'], products_schema, 'csv'),
            ("Stores", paths['stores'], stores_schema, 'csv'),
            ("Suppliers", paths['suppliers'], suppliers_schema, 'csv'),
        ]

        for name, path, schema, fmt in validations:
            if fmt == 'csv':
                is_valid, error, rows = validate_csv(path, schema)
            else:
                is_valid, error, rows = validate_parquet(path, schema)

            if is_valid:
                print(f"{name} data ({rows:,} rows) validates against schema")
            else:
                print(f"{name} validation failed: {error}")
    else:
        print("Skipping schema validation (default; pass --validate to enable).")

    print(f"Refactored data generation complete. Output written to {out}")


if __name__ == '__main__':
    main()

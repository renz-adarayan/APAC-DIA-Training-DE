# Ingest raw files into Bronze (Parquet + Delta), with schema validation, partitioning,
# rejects, and manifest tracking in DuckDB.
# Usage: python scripts/load_to_bronze.py --raw data_raw --lake lake --manifest duckdb/warehouse.duckdb
import argparse
import pathlib
import datetime as dt
import sys
import duckdb

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

# Import bronze utility functions 
try:
    from scripts.utils.bronze_utils import (
        process_file_with,
        read_csv_with_schema,
        read_xlsx_with_schema,
    )
except ModuleNotFoundError:
    util_path = pathlib.Path(__file__).resolve().parent / 'utils'
    if str(util_path) not in sys.path:
        sys.path.insert(0, str(util_path))
    from bronze_utils import process_file_with, read_csv_with_schema, read_xlsx_with_schema  # type: ignore

def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument('--raw', type=str, default='data_raw')
    ap.add_argument('--lake', type=str, default='lake')
    ap.add_argument('--manifest', type=str, default='duckdb/warehouse.duckdb')
    ap.add_argument('--dry-run', action='store_true')
    return ap.parse_args()

def ensure_dirs(lake_root):
    for sub in ['bronze/parquet','bronze/delta']:
        (lake_root/sub).mkdir(parents=True, exist_ok=True)
    (lake_root/'_rejects').mkdir(parents=True, exist_ok=True)

def init_manifest(conn):
    conn.execute('''
        CREATE TABLE IF NOT EXISTS manifest_processed_files (
            src_path TEXT PRIMARY KEY,
            processed_at TIMESTAMP,
            row_count BIGINT,
            reject_count BIGINT DEFAULT 0,
            file_hash TEXT,
            status TEXT DEFAULT 'SUCCESS',
            error_message TEXT,
            file_size_bytes BIGINT,
            processing_duration_ms INTEGER
        )
    ''')

def load_customers(raw_root, lake_root, conn, dry_run=False):
    """Wrapper to load customers via shared process utility."""
    process_file_with(
        src_path=raw_root / 'customers.csv',
        table_name='customers',
        schema=customers_schema,
        read_func=read_csv_with_schema,
        lake_root=lake_root,
        conn=conn,
        dry_run=dry_run,
    )
def load_products(raw_root, lake_root, conn, dry_run=False):
    """Wrapper to load products via shared process utility."""
    process_file_with(
        src_path=raw_root / 'products.csv',
        table_name='products',
        schema=products_schema,
        read_func=read_csv_with_schema,
        lake_root=lake_root,
        conn=conn,
        dry_run=dry_run,
    )

def load_stores(raw_root, lake_root, conn, dry_run=False):
    """Wrapper to load stores via shared process utility."""
    process_file_with(
        src_path=raw_root / 'stores.csv',
        table_name='stores',
        schema=stores_schema,
        read_func=read_csv_with_schema,
        lake_root=lake_root,
        conn=conn,
        dry_run=dry_run,
    )    

def load_suppliers(raw_root, lake_root, conn, dry_run=False):
    """Wrapper to load suppliers via shared process utility."""
    process_file_with(
        src_path=raw_root / 'suppliers.csv',
        table_name='suppliers',
        schema=suppliers_schema,
        read_func=read_csv_with_schema,
        lake_root=lake_root,
        conn=conn,
        dry_run=dry_run,
    )    

def load_exchange_rates(raw_root, lake_root, conn, dry_run=False):
    """Wrapper to load exchange rates XLSX via shared process utility."""
    process_file_with(
        src_path=raw_root / 'exchange_rates.xlsx',
        table_name='exchange_rates',
        schema=exchange_rates_schema,
        read_func=read_xlsx_with_schema,
        lake_root=lake_root,
        conn=conn,
        dry_run=dry_run,
    )

def main():
    args = parse_args()
    raw_root = pathlib.Path(args.raw)
    lake_root = pathlib.Path(args.lake)
    
    if args.dry_run:
        print("=== DRY RUN MODE - No data will be written ===")
    
    ensure_dirs(lake_root)
    pathlib.Path(args.manifest).parent.mkdir(parents=True, exist_ok=True)
    
    if not args.dry_run:
        conn = duckdb.connect(args.manifest)
        conn.execute("INSTALL delta; LOAD delta;")
        init_manifest(conn)
    else:
        conn = None

    load_customers(raw_root, lake_root, conn, args.dry_run)
    load_products(raw_root, lake_root, conn, args.dry_run)
    load_stores(raw_root, lake_root, conn, args.dry_run)
    load_suppliers(raw_root, lake_root, conn, args.dry_run)
    load_exchange_rates(raw_root, lake_root, conn, args.dry_run)

    print("✅ Bronze load completed for implemented loaders (customers, exchange_rates).")

if __name__ == '__main__':
    main()

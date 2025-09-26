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
        ingest_file_to_bronze,
        read_csv_with_schema,
        read_xlsx_with_schema,
        read_jsonl_with_schema,
        read_parquet_with_schema,
        read_delta_with_schema,
    )
except ModuleNotFoundError:
    util_path = pathlib.Path(__file__).resolve().parent / 'utils'
    if str(util_path) not in sys.path:
        sys.path.insert(0, str(util_path))
    from bronze_utils import ingest_file_to_bronze, read_csv_with_schema, read_xlsx_with_schema, read_jsonl_with_schema, read_parquet_with_schema, read_delta_with_schema

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
    ingest_file_to_bronze(
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
    ingest_file_to_bronze(
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
    ingest_file_to_bronze(
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
    ingest_file_to_bronze(
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
    ingest_file_to_bronze(
        src_path=raw_root / 'exchange_rates.xlsx',
        table_name='exchange_rates',
        schema=exchange_rates_schema,
        read_func=read_xlsx_with_schema,
        lake_root=lake_root,
        conn=conn,
        dry_run=dry_run,
    )

def load_events(raw_root, lake_root, conn, dry_run=False):
    """Wrapper to load events JSONL via shared process utility with incremental partition processing."""
    events_base = raw_root / 'events'
    
    if not events_base.is_dir():
        print(f"Events directory not found: {events_base}")
        return
    
    # Find all partitions (event_dt=YYYY-MM-DD directories)
    partitions = [p for p in events_base.iterdir() 
                  if p.is_dir() and p.name.startswith('event_dt=')]
    
    if not partitions:
        print(f"No event partitions found in: {events_base}")
        # Fallback: check for JSONL files directly in events directory
        jsonl_files = list(events_base.glob('*.jsonl'))
        if jsonl_files:
            print(f"Found {len(jsonl_files)} JSONL file(s) in root events directory")
            # Process the first JSONL file found
            ingest_file_to_bronze(
                src_path=jsonl_files[0],
                table_name='events',
                schema=events_schema,
                read_func=read_jsonl_with_schema,
                lake_root=lake_root,
                conn=conn,
                dry_run=dry_run,
            )
        return
    
    # Sort partitions by date (newest first for incremental processing)
    partitions.sort(key=lambda x: x.name, reverse=True)
    
    print(f"Found {len(partitions)} event partitions, processing newest unprocessed first...")
    
    # Process the latest unprocessed partition
    for partition_dir in partitions:
        print(f"Checking partition: {partition_dir.name}")
        
        # Find JSONL files in this partition
        jsonl_files = list(partition_dir.glob('*.jsonl'))
        
        if not jsonl_files:
            print(f"  No JSONL files found in partition {partition_dir.name}")
            continue
        
        print(f"  Found {len(jsonl_files)} JSONL file(s) in partition {partition_dir.name}")
        
        # Process the first unprocessed JSONL file in this partition
        for jsonl_file in jsonl_files:
            # Check if this file has already been processed (for incremental loading)
            if not dry_run and conn:
                from scripts.utils.bronze_utils import already_processed
                if already_processed(conn, jsonl_file):
                    print(f"  JSONL file already processed: {jsonl_file.name}")
                    continue
            
            # Process this partition's JSONL file
            print(f"  Processing JSONL file: {jsonl_file.name}")
            ingest_file_to_bronze(
                src_path=jsonl_file,
                table_name='events',
                schema=events_schema,
                read_func=read_jsonl_with_schema,
                lake_root=lake_root,
                conn=conn,
                dry_run=dry_run,
            )
            
            # For incremental processing, stop after processing one file
            # This prevents loading all 2M events at once and enables batched processing
            print(f"  Completed processing partition: {partition_dir.name}")
            return
        
        # If we reach here, all files in this partition were already processed
        print(f"  All files in partition {partition_dir.name} already processed")
    
    print("All event partitions have been processed or no unprocessed partitions found")

def load_shipments(raw_root, lake_root, conn, dry_run=False):
    """Wrapper to load shipments Parquet via shared process utility."""
    ingest_file_to_bronze(
        src_path=raw_root / 'shipments.parquet',
        table_name='shipments',
        schema=shipments_schema,
        read_func=read_parquet_with_schema,
        lake_root=lake_root,
        conn=conn,
        dry_run=dry_run,
    )

def load_returns(raw_root, lake_root, conn, dry_run=False):
    """Wrapper to load returns Delta table via shared process utility."""
    ingest_file_to_bronze(
        src_path=raw_root / 'returns_delta',
        table_name='returns',
        schema=returns_day1_schema,
        read_func=read_delta_with_schema,
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
    load_events(raw_root, lake_root, conn, args.dry_run)
    load_shipments(raw_root, lake_root, conn, args.dry_run)
    load_returns(raw_root, lake_root, conn, args.dry_run)

    print("✅ Bronze load completed for all implemented loaders (CSV, XLSX, JSONL, Parquet, Delta).")

if __name__ == '__main__':
    main()
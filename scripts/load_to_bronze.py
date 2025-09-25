# Ingest raw files into Bronze (Parquet + Delta), with schema validation, partitioning,
# rejects, and manifest tracking in DuckDB.
# Usage: python scripts/load_to_bronze.py --raw data_raw --lake lake --manifest duckdb/warehouse.duckdb
import argparse, pathlib, os, hashlib, json, datetime as dt, sys
import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.dataset as pads
import pyarrow.parquet as pq

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

try:
    from deltalake import write_deltalake
except Exception as e:
    write_deltalake = None

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

def already_processed(conn, p): 
    return conn.execute("SELECT 1 FROM manifest_processed_files WHERE src_path = ?", [str(p)]).fetchone() is not None

def mark_processed(conn, src_path, row_count, reject_count=0, file_hash=None, status='SUCCESS', error_message=None, file_size_bytes=None, processing_duration_ms=None):
    """Mark file as processed with comprehensive metadata in the enhanced manifest table."""
    conn.execute('''
        INSERT OR REPLACE INTO manifest_processed_files 
        (src_path, processed_at, row_count, reject_count, file_hash, status, error_message, file_size_bytes, processing_duration_ms)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', [str(src_path), dt.datetime.utcnow(), row_count, reject_count, file_hash, status, error_message, file_size_bytes, processing_duration_ms])

def calculate_file_hash(file_path):
    """Calculate SHA-256 hash of file for integrity checking."""
    import hashlib
    hash_sha256 = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_sha256.update(chunk)
    return hash_sha256.hexdigest()

def write_parquet_partitioned(table, base_path, partitioning=None):
    pads.write_dataset(table, base_dir=str(base_path), format='parquet', partitioning=partitioning, existing_data_behavior='overwrite_or_ignore')

def write_delta(table, base_path, mode='append', partition_by=None, merge_schema=False):
    if write_deltalake is None:
        raise RuntimeError('deltalake not installed')
    write_deltalake(str(base_path), data=table, mode=mode, partition_by=partition_by or [])

def load_customers(raw_root, lake_root, conn, dry_run=False):
    """Load customers data with enhanced manifest tracking."""
    src = raw_root/'customers.csv'
    if not src.exists(): 
        print(f"Customers file not found: {src}")
        return
    if not dry_run and already_processed(conn, src): 
        print(f"Customers file already processed: {src}")
        return
    
    print(f"Processing customers file: {src}")
    
    # Get file metadata
    file_size_bytes = src.stat().st_size
    
    if dry_run:
        print(f"DRY RUN: Would process {src} ({file_size_bytes} bytes)")
        return
    
    # Track processing time
    start_time = dt.datetime.utcnow()
    reject_count = 0
    error_message = None
    status = 'SUCCESS'
    file_hash = None
    
    try:
        # Calculate file hash for integrity checking
        print(f"  • Calculating file hash for integrity...")
        file_hash = calculate_file_hash(src)
        
        print(f"  • Reading and validating CSV data...")
        tbl = pacsv.read_csv(src, read_options=pacsv.ReadOptions(encoding='utf-8'))
        tbl = tbl.cast(customers_schema, safe=False)
        
        print(f"  • Adding audit columns...")
        now = pa.scalar(dt.datetime.utcnow(), type=pa.timestamp('us'))
        tbl = tbl.append_column('ingestion_ts', pa.array([now.as_py()]*len(tbl), type=pa.timestamp('us')))
        
        print(f"  • Writing to Parquet and Delta formats...")
        pq_base = lake_root/'bronze'/'parquet'/'customers'
        dl_base = lake_root/'bronze'/'delta'/'customers'
        write_parquet_partitioned(tbl, pq_base, partitioning=None)
        write_delta(tbl, dl_base, mode='append')
        
        # Calculate processing duration
        processing_duration_ms = int((dt.datetime.utcnow() - start_time).total_seconds() * 1000)
        
        # Mark as successfully processed with enhanced metadata
        mark_processed(conn, src, len(tbl), reject_count, file_hash, status,
                      error_message, file_size_bytes, processing_duration_ms)
        
        print(f"✅ Successfully processed customers: {len(tbl)} rows in {processing_duration_ms}ms")
        print(f"   File hash: {file_hash[:16]}... | Size: {file_size_bytes} bytes")
        
    except Exception as e:
        # Handle processing failure
        processing_duration_ms = int((dt.datetime.utcnow() - start_time).total_seconds() * 1000)
        status = 'FAILED'
        error_message = str(e)
        
        # Mark as failed in manifest with error details
        mark_processed(conn, src, 0, reject_count, file_hash, status,
                      error_message, file_size_bytes, processing_duration_ms)
        
        print(f"❌ Failed to process customers file: {error_message}")
        raise

def load_exchange_rates(raw_root, lake_root, conn, dry_run=False):
    """Load exchange rates from XLSX format with enhanced manifest tracking."""
    src = raw_root / 'exchange_rates.xlsx'
    if not src.exists():
        print(f"Exchange rates file not found: {src}")
        return
    if not dry_run and already_processed(conn, src):
        print(f"Exchange rates file already processed: {src}")
        return
    
    print(f"Processing exchange rates file: {src}")
    
    # Get file metadata
    file_size_bytes = src.stat().st_size
    
    if dry_run:
        print(f"DRY RUN: Would process {src} ({file_size_bytes} bytes)")
        return
    
    # Track processing time
    start_time = dt.datetime.utcnow()
    reject_count = 0
    error_message = None
    status = 'SUCCESS'
    file_hash = None
    
    try:
        # Calculate file hash for integrity checking
        print(f"  • Calculating file hash for integrity...")
        file_hash = calculate_file_hash(src)
        
        print(f"  • Reading and validating XLSX data...")
        # Read XLSX file using pandas with openpyxl engine
        df = pd.read_excel(src, engine='openpyxl')
        
        # Convert to PyArrow table and validate against schema
        tbl = pa.Table.from_pandas(df)
        tbl = tbl.cast(exchange_rates_schema, safe=False)
        
        print(f"  • Adding audit columns...")
        now = pa.scalar(dt.datetime.utcnow(), type=pa.timestamp('us'))
        tbl = tbl.append_column('ingestion_ts', pa.array([now.as_py()]*len(tbl), type=pa.timestamp('us')))
        
        print(f"  • Writing to Parquet and Delta formats...")
        pq_base = lake_root / 'bronze' / 'parquet' / 'exchange_rates'
        dl_base = lake_root / 'bronze' / 'delta' / 'exchange_rates'
        write_parquet_partitioned(tbl, pq_base, partitioning=None)
        write_delta(tbl, dl_base, mode='append')
        
        # Calculate processing duration
        processing_duration_ms = int((dt.datetime.utcnow() - start_time).total_seconds() * 1000)
        
        # Mark as successfully processed with enhanced metadata
        mark_processed(conn, src, len(tbl), reject_count, file_hash, status,
                      error_message, file_size_bytes, processing_duration_ms)
        
        print(f"✅ Successfully processed exchange rates: {len(tbl)} rows in {processing_duration_ms}ms")
        print(f"   File hash: {file_hash[:16]}... | Size: {file_size_bytes} bytes")
        
    except Exception as e:
        # Handle processing failure
        processing_duration_ms = int((dt.datetime.utcnow() - start_time).total_seconds() * 1000)
        status = 'FAILED'
        error_message = str(e)
        
        # Mark as failed in manifest with error details
        mark_processed(conn, src, 0, reject_count, file_hash, status,
                      error_message, file_size_bytes, processing_duration_ms)
        
        print(f"❌ Failed to process exchange rates file: {error_message}")
        raise

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
    load_exchange_rates(raw_root, lake_root, conn, args.dry_run)

    print("✅ Bronze load completed for implemented loaders (customers, exchange_rates).")

if __name__ == '__main__':
    main()

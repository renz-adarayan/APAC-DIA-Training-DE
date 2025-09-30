# Ingest raw files into Bronze (Parquet + Delta), with schema validation, partitioning,
# rejects, and manifest tracking in DuckDB.
# Usage: python scripts/load_to_bronze.py --raw data_raw --lake lake --manifest duckdb/warehouse.duckdb
import argparse
import pathlib
import datetime as dt
import sys
import duckdb
from typing import Optional, List

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
        calculate_file_hash,
        mark_processed,
        already_processed,
        write_parquet_partitioned,
    )
except ModuleNotFoundError:
    util_path = pathlib.Path(__file__).resolve().parent / 'utils'
    if str(util_path) not in sys.path:
        sys.path.insert(0, str(util_path))
    from bronze_utils import ingest_file_to_bronze, read_csv_with_schema, read_xlsx_with_schema, read_jsonl_with_schema, read_parquet_with_schema, read_delta_with_schema, calculate_file_hash, mark_processed, already_processed, write_parquet_partitioned

# Import PyArrow for data processing
import pyarrow as pa

def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument('--raw', type=str, default='data_raw')
    ap.add_argument('--lake', type=str, default='lake')
    ap.add_argument('--manifest', type=str, default='duckdb/warehouse.duckdb')
    ap.add_argument('--dry-run', action='store_true')
    ap.add_argument('--cutoff-date', type=str, default=None,
                    help='Optional YYYY-MM-DD cutoff; partitions older than this are pruned (events/order partitions)')
    ap.add_argument('--collect-partition-stats', action='store_true',
                    help='Collect partition statistics after ingestion (events, orders, sensors)')
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
    # Partition statistics registry
    conn.execute('''
        CREATE TABLE IF NOT EXISTS partition_stats (
            table_name TEXT,
            partition_path TEXT,
            partition_value TEXT,
            row_count BIGINT,
            file_count INTEGER,
            total_size_bytes BIGINT,
            min_mtime TIMESTAMP,
            max_mtime TIMESTAMP,
            fully_processed BOOLEAN,
            collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (table_name, partition_path)
        )
    ''')

def _should_prune_partition(partition_name: str, cutoff_date: Optional[dt.date], prefix: str) -> bool:
    """Return True if partition should be pruned based on cutoff date.
    partition_name examples: event_dt=2024-11-04, order_dt=2024-11-02
    prefix is 'event_dt=' or 'order_dt='
    """
    if not cutoff_date:
        return False
    try:
        part_date_str = partition_name.split('=')[1]
        part_date = dt.date.fromisoformat(part_date_str)
    except Exception:
        return False
    return part_date < cutoff_date

def _update_partition_stats_events(raw_root: pathlib.Path, conn):
    events_base = raw_root / 'events'
    if not events_base.exists():
        return
    partitions = [p for p in events_base.iterdir() if p.is_dir() and p.name.startswith('event_dt=')]
    for p in partitions:
        files = list(p.glob('*.jsonl'))
        file_count = len(files)
        if file_count == 0:
            continue
        total_size = sum(f.stat().st_size for f in files)
        mtimes = [dt.datetime.utcfromtimestamp(f.stat().st_mtime) for f in files]
        # Determine processed status and row counts from manifest
        manifest_rows = conn.execute(
            "SELECT SUM(row_count), COUNT(*) FROM manifest_processed_files WHERE src_path IN (%s)" %
            ','.join(['?']*file_count), [str(f) for f in files]
        ).fetchone()
        sum_rows = manifest_rows[0] if manifest_rows[0] is not None else None
        processed_files = manifest_rows[1]
        fully_processed = processed_files == file_count
        conn.execute('''
            INSERT OR REPLACE INTO partition_stats
            (table_name, partition_path, partition_value, row_count, file_count, total_size_bytes, min_mtime, max_mtime, fully_processed)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', [
            'events', str(p), p.name.split('=')[1], sum_rows, file_count, total_size,
            min(mtimes), max(mtimes), fully_processed
        ])

def _update_partition_stats_orders(raw_root: pathlib.Path, conn):
    orders_base = raw_root / 'orders'
    if not orders_base.exists():
        return
    partitions = [p for p in orders_base.iterdir() if p.is_dir() and p.name.startswith('order_dt=')]
    for p in partitions:
        files = list(p.glob('orders_*.csv'))  # header / lines
        if not files:
            continue
        total_size = sum(f.stat().st_size for f in files)
        mtimes = [dt.datetime.utcfromtimestamp(f.stat().st_mtime) for f in files]
        manifest_rows = conn.execute(
            "SELECT SUM(row_count), COUNT(*) FROM manifest_processed_files WHERE src_path IN (%s)" %
            ','.join(['?']*len(files)), [str(f) for f in files]
        ).fetchone()
        sum_rows = manifest_rows[0] if manifest_rows[0] is not None else None
        processed_files = manifest_rows[1]
        fully_processed = processed_files == len(files)
        conn.execute('''
            INSERT OR REPLACE INTO partition_stats
            (table_name, partition_path, partition_value, row_count, file_count, total_size_bytes, min_mtime, max_mtime, fully_processed)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', [
            'orders', str(p), p.name.split('=')[1], sum_rows, len(files), total_size,
            min(mtimes), max(mtimes), fully_processed
        ])

def _update_partition_stats_sensors(raw_root: pathlib.Path, conn):
    sensors_base = raw_root / 'sensors'
    if not sensors_base.exists():
        return
    store_partitions = [p for p in sensors_base.iterdir() if p.is_dir() and p.name.startswith('store_id=')]
    for store in store_partitions:
        month_parts = [m for m in store.iterdir() if m.is_dir() and m.name.startswith('month=')]
        for m in month_parts:
            files = list(m.glob('*.csv'))
            if not files:
                continue
            total_size = sum(f.stat().st_size for f in files)
            mtimes = [dt.datetime.utcfromtimestamp(f.stat().st_mtime) for f in files]
            manifest_rows = conn.execute(
                "SELECT SUM(row_count), COUNT(*) FROM manifest_processed_files WHERE src_path IN (%s)" %
                ','.join(['?']*len(files)), [str(f) for f in files]
            ).fetchone()
            sum_rows = manifest_rows[0] if manifest_rows[0] is not None else None
            processed_files = manifest_rows[1]
            fully_processed = processed_files == len(files)
            partition_value = f"{store.name.split('=')[1]}_{m.name.split('=')[1]}"
            conn.execute('''
                INSERT OR REPLACE INTO partition_stats
                (table_name, partition_path, partition_value, row_count, file_count, total_size_bytes, min_mtime, max_mtime, fully_processed)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', [
                'sensors', str(m), partition_value, sum_rows, len(files), total_size,
                min(mtimes), max(mtimes), fully_processed
            ])

def update_all_partition_stats(raw_root: pathlib.Path, conn):
    _update_partition_stats_events(raw_root, conn)
    _update_partition_stats_orders(raw_root, conn)
    _update_partition_stats_sensors(raw_root, conn)


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

def load_events(raw_root, lake_root, conn, dry_run=False, cutoff_date: Optional[dt.date]=None):
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
    
    # Sort partitions by date (newest first for incremental processing) then prune
    partitions.sort(key=lambda x: x.name, reverse=True)
    if cutoff_date:
        before_len = len(partitions)
        partitions = [p for p in partitions if not _should_prune_partition(p.name, cutoff_date, 'event_dt=')]
        pruned = before_len - len(partitions)
        if pruned:
            print(f"Pruned {pruned} event partitions older than cutoff {cutoff_date}")
    
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
    """Load returns Delta table with UPSERT functionality for proper merge operations."""
    src_path = raw_root / 'returns_delta'
    
    if not src_path.exists():
        print(f"Returns file not found: {src_path}")
        return
    if not dry_run and already_processed(conn, src_path):
        print(f"Returns file already processed: {src_path}")
        return
    
    print(f"Processing returns file: {src_path}")
    
    # Get file metadata
    if src_path.is_file():
        file_size_bytes = src_path.stat().st_size
    elif src_path.is_dir():
        # Calculate total size of all files in directory
        file_size_bytes = sum(f.stat().st_size for f in src_path.rglob('*') if f.is_file())
    else:
        file_size_bytes = 0
    
    if dry_run:
        print(f"DRY RUN: Would process {src_path} ({file_size_bytes} bytes)")
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
        file_hash = calculate_file_hash(src_path)
        
        # Load raw returns data
        print(f"  • Loading raw returns data...")
        raw_table = read_delta_with_schema(src_path, returns_day1_schema, validate=False)
        
        # Enhanced validation with detailed error capture
        print(f"  • Performing enhanced schema validation...")
        src_filename = src_path.name
        
        # Import validation utilities
        try:
            from scripts.utils.validation_utils import validate_table_with_errors, write_rejects_to_lake, write_rejects_summary
        except ImportError:
            # Fallback to basic validation if validation_utils not available
            print(f"  • Validation utilities not available, using basic validation...")
            tbl = raw_table
            if 'src_filename' not in tbl.column_names:
                # Add basic audit columns
                now = pa.scalar(dt.datetime.utcnow(), type=pa.timestamp('us'))
                tbl = tbl.append_column('src_filename', pa.array([src_filename]*len(tbl)))
                tbl = tbl.append_column('src_row_hash', pa.array(['basic_hash']*len(tbl)))  # Placeholder
                tbl = tbl.append_column('ingestion_ts', pa.array([now.as_py()]*len(tbl), type=pa.timestamp('us')))
            
            validation_result = None
        else:
            # Use enhanced validation
            validation_result = validate_table_with_errors(raw_table, returns_day1_schema, src_filename)
            
            # Handle validation results
            if validation_result.invalid_rows > 0:
                print(f"  • Found {validation_result.invalid_rows} invalid rows out of {validation_result.total_rows}")
                print(f"  • Success rate: {validation_result.success_rate:.1f}%")
                print(f"  • Error breakdown: {validation_result.error_summary}")
                
                # Write rejects to lake/_rejects/
                reject_count = write_rejects_to_lake(
                    validation_result.invalid_records, 
                    'returns', 
                    lake_root, 
                    start_time
                )
                
                # Write validation summary
                write_rejects_summary(validation_result, 'returns', lake_root, start_time)
            
            # Use the validated table (with audit columns already added)
            tbl = validation_result.valid_table
            
            if tbl is None or len(tbl) == 0:
                print(f"  • No valid records to process after validation")
                # Mark as processed with zero valid rows
                processing_duration_ms = int((dt.datetime.utcnow() - start_time).total_seconds() * 1000)
                mark_processed(conn, src_path, 0, reject_count, file_hash, 'SUCCESS',
                              f"All {validation_result.total_rows} rows rejected during validation", 
                              file_size_bytes, processing_duration_ms)
                return
        
        print(f"  • Writing {len(tbl)} valid records using UPSERT logic...")
        pq_base = lake_root / 'bronze' / 'parquet' / 'returns'
        dl_base = lake_root / 'bronze' / 'delta' / 'returns'
        
        # Write to Parquet (standard approach)
        write_parquet_partitioned(tbl, pq_base, partitioning=None)
        
        # Use UPSERT logic for Delta Lake
        from scripts.utils.bronze_utils import upsert_returns_delta
        rows_inserted, rows_updated, rows_deleted = upsert_returns_delta(tbl, dl_base, primary_key='return_id')
        
        # Calculate processing duration
        processing_duration_ms = int((dt.datetime.utcnow() - start_time).total_seconds() * 1000)
        
        # Mark as successfully processed with enhanced metadata
        mark_processed(conn, src_path, len(tbl), reject_count, file_hash, status,
                      error_message, file_size_bytes, processing_duration_ms)
        
        print(f"Successfully processed returns: {len(tbl)} valid rows ({rows_inserted} inserted, {rows_updated} updated, {rows_deleted} deleted), {reject_count} rejects in {processing_duration_ms}ms")
        print(f"   File hash: {file_hash[:16]}... | Size: {file_size_bytes} bytes")
        if validation_result and validation_result.invalid_rows > 0:
            print(f"   Data quality: {validation_result.success_rate:.1f}% success rate")
        
    except Exception as e:
        # Handle processing failure
        processing_duration_ms = int((dt.datetime.utcnow() - start_time).total_seconds() * 1000)
        status = 'FAILED'
        error_message = str(e)
        
        # Mark as failed in manifest with error details
        mark_processed(conn, src_path, 0, reject_count, file_hash, status,
                      error_message, file_size_bytes, processing_duration_ms)
        
        print(f"Failed to process returns file: {error_message}")
        raise

def load_sensors(raw_root, lake_root, conn, dry_run=False):  # sensors partition pruning by date not implemented yet
    """Wrapper to load sensors CSV files with incremental partition processing."""
    sensors_base = raw_root / 'sensors'
    
    if not sensors_base.is_dir():
        print(f"Sensors directory not found: {sensors_base}")
        return
    
    # Find all store_id partitions
    store_partitions = [p for p in sensors_base.iterdir() 
                       if p.is_dir() and p.name.startswith('store_id=')]
    
    if not store_partitions:
        print(f"No sensor store partitions found in: {sensors_base}")
        return
    
    # Sort store partitions by store_id (process numerically)
    store_partitions.sort(key=lambda x: int(x.name.split('=')[1]))
    
    print(f"Found {len(store_partitions)} sensor store partitions...")
    
    # Process each store's sensor data
    for store_partition in store_partitions:
        print(f"Checking store partition: {store_partition.name}")
        
        # Find all month partitions within this store
        month_partitions = [p for p in store_partition.iterdir() 
                           if p.is_dir() and p.name.startswith('month=')]
        
        if not month_partitions:
            print(f"  No month partitions found in store {store_partition.name}")
            continue
        
        # Sort month partitions by date (newest first for incremental processing)
        month_partitions.sort(key=lambda x: x.name.split('=')[1], reverse=True)
        
        print(f"  Found {len(month_partitions)} month partitions in {store_partition.name}")
        
        # Process the latest unprocessed month partition
        for month_partition in month_partitions:
            print(f"  Checking month partition: {month_partition.name}")
            
            # Find CSV files in this month partition
            csv_files = list(month_partition.glob('*.csv'))
            
            if not csv_files:
                print(f"    No CSV files found in partition {month_partition.name}")
                continue
            
            print(f"    Found {len(csv_files)} CSV file(s) in partition {month_partition.name}")
            
            # Process each CSV file in this partition
            for csv_file in csv_files:
                # Check if this file has already been processed (for incremental loading)
                if not dry_run and conn:
                    from scripts.utils.bronze_utils import already_processed
                    if already_processed(conn, csv_file):
                        print(f"    CSV file already processed: {csv_file.name}")
                        continue
                
                # Process this month's CSV file
                print(f"    Processing CSV file: {csv_file.name}")
                ingest_file_to_bronze(
                    src_path=csv_file,
                    table_name='sensors',
                    schema=sensors_schema,
                    read_func=read_csv_with_schema,
                    lake_root=lake_root,
                    conn=conn,
                    dry_run=dry_run,
                )
                
                # For incremental processing, stop after processing one file per run
                # This prevents loading all 5-10M sensor records at once
                print(f"    Completed processing partition: {store_partition.name}/{month_partition.name}")
                return
            
            # If we reach here, all files in this month partition were already processed
            print(f"    All files in partition {month_partition.name} already processed")
        
        # If we reach here, all months in this store partition were already processed
        print(f"  All month partitions in {store_partition.name} already processed")
    
    print("All sensor partitions have been processed or no unprocessed partitions found")

def load_orders_header(raw_root, lake_root, conn, dry_run=False, cutoff_date: Optional[dt.date]=None):
    """Wrapper to load orders header CSV files with incremental partition processing."""
    orders_base = raw_root / 'orders'
    
    if not orders_base.is_dir():
        print(f"Orders directory not found: {orders_base}")
        return
    
    # Find all order_dt partitions
    partitions = [p for p in orders_base.iterdir() 
                  if p.is_dir() and p.name.startswith('order_dt=')]
    
    if not partitions:
        print(f"No order date partitions found in: {orders_base}")
        return
    
    # Sort partitions by date (newest first) then apply cutoff pruning if provided
    partitions.sort(key=lambda x: x.name.split('=')[1], reverse=True)
    if cutoff_date:
        before_len = len(partitions)
        partitions = [p for p in partitions if not _should_prune_partition(p.name, cutoff_date, 'order_dt=')]
        pruned = before_len - len(partitions)
        if pruned:
            print(f"Pruned {pruned} orders_header partitions older than cutoff {cutoff_date}")
    
    print(f"Found {len(partitions)} order date partitions, processing newest unprocessed first...")
    
    # Process the latest unprocessed partition
    for partition_dir in partitions:
        print(f"Checking partition: {partition_dir.name}")
        
        # Look for orders_header.csv file in this partition
        header_file = partition_dir / 'orders_header.csv'
        
        if not header_file.exists():
            print(f"  No orders_header.csv found in partition {partition_dir.name}")
            continue
        
        print(f"  Found orders_header.csv in partition {partition_dir.name}")
        
        # Check if this file has already been processed (for incremental loading)
        if not dry_run and conn:
            from scripts.utils.bronze_utils import already_processed
            if already_processed(conn, header_file):
                print(f"Orders header file already processed: {header_file.name}")
                continue
        
        # Process this partition's orders header file
        print(f"  Processing orders header file: {header_file.name}")
        ingest_file_to_bronze(
            src_path=header_file,
            table_name='orders_header',
            schema=orders_header_schema,
            read_func=read_csv_with_schema,
            lake_root=lake_root,
            conn=conn,
            dry_run=dry_run,
        )
        
        # For incremental processing, stop after processing one file per run
        # This prevents loading all 1M+ orders at once and enables batched processing
        print(f"Completed processing partition: {partition_dir.name}")
        return
    
    print("All order header partitions have been processed or no unprocessed partitions found")

def load_orders_lines(raw_root, lake_root, conn, dry_run=False, cutoff_date: Optional[dt.date]=None):
    """Wrapper to load orders lines CSV files with incremental partition processing."""
    orders_base = raw_root / 'orders'
    
    if not orders_base.is_dir():
        print(f"Orders directory not found: {orders_base}")
        return
    
    # Find all order_dt partitions
    partitions = [p for p in orders_base.iterdir() 
                  if p.is_dir() and p.name.startswith('order_dt=')]
    
    if not partitions:
        print(f"No order date partitions found in: {orders_base}")
        return
    
    # Sort partitions by date (newest first) then apply cutoff pruning if provided
    partitions.sort(key=lambda x: x.name.split('=')[1], reverse=True)
    if cutoff_date:
        before_len = len(partitions)
        partitions = [p for p in partitions if not _should_prune_partition(p.name, cutoff_date, 'order_dt=')]
        pruned = before_len - len(partitions)
        if pruned:
            print(f"Pruned {pruned} orders_lines partitions older than cutoff {cutoff_date}")
    
    print(f"Found {len(partitions)} order date partitions, processing newest unprocessed first...")
    
    # Process the latest unprocessed partition
    for partition_dir in partitions:
        print(f"Checking partition: {partition_dir.name}")
        
        # Look for orders_lines.csv file in this partition
        lines_file = partition_dir / 'orders_lines.csv'
        
        if not lines_file.exists():
            print(f"No orders_lines.csv found in partition {partition_dir.name}")
            continue
        
        print(f"Found orders_lines.csv in partition {partition_dir.name}")
        
        # Check if this file has already been processed (for incremental loading)
        if not dry_run and conn:
            from scripts.utils.bronze_utils import already_processed
            if already_processed(conn, lines_file):
                print(f"  Orders lines file already processed: {lines_file.name}")
                continue
        
        # Process this partition's orders lines file
        print(f"  Processing orders lines file: {lines_file.name}")
        ingest_file_to_bronze(
            src_path=lines_file,
            table_name='orders_lines',
            schema=orders_lines_schema,
            read_func=read_csv_with_schema,
            lake_root=lake_root,
            conn=conn,
            dry_run=dry_run,
        )
        
        # For incremental processing, stop after processing one file per run
        # This prevents loading all 3-4M+ order lines at once and enables batched processing
        print(f"  Completed processing partition: {partition_dir.name}")
        return
    
    print("All order lines partitions have been processed or no unprocessed partitions found")

def main():
    args = parse_args()
    raw_root = pathlib.Path(args.raw)
    lake_root = pathlib.Path(args.lake)

    if args.dry_run:
        print("=== DRY RUN MODE - No data will be written ===")

    ensure_dirs(lake_root)
    pathlib.Path(args.manifest).parent.mkdir(parents=True, exist_ok=True)

    cutoff_date = None
    if args.cutoff_date:
        try:
            cutoff_date = dt.date.fromisoformat(args.cutoff_date)
            print(f"Using cutoff date for partition pruning: {cutoff_date}")
        except ValueError:
            print(f"Invalid --cutoff-date value '{args.cutoff_date}', expected YYYY-MM-DD. Ignoring.")

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
    load_events(raw_root, lake_root, conn, args.dry_run, cutoff_date=cutoff_date)
    load_sensors(raw_root, lake_root, conn, args.dry_run)
    load_orders_header(raw_root, lake_root, conn, args.dry_run, cutoff_date=cutoff_date)
    load_orders_lines(raw_root, lake_root, conn, args.dry_run, cutoff_date=cutoff_date)
    load_shipments(raw_root, lake_root, conn, args.dry_run)
    load_returns(raw_root, lake_root, conn, args.dry_run)

    if conn and args.collect_partition_stats and not args.dry_run:
        print("Collecting partition statistics...")
        update_all_partition_stats(raw_root, conn)
        # Quick summary output
        summary = conn.execute("SELECT table_name, COUNT(*), SUM(file_count), SUM(row_count) FROM partition_stats GROUP BY 1").fetchall()
        for row in summary:
            print(f"Partition stats -> table={row[0]} partitions={row[1]} files={row[2]} rows={row[3]}")

    print("✅ Bronze load completed (with partition pruning/stat collection where requested).")

if __name__ == '__main__':
    main()

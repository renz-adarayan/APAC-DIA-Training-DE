"""
Bronze data source loaders.

This module contains all the individual load_* functions for different data sources
in the bronze layer ingestion process.
"""

import pathlib
import datetime as dt
from typing import Optional, List
import pyarrow as pa

# Import schemas
from schemas import (
    customers_schema, products_schema, stores_schema, suppliers_schema,
    orders_header_schema, orders_lines_schema, events_schema, sensors_schema,
    exchange_rates_schema, shipments_schema, returns_day1_schema,
)

# Import bronze utilities
from scripts.utils.bronze_io import (
    read_csv_with_schema,
    read_xlsx_with_schema,
    read_jsonl_with_schema,
    read_parquet_with_schema,
    read_delta_with_schema,
    calculate_file_hash,
    write_parquet_partitioned,
)
from scripts.utils.bronze_ingestion import (
    ingest_file_to_bronze,
    mark_processed,
    already_processed,
    upsert_returns_delta,
)
from scripts.utils.bronze_partition_utils import should_prune_partition
from scripts.utils.bronze_batch_utils import (
    get_all_unprocessed_batches,
    get_next_unprocessed_batch,
)


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


def _process_event_batch(batch: List[pathlib.Path], lake_root: pathlib.Path, conn, dry_run: bool):
    """Process all JSONL files in a batch of event partitions.
    
    Args:
        batch: List of partition directories to process
        lake_root: Lake root directory
        conn: DuckDB connection
        dry_run: If True, only validate without writing
    """
    total_files_processed = 0
    
    for i, partition_dir in enumerate(batch, 1):
        # Find JSONL files in this partition
        jsonl_files = list(partition_dir.glob('*.jsonl'))
        
        if not jsonl_files:
            continue
        
        # Process all JSONL files in this partition
        for jsonl_file in jsonl_files:
            # Check if this file has already been processed (idempotency)
            if not dry_run and conn and already_processed(conn, jsonl_file):
                continue
            
            # Process this JSONL file with progress indicator
            print(f"Events [{i}/{len(batch)}]: {partition_dir.name}")
            ingest_file_to_bronze(
                src_path=jsonl_file,
                table_name='events',
                schema=events_schema,
                read_func=read_jsonl_with_schema,
                lake_root=lake_root,
                conn=conn,
                dry_run=dry_run,
            )
            total_files_processed += 1
    
    if total_files_processed > 0:
        print(f"Batch completed: {total_files_processed} files processed")


def load_events(raw_root, lake_root, conn, dry_run=False, cutoff_date: Optional[dt.date]=None, initial_mode: bool = False, batch_size_days: int = 7):
    """Load events JSONL files with weekly batch processing.
    
    Args:
        initial_mode: If True, process ALL unprocessed events in weekly batches. 
                     If False (default), process next 1 week batch incrementally.
        batch_size_days: Number of days per batch (default 7 for weekly)
    """
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
    
    # Apply cutoff date pruning if provided
    if cutoff_date:
        before_len = len(partitions)
        partitions = [p for p in partitions if not should_prune_partition(p.name, cutoff_date, 'event_dt=')]
        pruned = before_len - len(partitions)
        if pruned:
            print(f"Pruned {pruned} event partitions older than cutoff {cutoff_date}")
    
    print(f"Found {len(partitions)} event partitions...")
    
    # Determine processing mode and get batches
    if initial_mode:
        print(f"Initial loading mode: Processing ALL unprocessed events in {batch_size_days}-day batches...")
        batches = get_all_unprocessed_batches(partitions, conn, batch_size_days, dry_run)
        
        if not batches:
            print("All event partitions have been processed or no unprocessed partitions found")
            return
        
        print(f"Found {len(batches)} unprocessed batches to process...")
        
        # Process all batches for initial loading
        for batch_idx, batch in enumerate(batches, 1):
            batch_start = batch[0].name.split('=')[1]
            batch_end = batch[-1].name.split('=')[1]
            print(f"Processing batch {batch_idx}/{len(batches)}: {batch_start} to {batch_end} ({len(batch)} partitions)")
            
            _process_event_batch(batch, lake_root, conn, dry_run)
            
            print(f"✅ Completed batch {batch_idx}/{len(batches)}")
        
        print(f"✅ Initial loading completed: Processed {len(batches)} batches")
        
    else:
        print(f"Incremental mode: Processing next {batch_size_days}-day batch of unprocessed events...")
        batch = get_next_unprocessed_batch(partitions, conn, batch_size_days, dry_run)
        
        if not batch:
            print("All event partitions have been processed or no unprocessed partitions found")
            return
        
        batch_start = batch[0].name.split('=')[1]
        batch_end = batch[-1].name.split('=')[1]
        print(f"Processing incremental batch: {batch_start} to {batch_end} ({len(batch)} partitions)")
        
        _process_event_batch(batch, lake_root, conn, dry_run)
        
        print(f"✅ Incremental batch completed")


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


def load_sensors(raw_root, lake_root, conn, dry_run=False):
    """Wrapper to load sensors CSV files with incremental partition processing."""
    sensors_base = raw_root / 'sensors'
    
    if not sensors_base.is_dir():
        print(f"Sensors directory not found: {sensors_base}")
        return
    
    # Find all store_id partitions
    store_partitions = [p for p in sensors_base.iterdir() 
                       if p.is_dir() and p.name.startswith('store_id=')]
    
    if not store_partitions:
        print(f"No sensor store partitions found")
        return
    
    # Sort store partitions by store_id (process numerically)
    store_partitions.sort(key=lambda x: int(x.name.split('=')[1]))
    
    print(f"Sensors: Found {len(store_partitions)} stores, scanning for unprocessed data...")
    
    # Process each store's sensor data
    for store_partition in store_partitions:
        # Find all month partitions within this store
        month_partitions = [p for p in store_partition.iterdir() 
                           if p.is_dir() and p.name.startswith('month=')]
        
        if not month_partitions:
            continue
        
        # Sort month partitions by date (newest first for incremental processing)
        month_partitions.sort(key=lambda x: x.name.split('=')[1], reverse=True)
        
        # Process the latest unprocessed month partition
        for month_partition in month_partitions:
            # Find CSV files in this month partition
            csv_files = list(month_partition.glob('*.csv'))
            
            if not csv_files:
                continue
            
            # Process each CSV file in this partition
            for csv_file in csv_files:
                # Check if this file has already been processed (for incremental loading)
                if not dry_run and conn and already_processed(conn, csv_file):
                    continue
                
                # Process this month's CSV file
                print(f"Sensors: Processing {store_partition.name}/{month_partition.name}")
                ingest_file_to_bronze(
                    src_path=csv_file,
                    table_name='sensors',
                    schema=sensors_schema,
                    read_func=read_csv_with_schema,
                    lake_root=lake_root,
                    conn=conn,
                    dry_run=dry_run,
                )
                return

    print("Sensors: All partitions already processed")


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
        print(f"No order date partitions found")
        return
    
    # Sort partitions by date (newest first) then apply cutoff pruning if provided
    partitions.sort(key=lambda x: x.name.split('=')[1], reverse=True)
    if cutoff_date:
        before_len = len(partitions)
        partitions = [p for p in partitions if not should_prune_partition(p.name, cutoff_date, 'order_dt=')]
        pruned = before_len - len(partitions)
        if pruned:
            print(f"Pruned {pruned} orders_header partitions older than cutoff {cutoff_date}")
    
    print(f"Orders Header: Found {len(partitions)} partitions, scanning for unprocessed data...")
    
    # Process the latest unprocessed partition
    for partition_dir in partitions:
        # Look for orders_header.csv file in this partition
        header_file = partition_dir / 'orders_header.csv'
        
        if not header_file.exists():
            continue
        
        # Check if this file has already been processed (for incremental loading)
        if not dry_run and conn and already_processed(conn, header_file):
            continue
        
        # Process this partition's orders header file
        print(f"Orders Header: Processing {partition_dir.name}")
        ingest_file_to_bronze(
            src_path=header_file,
            table_name='orders_header',
            schema=orders_header_schema,
            read_func=read_csv_with_schema,
            lake_root=lake_root,
            conn=conn,
            dry_run=dry_run,
        )
        return
    
    print("Orders Header: All partitions already processed")


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
        print(f"No order date partitions found")
        return
    
    # Sort partitions by date (newest first) then apply cutoff pruning if provided
    partitions.sort(key=lambda x: x.name.split('=')[1], reverse=True)
    if cutoff_date:
        before_len = len(partitions)
        partitions = [p for p in partitions if not should_prune_partition(p.name, cutoff_date, 'order_dt=')]
        pruned = before_len - len(partitions)
        if pruned:
            print(f"Pruned {pruned} orders_lines partitions older than cutoff {cutoff_date}")
    
    print(f"Orders Lines: Found {len(partitions)} partitions, scanning for unprocessed data...")
    
    # Process the latest unprocessed partition
    for partition_dir in partitions:
        # Look for orders_lines.csv file in this partition
        lines_file = partition_dir / 'orders_lines.csv'
        
        if not lines_file.exists():
            continue
        
        # Check if this file has already been processed (for incremental loading)
        if not dry_run and conn and already_processed(conn, lines_file):
            continue
        
        # Process this partition's orders lines file
        print(f"Orders Lines: Processing {partition_dir.name}")
        ingest_file_to_bronze(
            src_path=lines_file,
            table_name='orders_lines',
            schema=orders_lines_schema,
            read_func=read_csv_with_schema,
            lake_root=lake_root,
            conn=conn,
            dry_run=dry_run,
        )
        return
    
    print("Orders Lines: All partitions already processed")


__all__ = [
    'load_customers',
    'load_products', 
    'load_stores',
    'load_suppliers',
    'load_exchange_rates',
    'load_shipments',
    'load_events',
    'load_returns',
    'load_sensors',
    'load_orders_header',
    'load_orders_lines',
]

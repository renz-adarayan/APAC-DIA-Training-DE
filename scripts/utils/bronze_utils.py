"""Bronze layer utility functions for file processing with manifest tracking."""

import datetime as dt
import hashlib
import pathlib
import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.dataset as pads
import pandas as pd

try:
    from deltalake import write_deltalake
except Exception as e:
    write_deltalake = None


def calculate_file_hash(file_path):
    """Calculate SHA-256 hash of file or directory for integrity checking."""
    file_path = pathlib.Path(file_path)
    hash_sha256 = hashlib.sha256()
    
    if file_path.is_file():
        # Handle single file
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_sha256.update(chunk)
    elif file_path.is_dir():
        # Handle directory (e.g., Delta table directory)
        return calculate_directory_hash(file_path)
    else:
        raise FileNotFoundError(f"Path does not exist: {file_path}")
    
    return hash_sha256.hexdigest()


def calculate_directory_hash(dir_path):
    """Calculate SHA-256 hash of directory contents for integrity checking."""
    dir_path = pathlib.Path(dir_path)
    hash_sha256 = hashlib.sha256()
    
    # Get all files in directory, sorted for consistent hashing
    all_files = []
    for file_path in dir_path.rglob('*'):
        if file_path.is_file():
            all_files.append(file_path)
    
    all_files.sort()  # Ensure consistent ordering
    
    for file_path in all_files:
        # Include relative path in hash for structure integrity
        relative_path = file_path.relative_to(dir_path)
        hash_sha256.update(str(relative_path).encode('utf-8'))
        
        # Include file modification time and size for quick change detection
        stat = file_path.stat()
        hash_sha256.update(str(stat.st_mtime).encode('utf-8'))
        hash_sha256.update(str(stat.st_size).encode('utf-8'))
        
        # For small files (like Delta log files), include full content
        # For large files (like Parquet), use metadata only for performance
        if stat.st_size < 1024 * 1024:  # 1MB threshold
            with open(file_path, "rb") as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hash_sha256.update(chunk)
    
    return hash_sha256.hexdigest()


def write_parquet_partitioned(table, base_path, partitioning=None):
    """Write table to Parquet format with optional partitioning."""
    pads.write_dataset(table, base_dir=str(base_path), format='parquet', 
                       partitioning=partitioning, existing_data_behavior='overwrite_or_ignore')


def write_delta(table, base_path, mode='append', partition_by=None, merge_schema=False):
    """Write table to Delta Lake format."""
    if write_deltalake is None:
        raise RuntimeError('deltalake not installed')
    write_deltalake(str(base_path), data=table, mode=mode, partition_by=partition_by or [])


def ingest_file_to_bronze(src_path, table_name, schema, read_func, lake_root, conn, dry_run=False):
    """Utility function to process file"""
    if not src_path.exists():
        print(f"{table_name.title()} file not found: {src_path}")
        return
    if not dry_run and already_processed(conn, src_path):
        print(f"{table_name.title()} file already processed: {src_path}")
        return
    
    print(f"Processing {table_name} file: {src_path}")
    
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
        
        # Use the provided read function to load and validate data
        tbl = read_func(src_path, schema)
        
        print(f"  • Adding audit columns...")
        now = pa.scalar(dt.datetime.utcnow(), type=pa.timestamp('us'))
        tbl = tbl.append_column('ingestion_ts', pa.array([now.as_py()]*len(tbl), type=pa.timestamp('us')))
        
        print(f"  • Writing to Parquet and Delta formats...")
        pq_base = lake_root / 'bronze' / 'parquet' / table_name
        dl_base = lake_root / 'bronze' / 'delta' / table_name
        write_parquet_partitioned(tbl, pq_base, partitioning=None)
        write_delta(tbl, dl_base, mode='append')
        
        # Calculate processing duration
        processing_duration_ms = int((dt.datetime.utcnow() - start_time).total_seconds() * 1000)
        
        # Mark as successfully processed with enhanced metadata
        mark_processed(conn, src_path, len(tbl), reject_count, file_hash, status,
                      error_message, file_size_bytes, processing_duration_ms)
        
        print(f"✅ Successfully processed {table_name}: {len(tbl)} rows in {processing_duration_ms}ms")
        print(f"   File hash: {file_hash[:16]}... | Size: {file_size_bytes} bytes")
        
    except Exception as e:
        # Handle processing failure
        processing_duration_ms = int((dt.datetime.utcnow() - start_time).total_seconds() * 1000)
        status = 'FAILED'
        error_message = str(e)
        
        # Mark as failed in manifest with error details
        mark_processed(conn, src_path, 0, reject_count, file_hash, status,
                      error_message, file_size_bytes, processing_duration_ms)
        
        print(f"❌ Failed to process {table_name} file: {error_message}")
        raise


def already_processed(conn, p): 
    """Check if file has already been processed."""
    return conn.execute("SELECT 1 FROM manifest_processed_files WHERE src_path = ?", [str(p)]).fetchone() is not None


def mark_processed(conn, src_path, row_count, reject_count=0, file_hash=None, status='SUCCESS', error_message=None, file_size_bytes=None, processing_duration_ms=None):
    """Mark file as processed with comprehensive metadata in the enhanced manifest table."""
    conn.execute('''
        INSERT OR REPLACE INTO manifest_processed_files 
        (src_path, processed_at, row_count, reject_count, file_hash, status, error_message, file_size_bytes, processing_duration_ms)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', [str(src_path), dt.datetime.utcnow(), row_count, reject_count, file_hash, status, error_message, file_size_bytes, processing_duration_ms])


def read_csv_with_schema(file_path, schema):
    """Read CSV file and validate against schema."""
    print(f"  • Reading and validating CSV data...")
    tbl = pacsv.read_csv(file_path, read_options=pacsv.ReadOptions(encoding='utf-8'))
    return tbl.cast(schema, safe=False)


def read_xlsx_with_schema(file_path, schema):
    """Read XLSX file and validate against schema."""
    print(f"  • Reading and validating XLSX data...")
    df = pd.read_excel(file_path, engine='openpyxl')
    tbl = pa.Table.from_pandas(df)
    return tbl.cast(schema, safe=False)


def read_jsonl_with_schema(file_path, schema):
    """Read JSONL file and validate against schema."""
    print(f"Reading and validating JSONL data...")
    import json
    
    # Check if schema expects raw JSON string or parsed fields
    schema_field_names = [field.name for field in schema]
    
    if 'json' in schema_field_names and len(schema_field_names) == 1:
        # Schema expects raw JSON as string - store entire JSON line as string
        records = []
        with open(file_path, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if line:  # Skip empty lines
                    try:
                        # Validate it's valid JSON but store as string
                        json.loads(line)  # Validate JSON syntax
                        records.append({"json": line})  # Store raw JSON string
                    except json.JSONDecodeError as e:
                        print(f"    Warning: Invalid JSON on line {line_num}: {e}")
                        continue
        
        print(f"Loaded {len(records)} records from JSONL (stored as raw JSON strings)")
        
        # Convert to PyArrow table
        if records:
            tbl = pa.Table.from_pylist(records)
            return tbl.cast(schema, safe=False)
        else:
            # Return empty table with correct schema if no valid records
            return pa.table([], schema=schema)
    
    else:
        # Schema expects parsed fields - parse JSON into individual fields
        records = []
        with open(file_path, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if line:  # Skip empty lines
                    try:
                        record = json.loads(line)
                        records.append(record)
                    except json.JSONDecodeError as e:
                        print(f"    Warning: Invalid JSON on line {line_num}: {e}")
                        continue
        
        print(f"Loaded {len(records)} records from JSONL (parsed as structured data)")
        
        # Convert to PyArrow table
        if records:
            tbl = pa.Table.from_pylist(records)
            return tbl.cast(schema, safe=False)
        else:
            # Return empty table with correct schema if no valid records
            return pa.table([], schema=schema)
        

def read_parquet_with_schema(file_path, schema):
    """Read Parquet file and validate against schema."""
    print(f"Reading and validating Parquet data...")
    import pyarrow.parquet as pq
    
    # Read Parquet file using PyArrow
    tbl = pq.read_table(file_path)
    
    print(f"Loaded {len(tbl)} records from Parquet")
    
    # Cast to target schema for validation
    return tbl.cast(schema, safe=False)


def read_delta_with_schema(file_path, schema):
    """Read Delta table and validate against schema."""
    print(f"Reading and validating Delta table data...")
    try:
        from deltalake import DeltaTable
    except ImportError:
        raise ImportError("deltalake package required for Delta table reading")
    
    # Read Delta table using deltalake
    dt = DeltaTable(str(file_path))
    tbl = dt.to_pyarrow_table()
    
    print(f"Loaded {len(tbl)} records from Delta table")
    
    # Cast to target schema for validation
    return tbl.cast(schema, safe=False)

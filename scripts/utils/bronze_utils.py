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
    """Enhanced utility function to process file with validation and reject handling"""
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
        
        # Use the provided read function to load raw data (no validation yet)
        print(f"  • Loading raw data...")
        raw_table = read_func(src_path, schema, validate=False)  # Pass validate=False to skip validation in read_func
        
        # Enhanced validation with detailed error capture
        print(f"  • Performing enhanced schema validation...")
        src_filename = src_path.name
        
        # Import validation utilities
        try:
            from scripts.utils.validation_utils import validate_table_with_errors, write_rejects_to_lake, write_rejects_summary
        except ImportError:
            # Fallback to basic validation if validation_utils not available
            print(f"  • Validation utilities not available, using basic validation...")
            tbl = raw_table.cast(schema, safe=False)
            
            # Add basic audit columns
            now = pa.scalar(dt.datetime.utcnow(), type=pa.timestamp('us'))
            tbl = tbl.append_column('src_filename', pa.array([src_filename]*len(tbl)))
            tbl = tbl.append_column('src_row_hash', pa.array(['basic_hash']*len(tbl)))  # Placeholder
            tbl = tbl.append_column('ingestion_ts', pa.array([now.as_py()]*len(tbl), type=pa.timestamp('us')))
            
            validation_result = None
        else:
            # Use enhanced validation
            validation_result = validate_table_with_errors(raw_table, schema, src_filename)
            
            # Handle validation results
            if validation_result.invalid_rows > 0:
                print(f"  • Found {validation_result.invalid_rows} invalid rows out of {validation_result.total_rows}")
                print(f"  • Success rate: {validation_result.success_rate:.1f}%")
                print(f"  • Error breakdown: {validation_result.error_summary}")
                
                # Write rejects to lake/_rejects/
                reject_count = write_rejects_to_lake(
                    validation_result.invalid_records, 
                    table_name, 
                    lake_root, 
                    start_time
                )
                
                # Write validation summary
                write_rejects_summary(validation_result, table_name, lake_root, start_time)
            
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
        
        print(f"  • Writing {len(tbl)} valid records to Parquet and Delta formats...")
        pq_base = lake_root / 'bronze' / 'parquet' / table_name
        dl_base = lake_root / 'bronze' / 'delta' / table_name
        write_parquet_partitioned(tbl, pq_base, partitioning=None)
        write_delta(tbl, dl_base, mode='append')
        
        # Calculate processing duration
        processing_duration_ms = int((dt.datetime.utcnow() - start_time).total_seconds() * 1000)
        
        # Mark as successfully processed with enhanced metadata
        mark_processed(conn, src_path, len(tbl), reject_count, file_hash, status,
                      error_message, file_size_bytes, processing_duration_ms)
        
        print(f"✅ Successfully processed {table_name}: {len(tbl)} valid rows, {reject_count} rejects in {processing_duration_ms}ms")
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


def read_csv_with_schema(file_path, schema, validate=True):
    """Read CSV file and optionally validate against schema."""
    print(f"  • Reading CSV data...")
    tbl = pacsv.read_csv(file_path, read_options=pacsv.ReadOptions(encoding='utf-8'))
    
    if validate:
        print(f"  • Validating CSV data against schema...")
        return tbl.cast(schema, safe=False)
    else:
        return tbl


def read_xlsx_with_schema(file_path, schema, validate=True):
    """Read XLSX file and optionally validate against schema."""
    print(f"  • Reading XLSX data...")
    df = pd.read_excel(file_path, engine='openpyxl')
    tbl = pa.Table.from_pandas(df)
    
    if validate:
        print(f"  • Validating XLSX data against schema...")
        return tbl.cast(schema, safe=False)
    else:
        return tbl


def read_jsonl_with_schema(file_path, schema, validate=True):
    """Read JSONL file and optionally validate against schema."""
    print(f"  • Reading JSONL data...")
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
        
        print(f"    Loaded {len(records)} records from JSONL (stored as raw JSON strings)")
        
        # Convert to PyArrow table
        if records:
            tbl = pa.Table.from_pylist(records)
            if validate:
                print(f"  • Validating JSONL data against schema...")
                return tbl.cast(schema, safe=False)
            else:
                return tbl
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
        
        print(f"    Loaded {len(records)} records from JSONL (parsed as structured data)")
        
        # Convert to PyArrow table
        if records:
            tbl = pa.Table.from_pylist(records)
            if validate:
                print(f"  • Validating JSONL data against schema...")
                return tbl.cast(schema, safe=False)
            else:
                return tbl
        else:
            # Return empty table with correct schema if no valid records
            return pa.table([], schema=schema)
        

def read_parquet_with_schema(file_path, schema, validate=True):
    """Read Parquet file and optionally validate against schema."""
    print(f"  • Reading Parquet data...")
    import pyarrow.parquet as pq
    
    # Read Parquet file using PyArrow
    tbl = pq.read_table(file_path)
    
    print(f"    Loaded {len(tbl)} records from Parquet")
    
    if validate:
        print(f"  • Validating Parquet data against schema...")
        # Cast to target schema for validation
        return tbl.cast(schema, safe=False)
    else:
        return tbl


def read_delta_with_schema(file_path, schema, validate=True):
    """Read Delta table and handle schema evolution gracefully."""
    print(f"  • Reading Delta table data...")
    try:
        from deltalake import DeltaTable
    except ImportError:
        raise ImportError("deltalake package required for Delta table reading")
    
    # Read Delta table using deltalake
    dt = DeltaTable(str(file_path))
    tbl = dt.to_pyarrow_table()
    
    print(f"    Loaded {len(tbl)} records from Delta table")
    
    # Handle schema evolution for returns table specifically
    table_name = file_path.name if hasattr(file_path, 'name') else str(file_path).split('/')[-1]
    
    if validate:
        print(f"  • Handling schema evolution for Delta table...")
        
        # Get expected field names and types from target schema
        expected_fields = {field.name: field.type for field in schema}
        actual_fields = {name: tbl.schema.field(name).type for name in tbl.column_names}
        
        # Find schema differences
        extra_fields = [col for col in tbl.column_names if col not in expected_fields]
        missing_fields = [field for field in expected_fields if field not in tbl.column_names]
        
        print(f"    Schema evolution detected:")
        print(f"    - Expected fields: {list(expected_fields.keys())}")
        print(f"    - Actual fields: {list(actual_fields.keys())}")
        
        if extra_fields:
            print(f"    - Extra fields (v2 evolution): {extra_fields}")
        if missing_fields:
            print(f"    - Missing fields: {missing_fields}")
        
        # For returns table, handle schema evolution by preserving all columns
        if 'return' in table_name.lower() and extra_fields:
            print(f"    • Applying schema evolution handling for returns table")
            
            # Create evolved schema that includes all fields from actual data
            evolved_schema_fields = []
            
            # Add all expected fields first (maintain order)
            for field in schema:
                if field.name in actual_fields:
                    # Handle timestamp type conversion for return_ts
                    if field.name == 'return_ts' and str(actual_fields[field.name]) == 'timestamp[ns]':
                        # Convert timestamp_ntz to timestamp(us)
                        evolved_schema_fields.append(pa.field(field.name, pa.timestamp('us')))
                    else:
                        evolved_schema_fields.append(field)
            
            # Add any extra fields from evolved schema
            for extra_field in extra_fields:
                evolved_schema_fields.append(pa.field(extra_field, actual_fields[extra_field]))
            
            # Create evolved schema
            evolved_schema = pa.schema(evolved_schema_fields)
            print(f"    • Created evolved schema with {len(evolved_schema_fields)} fields")
            
            # Handle timestamp conversion if needed
            if 'return_ts' in tbl.column_names:
                # Convert timestamp_ntz to timestamp(us) for compatibility
                import pyarrow.compute as pc
                return_ts_col = tbl.column('return_ts')
                
                # Convert to timestamp(us) format
                if str(return_ts_col.type) == 'timestamp[ns]':
                    return_ts_converted = pc.cast(return_ts_col, pa.timestamp('us'))
                    # Replace the column
                    tbl = tbl.set_column(tbl.schema.get_field_index('return_ts'), 'return_ts', return_ts_converted)
            
            # Return table with evolved schema (no casting to avoid type conflicts)
            print(f"    • Schema evolution successful - preserving {len(tbl)} records with evolved schema")
            return tbl
        
        else:
            # Standard schema validation for non-evolving tables
            available_fields = [col for col in tbl.column_names if col in expected_fields]
            
            if extra_fields:
                print(f"    Ignoring extra fields not in target schema: {extra_fields}")
            if missing_fields:
                print(f"    Warning: Missing expected fields: {missing_fields}")
            
            # Select only the available columns that match the schema
            tbl_filtered = tbl.select(available_fields)
            
            # Cast to target schema for validation (will only include matching fields)
            return tbl_filtered.cast(schema, safe=False)
    else:
        return tbl

"""Validation utilities for enhanced error handling and reject processing."""

import hashlib
import json
import datetime as dt
from datetime import timezone
from dataclasses import dataclass
from typing import Dict, List, Optional, Any
import pyarrow as pa


@dataclass
class ValidationError:
    """Represents a single validation error with detailed context."""
    row_index: int
    column: str
    error_type: str
    error_message: str
    original_value: Any
    expected_type: str
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert validation error to dictionary for storage."""
        return {
            'row_index': self.row_index,
            'column': self.column,
            'error_type': self.error_type,
            'error_message': self.error_message,
            'original_value': str(self.original_value),
            'expected_type': self.expected_type
        }


@dataclass
class ValidationResult:
    """Contains the results of data validation including valid records and errors."""
    valid_table: Optional[pa.Table]
    invalid_records: List[Dict[str, Any]]
    validation_errors: List[ValidationError]
    total_rows: int
    valid_rows: int
    invalid_rows: int
    error_summary: Dict[str, int]
    
    @property
    def success_rate(self) -> float:
        """Calculate validation success rate as percentage."""
        if self.total_rows == 0:
            return 100.0
        return (self.valid_rows / self.total_rows) * 100.0


def categorize_error(error: Exception, column: str = None, value: Any = None) -> str:
    """Categorize validation errors into standardized types."""
    error_str = str(error).lower()
    
    # Data type conversion errors
    if any(keyword in error_str for keyword in ['cannot convert', 'invalid literal', 'could not convert']):
        return 'TYPE_CONVERSION_ERROR'
    
    # Schema validation errors
    if any(keyword in error_str for keyword in ['schema', 'field', 'column']):
        return 'SCHEMA_VALIDATION_ERROR'
    
    # Null/missing value errors
    if any(keyword in error_str for keyword in ['null', 'none', 'missing', 'required']):
        return 'NULL_VALUE_ERROR'
    
    # Range/constraint errors
    if any(keyword in error_str for keyword in ['range', 'constraint', 'out of bounds']):
        return 'CONSTRAINT_ERROR'
    
    # Encoding/format errors
    if any(keyword in error_str for keyword in ['encoding', 'decode', 'format']):
        return 'ENCODING_ERROR'
    
    # JSON parsing errors (for JSONL files)
    if any(keyword in error_str for keyword in ['json', 'parse']):
        return 'JSON_PARSE_ERROR'
    
    # Default category for unclassified errors
    return 'UNKNOWN_ERROR'


def validate_table_with_errors(table: pa.Table, schema: pa.Schema, src_filename: str) -> ValidationResult:
    """
    Validate PyArrow table against schema with detailed error capture and schema evolution support.
    
    Args:
        table: PyArrow table to validate
        schema: Target schema for validation
        src_filename: Source filename for audit purposes
        
    Returns:
        ValidationResult with valid records, errors, and statistics
    """
    validation_errors = []
    invalid_records = []
    error_summary = {}
    
    # Check for schema evolution (returns table specifically)
    if 'return' in src_filename.lower():
        print(f"  • Detected returns table - checking for schema evolution...")
        
        # Get expected field names and types from target schema
        expected_fields = {field.name: field.type for field in schema}
        actual_fields = {name: table.schema.field(name).type for name in table.column_names}
        
        # Find schema differences
        extra_fields = [col for col in table.column_names if col not in expected_fields]
        missing_fields = [field for field in expected_fields if field not in table.column_names]
        
        if extra_fields and not missing_fields:
            print(f"    • Schema evolution detected - extra fields: {extra_fields}")
            print(f"    • Applying schema evolution handling...")
            
            # Create evolved schema that includes all fields from actual data
            evolved_schema_fields = []
            
            # Add all expected fields first (maintain order)
            for field in schema:
                if field.name in actual_fields:
                    # Handle timestamp type conversion for return_ts
                    if field.name == 'return_ts' and 'timestamp' in str(actual_fields[field.name]):
                        # Keep original timestamp type but ensure compatibility
                        evolved_schema_fields.append(pa.field(field.name, pa.timestamp('us')))
                    else:
                        evolved_schema_fields.append(field)
            
            # Add any extra fields from evolved schema
            for extra_field in extra_fields:
                evolved_schema_fields.append(pa.field(extra_field, actual_fields[extra_field]))
            
            # Handle timestamp conversion if needed
            if 'return_ts' in table.column_names:
                # Convert timestamp_ntz to timestamp(us) for compatibility
                import pyarrow.compute as pc
                return_ts_col = table.column('return_ts')
                
                # Convert to timestamp(us) format if needed
                if 'timestamp[ns]' in str(return_ts_col.type):
                    return_ts_converted = pc.cast(return_ts_col, pa.timestamp('us'))
                    # Replace the column
                    table = table.set_column(table.schema.get_field_index('return_ts'), 'return_ts', return_ts_converted)
            
            # Add audit columns
            now = pa.scalar(dt.datetime.now(timezone.utc), type=pa.timestamp('us'))
            row_count = len(table)
            
            # Generate row hashes
            table_dict = table.to_pydict()
            row_hashes = []
            for row_idx in range(row_count):
                row_data = {col: table_dict[col][row_idx] for col in table_dict.keys()}
                row_hashes.append(generate_row_hash(row_data))
            
            # Add audit columns to table
            table = table.append_column('src_filename', pa.array([src_filename] * row_count))
            table = table.append_column('src_row_hash', pa.array(row_hashes))
            table = table.append_column('ingestion_ts', pa.array([now.as_py()] * row_count, type=pa.timestamp('us')))
            
            print(f"    • Schema evolution successful - processed {row_count} records with evolved schema")
            
            return ValidationResult(
                valid_table=table,
                invalid_records=[],
                validation_errors=[],
                total_rows=row_count,
                valid_rows=row_count,
                invalid_rows=0,
                error_summary={}
            )
    
    try:
        # First attempt: try to cast the entire table
        valid_table = table.cast(schema, safe=False)
        
        # Add audit columns to all successful records
        now = pa.scalar(dt.datetime.now(timezone.utc), type=pa.timestamp('us'))
        row_count = len(valid_table)
        
        # Generate row hashes
        table_dict = valid_table.to_pydict()
        row_hashes = []
        for row_idx in range(row_count):
            row_data = {col: table_dict[col][row_idx] for col in table_dict.keys()}
            row_hashes.append(generate_row_hash(row_data))
        
        # Add audit columns/metadata based on schema type
        if is_events_schema(schema):
            # For events: embed audit metadata into JSON objects
            print(f"  • Detected events schema - embedding audit metadata into JSON objects")
            json_data = valid_table.column('json').to_pylist()
            enriched_json = []
            for idx, json_str in enumerate(json_data):
                enriched = embed_audit_in_json(json_str, src_filename, row_hashes[idx], now.as_py())
                enriched_json.append(enriched)
            valid_table = pa.table({'json': enriched_json}, schema=schema)
        else:
            # For other formats: append audit columns
            valid_table = valid_table.append_column('src_filename', pa.array([src_filename] * row_count))
            valid_table = valid_table.append_column('src_row_hash', pa.array(row_hashes))
            valid_table = valid_table.append_column('ingestion_ts', pa.array([now.as_py()] * row_count, type=pa.timestamp('us')))
        
        # If successful, all records are valid
        return ValidationResult(
            valid_table=valid_table,
            invalid_records=[],
            validation_errors=[],
            total_rows=len(table),
            valid_rows=len(table),
            invalid_rows=0,
            error_summary={}
        )
        
    except Exception as global_error:
        # If global cast fails, validate row by row to capture specific errors
        print(f"  • Schema validation failed globally, performing row-by-row validation...")
        
        valid_rows_data = []
        
        # Convert table to list of dictionaries for row-by-row processing
        table_dict = table.to_pydict()
        row_count = len(table)
        
        for row_idx in range(row_count):
            # Extract single row as dictionary
            row_data = {col: table_dict[col][row_idx] for col in table_dict.keys()}
            row_hash = generate_row_hash(row_data)
            
            try:
                # Try to validate this single row
                single_row_table = pa.table([pa.array([row_data[col]]) for col in row_data.keys()], 
                                          names=list(row_data.keys()))
                validated_row = single_row_table.cast(schema, safe=False)
                
                # Add audit columns to valid row
                valid_row_dict = validated_row.to_pylist()[0]
                valid_row_dict['src_filename'] = src_filename
                valid_row_dict['src_row_hash'] = row_hash
                valid_rows_data.append(valid_row_dict)
                
            except Exception as row_error:
                # This row failed validation - categorize and store error
                error_type = categorize_error(row_error)
                
                # Try to identify which column caused the error
                failing_column = identify_failing_column(row_data, schema, row_error)
                
                validation_error = ValidationError(
                    row_index=row_idx,
                    column=failing_column,
                    error_type=error_type,
                    error_message=str(row_error),
                    original_value=row_data.get(failing_column, 'UNKNOWN'),
                    expected_type=str(schema.field(failing_column).type) if failing_column else 'UNKNOWN'
                )
                
                validation_errors.append(validation_error)
                
                # Add to error summary
                error_summary[error_type] = error_summary.get(error_type, 0) + 1
                
                # Store invalid record with audit information
                invalid_record = row_data.copy()
                invalid_record['src_filename'] = src_filename
                invalid_record['src_row_hash'] = row_hash
                invalid_record['reject_reason'] = error_type
                invalid_record['reject_message'] = str(row_error)
                invalid_record['reject_column'] = failing_column
                invalid_record['rejected_at'] = dt.datetime.now(timezone.utc).isoformat()
                
                invalid_records.append(invalid_record)
        
        # Create valid table from successful rows
        valid_table = None
        if valid_rows_data:
            if is_events_schema(schema):
                # For events: embed audit metadata into JSON objects
                print(f"  • Creating valid events table with embedded audit metadata")
                enriched_json = []
                ingestion_ts_val = dt.datetime.now(timezone.utc)
                for row_dict in valid_rows_data:
                    json_str = row_dict.get('json', '{}')
                    src_filename_val = row_dict.get('src_filename', src_filename)
                    src_row_hash_val = row_dict.get('src_row_hash', '')
                    enriched = embed_audit_in_json(json_str, src_filename_val, src_row_hash_val, ingestion_ts_val)
                    enriched_json.append(enriched)
                valid_table = pa.table({'json': enriched_json}, schema=schema)
            else:
                # For other formats: add audit columns
                for row in valid_rows_data:
                    row['ingestion_ts'] = dt.datetime.now(timezone.utc)
                valid_table = pa.Table.from_pylist(valid_rows_data, schema=enhance_schema_with_audit(schema))
        
        return ValidationResult(
            valid_table=valid_table,
            invalid_records=invalid_records,
            validation_errors=validation_errors,
            total_rows=row_count,
            valid_rows=len(valid_rows_data),
            invalid_rows=len(invalid_records),
            error_summary=error_summary
        )


def identify_failing_column(row_data: Dict[str, Any], schema: pa.Schema, error: Exception) -> str:
    """Attempt to identify which column caused the validation failure."""
    error_str = str(error).lower()
    
    # Check if error message mentions a specific column
    for field in schema:
        if field.name.lower() in error_str:
            return field.name
    
    # Try validation field by field to identify the problem
    for field in schema:
        if field.name in row_data:
            try:
                # Try to cast this single field
                single_field_array = pa.array([row_data[field.name]])
                single_field_array.cast(field.type, safe=False)
            except Exception:
                return field.name
    
    return 'UNKNOWN'


def generate_row_hash(row_data: Dict[str, Any]) -> str:
    """Generate a hash for a single row of data for audit purposes."""
    # Create a deterministic string representation of the row
    sorted_items = sorted(row_data.items())
    row_str = '|'.join(f"{k}:{v}" for k, v in sorted_items)
    
    # Generate SHA-256 hash
    return hashlib.sha256(row_str.encode('utf-8')).hexdigest()[:16]  # First 16 chars for brevity


def is_events_schema(schema: pa.Schema) -> bool:
    """Check if schema is for events table (single 'json' column)."""
    return len(schema) == 1 and schema[0].name == 'json' and schema[0].type == pa.string()


def embed_audit_in_json(json_str: str, src_filename: str, src_row_hash: str, ingestion_ts: dt.datetime) -> str:
    """Embed audit metadata into a JSON object string.
    
    Args:
        json_str: Original JSON string
        src_filename: Source filename for audit
        src_row_hash: Row hash for audit
        ingestion_ts: Ingestion timestamp
        
    Returns:
        JSON string with embedded audit fields
    """
    try:
        obj = json.loads(json_str)
        # Add audit metadata as top-level fields
        obj['_audit'] = {
            'src_filename': src_filename,
            'src_row_hash': src_row_hash,
            'ingestion_ts': ingestion_ts.isoformat()
        }
        return json.dumps(obj, ensure_ascii=False)
    except (json.JSONDecodeError, TypeError):
        # If JSON is malformed, wrap it with audit data
        return json.dumps({
            'original': json_str,
            '_audit': {
                'src_filename': src_filename,
                'src_row_hash': src_row_hash,
                'ingestion_ts': ingestion_ts.isoformat()
            }
        }, ensure_ascii=False)


def enhance_schema_with_audit(base_schema: pa.Schema) -> pa.Schema:
    """Add audit columns to existing schema."""
    audit_fields = [
        pa.field('src_filename', pa.string()),
        pa.field('src_row_hash', pa.string()),
        pa.field('ingestion_ts', pa.timestamp('us'))
    ]
    
    # Add audit fields to existing schema
    enhanced_fields = list(base_schema) + audit_fields
    return pa.schema(enhanced_fields)


def create_reject_schema() -> pa.Schema:
    """Create schema for reject records with audit and error information."""
    return pa.schema([
        # Original data fields are preserved as strings to handle any data type
        pa.field('original_data', pa.string()),  # JSON representation of original row
        
        # Audit columns
        pa.field('src_filename', pa.string()),
        pa.field('src_row_hash', pa.string()),
        pa.field('rejected_at', pa.timestamp('us')),
        
        # Error details
        pa.field('reject_reason', pa.string()),
        pa.field('reject_message', pa.string()),
        pa.field('reject_column', pa.string()),
        
        # Processing metadata
        pa.field('table_name', pa.string()),
        pa.field('error_category', pa.string()),
    ])


def write_rejects_to_lake(invalid_records: List[Dict[str, Any]], table_name: str, lake_root, timestamp: dt.datetime = None) -> int:
    """
    Write reject records to the lake/_rejects/ directory.
    
    Args:
        invalid_records: List of invalid records with error information
        table_name: Name of the source table
        lake_root: Path to lake root directory
        timestamp: Processing timestamp (defaults to current time)
        
    Returns:
        Number of reject records written
    """
    if not invalid_records:
        return 0
    
    if timestamp is None:
        timestamp = dt.datetime.now(timezone.utc)
    
    # Create rejects directory structure
    rejects_dir = lake_root / '_rejects' / table_name
    rejects_dir.mkdir(parents=True, exist_ok=True)
    
    # Prepare reject records for storage
    reject_records = []
    
    for record in invalid_records:
        # Extract error information from the record
        reject_reason = record.pop('reject_reason', 'UNKNOWN_ERROR')
        reject_message = record.pop('reject_message', '')
        reject_column = record.pop('reject_column', 'UNKNOWN')
        rejected_at_str = record.pop('rejected_at', timestamp.isoformat())
        
        # Convert original data to JSON string (excluding audit fields)
        original_data = {k: v for k, v in record.items() 
                        if k not in ['src_filename', 'src_row_hash', 'reject_reason', 'reject_message', 'reject_column', 'rejected_at']}
        
        # Create reject record
        reject_record = {
            'original_data': str(original_data),  # JSON representation as string
            'src_filename': record.get('src_filename', ''),
            'src_row_hash': record.get('src_row_hash', ''),
            'rejected_at': dt.datetime.fromisoformat(rejected_at_str.replace('Z', '+00:00')) if isinstance(rejected_at_str, str) else timestamp,
            'reject_reason': reject_reason,
            'reject_message': reject_message[:1000],  # Truncate long error messages
            'reject_column': reject_column,
            'table_name': table_name,
            'error_category': reject_reason  # Same as reject_reason for now
        }
        
        reject_records.append(reject_record)
    
    # Convert to PyArrow table and write to Parquet
    reject_table = pa.Table.from_pylist(reject_records, schema=create_reject_schema())
    
    # Create filename with timestamp for partitioning
    timestamp_str = timestamp.strftime('%Y%m%d_%H%M%S')
    reject_file = rejects_dir / f"rejects_{timestamp_str}.parquet"
    
    # Write rejects to Parquet file
    import pyarrow.parquet as pq
    pq.write_table(reject_table, str(reject_file))
    
    print(f"  • Wrote {len(reject_records)} reject records to: {reject_file}")
    
    return len(reject_records)


def write_rejects_summary(validation_result: ValidationResult, table_name: str, lake_root, timestamp: dt.datetime = None) -> str:
    """
    Write a summary of validation results to a JSON file.
    
    Args:
        validation_result: ValidationResult containing error statistics
        table_name: Name of the source table
        lake_root: Path to lake root directory  
        timestamp: Processing timestamp (defaults to current time)
        
    Returns:
        Path to the summary file
    """
    import json
    
    if timestamp is None:
        timestamp = dt.datetime.now(timezone.utc)
    
    # Create rejects directory structure
    rejects_dir = lake_root / '_rejects' / table_name
    rejects_dir.mkdir(parents=True, exist_ok=True)
    
    # Create summary data
    summary = {
        'table_name': table_name,
        'processing_timestamp': timestamp.isoformat(),
        'total_rows': validation_result.total_rows,
        'valid_rows': validation_result.valid_rows,
        'invalid_rows': validation_result.invalid_rows,
        'success_rate': validation_result.success_rate,
        'error_summary': validation_result.error_summary,
        'validation_errors': [error.to_dict() for error in validation_result.validation_errors[:100]]  # Limit to first 100 errors
    }
    
    # Create filename with timestamp
    timestamp_str = timestamp.strftime('%Y%m%d_%H%M%S')
    summary_file = rejects_dir / f"validation_summary_{timestamp_str}.json"
    
    # Write summary to JSON file
    with open(summary_file, 'w', encoding='utf-8') as f:
        json.dump(summary, f, indent=2, default=str)
    
    print(f"  • Wrote validation summary to: {summary_file}")
    
    return str(summary_file)


def generate_processing_report(lake_root, timestamp: dt.datetime = None) -> str:
    """
    Generate a comprehensive processing report for all tables.
    
    Args:
        lake_root: Path to lake root directory
        timestamp: Processing timestamp (defaults to current time)
        
    Returns:
        Path to the generated report file
    """
    import json
    import os
    
    if timestamp is None:
        timestamp = dt.datetime.now(timezone.utc)
    
    # Create reports directory
    reports_dir = lake_root / '_reports'
    reports_dir.mkdir(parents=True, exist_ok=True)
    
    # Initialize report data
    report_data = {
        'report_timestamp': timestamp.isoformat(),
        'report_type': 'comprehensive_processing_report',
        'tables': {},
        'summary': {
            'total_tables': 0,
            'total_valid_records': 0,
            'total_rejected_records': 0,
            'overall_success_rate': 0.0,
            'error_categories': {}
        }
    }
    
    # Scan rejects directory for validation summaries
    rejects_dir = lake_root / '_rejects'
    if rejects_dir.exists():
        for table_dir in rejects_dir.iterdir():
            if table_dir.is_dir():
                table_name = table_dir.name
                table_stats = {
                    'table_name': table_name,
                    'total_valid_records': 0,
                    'total_rejected_records': 0,
                    'success_rate': 100.0,
                    'error_breakdown': {},
                    'recent_summaries': []
                }
                
                # Find recent validation summaries
                summary_files = list(table_dir.glob('validation_summary_*.json'))
                summary_files.sort(key=lambda x: x.name, reverse=True)  # Most recent first
                
                for summary_file in summary_files[:5]:  # Last 5 summaries
                    try:
                        with open(summary_file, 'r') as f:
                            summary = json.load(f)
                        
                        table_stats['total_valid_records'] += summary.get('valid_rows', 0)
                        table_stats['total_rejected_records'] += summary.get('invalid_rows', 0)
                        
                        # Aggregate error breakdown
                        for error_type, count in summary.get('error_summary', {}).items():
                            table_stats['error_breakdown'][error_type] = table_stats['error_breakdown'].get(error_type, 0) + count
                        
                        # Add summary to recent list
                        table_stats['recent_summaries'].append({
                            'timestamp': summary.get('processing_timestamp'),
                            'valid_rows': summary.get('valid_rows', 0),
                            'invalid_rows': summary.get('invalid_rows', 0),
                            'success_rate': summary.get('success_rate', 100.0)
                        })
                        
                    except Exception as e:
                        print(f"    Warning: Could not read summary file {summary_file}: {e}")
                        continue
                
                # Calculate success rate for this table
                total_records = table_stats['total_valid_records'] + table_stats['total_rejected_records']
                if total_records > 0:
                    table_stats['success_rate'] = (table_stats['total_valid_records'] / total_records) * 100.0
                
                report_data['tables'][table_name] = table_stats
                report_data['summary']['total_tables'] += 1
                report_data['summary']['total_valid_records'] += table_stats['total_valid_records']
                report_data['summary']['total_rejected_records'] += table_stats['total_rejected_records']
                
                # Aggregate error categories
                for error_type, count in table_stats['error_breakdown'].items():
                    report_data['summary']['error_categories'][error_type] = report_data['summary']['error_categories'].get(error_type, 0) + count
    
    # Calculate overall success rate
    total_all_records = report_data['summary']['total_valid_records'] + report_data['summary']['total_rejected_records']
    if total_all_records > 0:
        report_data['summary']['overall_success_rate'] = (report_data['summary']['total_valid_records'] / total_all_records) * 100.0
    else:
        report_data['summary']['overall_success_rate'] = 100.0
    
    # Create report filename with timestamp
    timestamp_str = timestamp.strftime('%Y%m%d_%H%M%S')
    report_file = reports_dir / f"processing_report_{timestamp_str}.json"
    
    # Write comprehensive report
    with open(report_file, 'w', encoding='utf-8') as f:
        json.dump(report_data, f, indent=2, default=str)
    
    print(f"  • Generated comprehensive processing report: {report_file}")
    
    # Also create a human-readable summary
    summary_file = reports_dir / f"processing_summary_{timestamp_str}.txt"
    with open(summary_file, 'w', encoding='utf-8') as f:
        f.write(f"Bronze Layer Processing Report\n")
        f.write(f"Generated: {timestamp.isoformat()}\n")
        f.write(f"="*50 + "\n\n")
        
        f.write(f"OVERALL SUMMARY:\n")
        f.write(f"  Total Tables Processed: {report_data['summary']['total_tables']}\n")
        f.write(f"  Total Valid Records: {report_data['summary']['total_valid_records']:,}\n")
        f.write(f"  Total Rejected Records: {report_data['summary']['total_rejected_records']:,}\n")
        f.write(f"  Overall Success Rate: {report_data['summary']['overall_success_rate']:.2f}%\n\n")
        
        if report_data['summary']['error_categories']:
            f.write(f"ERROR BREAKDOWN:\n")
            for error_type, count in sorted(report_data['summary']['error_categories'].items()):
                f.write(f"  {error_type}: {count:,} records\n")
            f.write("\n")
        
        f.write(f"TABLE DETAILS:\n")
        for table_name, stats in report_data['tables'].items():
            f.write(f"  {table_name}:\n")
            f.write(f"    Valid Records: {stats['total_valid_records']:,}\n")
            f.write(f"    Rejected Records: {stats['total_rejected_records']:,}\n")
            f.write(f"    Success Rate: {stats['success_rate']:.2f}%\n")
            if stats['error_breakdown']:
                f.write(f"    Top Errors: {', '.join(list(stats['error_breakdown'].keys())[:3])}\n")
            f.write("\n")
    
    print(f"  • Generated human-readable summary: {summary_file}")
    
    return str(report_file)

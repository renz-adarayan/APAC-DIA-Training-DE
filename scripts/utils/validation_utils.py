"""Validation utilities for enhanced error handling and reject processing."""

import hashlib
import datetime as dt
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
    Validate PyArrow table against schema with detailed error capture.
    
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
    
    try:
        # First attempt: try to cast the entire table
        valid_table = table.cast(schema, safe=False)
        
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
                invalid_record['rejected_at'] = dt.datetime.utcnow().isoformat()
                
                invalid_records.append(invalid_record)
        
        # Create valid table from successful rows
        valid_table = None
        if valid_rows_data:
            # Add ingestion timestamp to all valid rows
            for row in valid_rows_data:
                row['ingestion_ts'] = dt.datetime.utcnow()
            
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
        timestamp = dt.datetime.utcnow()
    
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
        timestamp = dt.datetime.utcnow()
    
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
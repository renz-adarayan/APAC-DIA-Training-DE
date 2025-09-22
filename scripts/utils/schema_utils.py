"""Schema utility functions for data generation."""
import pathlib
import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq


def get_schema_columns(schema):
    """Extract column names and types from PyArrow schema"""
    return [(field.name, field.type) for field in schema]


def get_column_names(schema):
    """Get just the column names from a schema"""
    return [field.name for field in schema]


def validate_data_against_schema(data_dict, schema):
    """Validate that generated data matches schema expectations"""
    try:
        table = pa.table(data_dict)
        casted = table.cast(schema)
        return True, None
    except Exception as e:
        return False, str(e)


def validate_csv(path: pathlib.Path, schema: pa.Schema):
    """Read a CSV file at path and validate against provided PyArrow schema.

    Returns (is_valid, error_message, row_count).
    """
    try:
        table = pacsv.read_csv(path)
        is_valid, error = validate_data_against_schema(table.to_pydict(), schema)
        return is_valid, error, table.num_rows
    except Exception as e:
        return False, str(e), 0


def validate_parquet(path: pathlib.Path, schema: pa.Schema):
    """Read a Parquet file at path and validate against provided PyArrow schema.

    Returns (is_valid, error_message, row_count).
    """
    try:
        table = pq.read_table(path)
        is_valid, error = validate_data_against_schema(table.to_pydict(), schema)
        return is_valid, error, table.num_rows
    except Exception as e:
        return False, str(e), 0

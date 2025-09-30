"""Low-level Bronze layer IO and integrity utilities.

This module is intentionally focused on:
  * File / directory hashing (integrity + idempotency helpers)
  * Read helpers for supported raw formats (CSV, XLSX, JSONL, Parquet, Delta)
  * Write helpers for Parquet + Delta (no business logic)

It contains NO manifest/database awareness and NO ingestion orchestration logic.
That separation keeps these functions side-effect light (aside from filesystem
reads/writes) and easy to unit test in isolation.
"""
from __future__ import annotations

import hashlib
import pathlib
import datetime as dt  # (kept: some readers/writers may add timestamps later)
from typing import Optional

import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.dataset as pads
import pandas as pd

try:  # Optional dependency
    from deltalake import write_deltalake as _write_deltalake
except Exception:  # pragma: no cover - handled gracefully
    _write_deltalake = None  # type: ignore

# ---------------------------------------------------------------------------
# Hash / integrity helpers
# ---------------------------------------------------------------------------

def calculate_file_hash(file_path: pathlib.Path | str) -> str:
    """Return SHA-256 hash for a file or directory.

    Directories are hashed via ``calculate_directory_hash``.
    """
    file_path = pathlib.Path(file_path)
    if file_path.is_file():
        sha = hashlib.sha256()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                sha.update(chunk)
        return sha.hexdigest()
    if file_path.is_dir():
        return calculate_directory_hash(file_path)
    raise FileNotFoundError(f"Path does not exist: {file_path}")


def calculate_directory_hash(dir_path: pathlib.Path | str) -> str:
    """Return SHA-256 hash for directory contents (names + metadata + small file data)."""
    dir_path = pathlib.Path(dir_path)
    sha = hashlib.sha256()
    all_files = sorted([p for p in dir_path.rglob('*') if p.is_file()])
    for fp in all_files:
        rel = fp.relative_to(dir_path)
        sha.update(str(rel).encode('utf-8'))
        st = fp.stat()
        sha.update(str(st.st_mtime).encode('utf-8'))
        sha.update(str(st.st_size).encode('utf-8'))
        if st.st_size < 1024 * 1024:  # inline small file content (<1MB)
            with open(fp, 'rb') as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    sha.update(chunk)
    return sha.hexdigest()

# ---------------------------------------------------------------------------
# Writers
# ---------------------------------------------------------------------------

def write_parquet_partitioned(table: pa.Table, base_path: pathlib.Path | str, partitioning: Optional[list[str]] = None) -> None:
    """Write a PyArrow table to partitioned Parquet (overwrite_or_ignore semantics)."""
    pads.write_dataset(
        table,
        base_dir=str(base_path),
        format='parquet',
        partitioning=partitioning,
        existing_data_behavior='overwrite_or_ignore'
    )


def write_delta(table: pa.Table, base_path: pathlib.Path | str, mode: str = 'append', partition_by: Optional[list[str]] = None, merge_schema: bool = False) -> None:  # noqa: D401
    """Write table to a Delta Lake path (requires deltalake)."""
    if _write_deltalake is None:  # pragma: no cover
        raise RuntimeError('deltalake not installed')
    _write_deltalake(str(base_path), data=table, mode=mode, partition_by=partition_by or [])

# ---------------------------------------------------------------------------
# Readers (schema-aware, optional validation)
# ---------------------------------------------------------------------------

def read_csv_with_schema(file_path: pathlib.Path | str, schema: pa.Schema, validate: bool = True) -> pa.Table:
    print("  • Reading CSV data...")
    tbl = pacsv.read_csv(file_path, read_options=pacsv.ReadOptions(encoding='utf-8'))
    if validate:
        print("  • Validating CSV data against schema...")
        return tbl.cast(schema, safe=False)
    return tbl


def read_xlsx_with_schema(file_path: pathlib.Path | str, schema: pa.Schema, validate: bool = True) -> pa.Table:
    print("  • Reading XLSX data...")
    df = pd.read_excel(file_path, engine='openpyxl')
    tbl = pa.Table.from_pandas(df)
    if validate:
        print("  • Validating XLSX data against schema...")
        return tbl.cast(schema, safe=False)
    return tbl


def read_jsonl_with_schema(file_path: pathlib.Path | str, schema: pa.Schema, validate: bool = True) -> pa.Table:
    print("  • Reading JSONL data...")
    import json
    schema_field_names = [f.name for f in schema]
    # Case 1: Single raw json column
    if 'json' in schema_field_names and len(schema_field_names) == 1:
        records = []
        with open(file_path, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                try:
                    json.loads(line)  # validate only
                except json.JSONDecodeError as e:
                    print(f"    Warning: Invalid JSON on line {line_num}: {e}")
                    continue
                records.append({'json': line})
        print(f"    Loaded {len(records)} records from JSONL (stored as raw JSON strings)")
        if not records:
            return pa.table([], schema=schema)
        tbl = pa.Table.from_pylist(records)
        return tbl.cast(schema, safe=False) if validate else tbl

    # Case 2: Structured JSON expected
    records = []
    with open(file_path, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError as e:
                print(f"    Warning: Invalid JSON on line {line_num}: {e}")
                continue
            records.append(rec)
    print(f"    Loaded {len(records)} records from JSONL (parsed as structured data)")
    if not records:
        return pa.table([], schema=schema)
    tbl = pa.Table.from_pylist(records)
    return tbl.cast(schema, safe=False) if validate else tbl


def read_parquet_with_schema(file_path: pathlib.Path | str, schema: pa.Schema, validate: bool = True) -> pa.Table:
    print("  • Reading Parquet data...")
    import pyarrow.parquet as pq
    tbl = pq.read_table(file_path)
    print(f"    Loaded {len(tbl)} records from Parquet")
    if validate:
        print("  • Validating Parquet data against schema...")
        return tbl.cast(schema, safe=False)
    return tbl


def read_delta_with_schema(file_path: pathlib.Path | str, schema: pa.Schema, validate: bool = True) -> pa.Table:
    print("  • Reading Delta table data...")
    try:
        from deltalake import DeltaTable
    except ImportError:  # pragma: no cover
        raise ImportError("deltalake package required for Delta table reading")
    dtbl = DeltaTable(str(file_path))
    tbl = dtbl.to_pyarrow_table()
    print(f"    Loaded {len(tbl)} records from Delta table")

    if not validate:
        return tbl

    print("  • Handling schema evolution for Delta table...")
    table_name = pathlib.Path(file_path).name
    expected_fields = {f.name: f.type for f in schema}
    actual_fields = {name: tbl.schema.field(name).type for name in tbl.column_names}
    extra_fields = [c for c in tbl.column_names if c not in expected_fields]
    missing_fields = [c for c in expected_fields if c not in tbl.column_names]
    print("    Schema evolution detected:")
    print(f"    - Expected fields: {list(expected_fields.keys())}")
    print(f"    - Actual fields: {list(actual_fields.keys())}")
    if extra_fields:
        print(f"    - Extra fields (v2 evolution): {extra_fields}")
    if missing_fields:
        print(f"    - Missing fields: {missing_fields}")

    # Special evolution handling for returns-like tables
    if 'return' in table_name.lower() and extra_fields:
        print("    • Applying schema evolution handling for returns table")
        # Convert timestamp if needed
        if 'return_ts' in tbl.column_names and str(tbl.schema.field('return_ts').type) == 'timestamp[ns]':
            import pyarrow.compute as pc
            idx = tbl.schema.get_field_index('return_ts')
            converted = pc.cast(tbl.column('return_ts'), pa.timestamp('us'))
            tbl = tbl.set_column(idx, 'return_ts', converted)
        return tbl  # Preserve evolved shape

    available = [c for c in tbl.column_names if c in expected_fields]
    if extra_fields:
        print(f"    Ignoring extra fields not in target schema: {extra_fields}")
    if missing_fields:
        print(f"    Warning: Missing expected fields: {missing_fields}")
    filtered = tbl.select(available)
    return filtered.cast(schema, safe=False)

__all__ = [
    'calculate_file_hash', 'calculate_directory_hash',
    'write_parquet_partitioned', 'write_delta',
    'read_csv_with_schema', 'read_xlsx_with_schema', 'read_jsonl_with_schema',
    'read_parquet_with_schema', 'read_delta_with_schema'
]

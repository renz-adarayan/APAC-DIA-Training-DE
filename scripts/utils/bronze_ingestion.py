"""Bronze ingestion orchestration utilities.

This module depends on low-level IO primitives from ``bronze_io`` and adds:
  * Manifest / idempotency helpers (already_processed, mark_processed)
  * Table partition strategy selection
  * Generic file ingestion pipeline (ingest_file_to_bronze)
  * Delta UPSERT logic for the returns table (upsert_returns_delta)

It deliberately keeps business / orchestration concerns separate from the pure
IO helpers defined in ``bronze_io`` for testability and separation of concerns.
"""
from __future__ import annotations

import datetime as dt
import pathlib
from typing import Optional

import pyarrow as pa

from .bronze_io import (
    calculate_file_hash,
    write_parquet_partitioned,
    write_delta,
)

# ---------------------------------------------------------------------------
# Partition strategy
# ---------------------------------------------------------------------------

def get_partitioning_strategy(table_name: str):
    """Return partitioning strategy for Bronze tables.

    Most early Bronze tables are written unpartitioned; partitioning is largely
    deferred to Silver where derived columns (e.g. month) are materialised.
    Currently only sensors benefits from store_id partitioning at Bronze.
    """
    if table_name == 'sensors':
        return ['store_id']
    return None

# ---------------------------------------------------------------------------
# Manifest helpers (DuckDB connection expected)
# ---------------------------------------------------------------------------

def already_processed(conn, p: pathlib.Path) -> bool:
    return conn.execute(
        "SELECT 1 FROM manifest_processed_files WHERE src_path = ?",
        [str(p)]
    ).fetchone() is not None


def mark_processed(
    conn,
    src_path: pathlib.Path,
    row_count: int,
    reject_count: int = 0,
    file_hash: str | None = None,
    status: str = 'SUCCESS',
    error_message: str | None = None,
    file_size_bytes: int | None = None,
    processing_duration_ms: int | None = None,
):
    conn.execute(
        """
        INSERT OR REPLACE INTO manifest_processed_files
        (src_path, processed_at, row_count, reject_count, file_hash, status, error_message, file_size_bytes, processing_duration_ms)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            str(src_path), dt.datetime.utcnow(), row_count, reject_count, file_hash,
            status, error_message, file_size_bytes, processing_duration_ms
        ]
    )

# ---------------------------------------------------------------------------
# UPSERT logic for returns (Delta)
# ---------------------------------------------------------------------------

def upsert_returns_delta(new_data_table: pa.Table, delta_path: pathlib.Path | str, primary_key: str = 'return_id'):
    """Merge/upsert logic for returns table with soft deletes and schema evolution.

    Returns tuple: (rows_inserted, rows_updated, rows_deleted)
    """
    try:
        from deltalake import DeltaTable
        from deltalake.writer import write_deltalake
    except ImportError as e:  # pragma: no cover
        raise ImportError("deltalake package required for UPSERT operations") from e

    import pyarrow.compute as pc

    delta_path = pathlib.Path(delta_path)

    # Table create path
    if not delta_path.exists() or not any(delta_path.iterdir()):
        print("  • Delta table does not exist, creating new table with initial data")
        write_deltalake(str(delta_path), new_data_table, mode='overwrite')
        return len(new_data_table), 0, 0

    print("  • Reading existing Delta table for UPSERT operation")
    dtbl = DeltaTable(str(delta_path))
    existing = dtbl.to_pyarrow_table()
    print(f"    Existing records: {len(existing)}")
    print(f"    New/updated records: {len(new_data_table)}")

    existing_cols = set(existing.column_names)
    new_cols = set(new_data_table.column_names)
    all_cols = existing_cols | new_cols

    # Schema evolution – add missing columns as nulls on either side
    if existing_cols != new_cols:
        print("  • Schema evolution detected:")
        print(f"    Existing columns: {sorted(existing_cols)}")
        print(f"    New columns: {sorted(new_cols)}")

        missing_in_existing = new_cols - existing_cols
        for col in sorted(missing_in_existing):
            col_type = new_data_table.schema.field(col).type
            existing = existing.append_column(col, pa.nulls(len(existing), type=col_type))

        missing_in_new = existing_cols - new_cols
        for col in sorted(missing_in_new):
            col_type = existing.schema.field(col).type
            new_data_table = new_data_table.append_column(col, pa.nulls(len(new_data_table), type=col_type))

    order = sorted(all_cols)
    existing = existing.select(order)
    new_data_table = new_data_table.select(order)

    if primary_key not in new_data_table.column_names:
        raise ValueError(f"Primary key '{primary_key}' not found in new data columns: {new_data_table.column_names}")

    new_keys = new_data_table.column(primary_key).to_pylist()
    print(f"  • Processing UPSERT for {len(new_keys)} records with primary keys: {new_keys[:5]}{'...' if len(new_keys) > 5 else ''}")

    rows_deleted = 0

    # Soft delete detection (qty=0 & reason='DELETED')
    if 'qty' in new_data_table.column_names and 'reason' in new_data_table.column_names:
        qty_col = new_data_table.column('qty')
        reason_col = new_data_table.column('reason')
        tombstone_mask = pc.and_(pc.equal(qty_col, 0), pc.equal(reason_col, pa.scalar('DELETED')))
        # Build indices for tombstones
        if tombstone_mask.null_count == 0:  # mask is a boolean array
            import pyarrow.compute as pc2
            tombstone_indices = [i for i, v in enumerate(tombstone_mask.to_pylist()) if v]
            if tombstone_indices:
                tombstone_keys = pc2.take(new_data_table.column(primary_key), pa.array(tombstone_indices)).to_pylist()
                print(f"  • Found {len(tombstone_keys)} soft delete records (tombstones): {tombstone_keys}")
                rows_deleted = len(tombstone_keys)
                existing_keys = existing.column(primary_key)
                keep_mask_existing = pc.invert(pc.is_in(existing_keys, pa.array(tombstone_keys)))
                existing = pc.filter(existing, keep_mask_existing)
                keep_mask_new = pa.array([(i not in tombstone_indices) for i in range(len(new_data_table))])
                new_data_table = pc.filter(new_data_table, keep_mask_new)
                new_keys = [k for k in new_keys if k not in tombstone_keys]
                print(f"  • After tombstone processing: {len(new_data_table)} records to insert/update")

    existing_keys = existing.column(primary_key)
    update_mask = pc.is_in(new_data_table.column(primary_key), existing_keys)
    update_records = pc.filter(new_data_table, update_mask)
    insert_records = pc.filter(new_data_table, pc.invert(update_mask))
    rows_updated = len(update_records)
    rows_inserted = len(insert_records)
    print(f"  • UPSERT breakdown: {rows_inserted} inserts, {rows_updated} updates, {rows_deleted} deletes")

    if rows_updated > 0:
        update_keys = update_records.column(primary_key)
        keep_mask = pc.invert(pc.is_in(existing_keys, update_keys))
        existing = pc.filter(existing, keep_mask)

    if len(existing) > 0 and len(new_data_table) > 0:
        merged = pa.concat_tables([existing, new_data_table])
    elif len(new_data_table) > 0:
        merged = new_data_table
    else:
        merged = existing

    print(f"  • Final merged table: {len(merged)} total records")
    write_deltalake(str(delta_path), merged, mode='overwrite')
    print(f"  • UPSERT completed: {rows_inserted} inserted, {rows_updated} updated, {rows_deleted} deleted")
    return rows_inserted, rows_updated, rows_deleted

# ---------------------------------------------------------------------------
# Generic ingestion wrapper
# ---------------------------------------------------------------------------

def ingest_file_to_bronze(
    src_path: pathlib.Path,
    table_name: str,
    schema: pa.Schema,
    read_func,
    lake_root: pathlib.Path,
    conn,
    dry_run: bool = False,
):
    if not src_path.exists():
        print(f"{table_name.title()} file not found: {src_path}")
        return
    if not dry_run and already_processed(conn, src_path):
        print(f"{table_name.title()} file already processed: {src_path}")
        return

    print(f"Processing {table_name} file: {src_path}")

    if src_path.is_file():
        file_size_bytes = src_path.stat().st_size
    elif src_path.is_dir():
        file_size_bytes = sum(f.stat().st_size for f in src_path.rglob('*') if f.is_file())
    else:
        file_size_bytes = 0

    if dry_run:
        print(f"DRY RUN: Would process {src_path} ({file_size_bytes} bytes)")
        return

    start_time = dt.datetime.utcnow()
    reject_count = 0
    error_message = None
    status = 'SUCCESS'
    file_hash = None

    try:
        print("  • Calculating file hash for integrity...")
        file_hash = calculate_file_hash(src_path)
        print("  • Loading raw data...")
        raw_table = read_func(src_path, schema, validate=False)
        print("  • Performing enhanced schema validation...")
        src_filename = src_path.name

        try:
            from scripts.utils.validation_utils import (
                validate_table_with_errors, write_rejects_to_lake, write_rejects_summary
            )
        except ImportError:
            print("  • Validation utilities not available, using basic validation...")
            tbl = raw_table.cast(schema, safe=False)
            now = pa.scalar(dt.datetime.utcnow(), type=pa.timestamp('us'))
            tbl = tbl.append_column('src_filename', pa.array([src_filename]*len(tbl)))
            tbl = tbl.append_column('src_row_hash', pa.array(['basic_hash']*len(tbl)))
            tbl = tbl.append_column('ingestion_ts', pa.array([now.as_py()]*len(tbl), type=pa.timestamp('us')))
            validation_result = None
        else:
            validation_result = validate_table_with_errors(raw_table, schema, src_filename)
            if validation_result.invalid_rows > 0:
                print(f"  • Found {validation_result.invalid_rows} invalid rows out of {validation_result.total_rows}")
                print(f"  • Success rate: {validation_result.success_rate:.1f}%")
                print(f"  • Error breakdown: {validation_result.error_summary}")
                reject_count = write_rejects_to_lake(validation_result.invalid_records, table_name, lake_root, start_time)
                write_rejects_summary(validation_result, table_name, lake_root, start_time)
            tbl = validation_result.valid_table
            if tbl is None or len(tbl) == 0:
                print("  • No valid records to process after validation")
                duration_ms = int((dt.datetime.utcnow() - start_time).total_seconds() * 1000)
                mark_processed(
                    conn, src_path, 0, reject_count, file_hash, 'SUCCESS',
                    f"All {validation_result.total_rows} rows rejected during validation",
                    file_size_bytes, duration_ms
                )
                return

        partitioning = get_partitioning_strategy(table_name)
        partition_info = f" with partitioning by {partitioning}" if partitioning else " without partitioning"
        print(f"  • Writing {len(tbl)} valid records to Parquet and Delta formats{partition_info}...")
        pq_base = lake_root / 'bronze' / 'parquet' / table_name
        dl_base = lake_root / 'bronze' / 'delta' / table_name
        write_parquet_partitioned(tbl, pq_base, partitioning=partitioning)
        write_delta(tbl, dl_base, mode='append', partition_by=partitioning)

        duration_ms = int((dt.datetime.utcnow() - start_time).total_seconds() * 1000)
        mark_processed(
            conn, src_path, len(tbl), reject_count, file_hash, status, error_message,
            file_size_bytes, duration_ms
        )
        print(
            f"✅ Successfully processed {table_name}: {len(tbl)} valid rows, {reject_count} rejects in {duration_ms}ms"
        )
        print(f"   File hash: {file_hash[:16]}... | Size: {file_size_bytes} bytes")
        if 'validation_result' in locals() and validation_result and validation_result.invalid_rows > 0:
            print(f"   Data quality: {validation_result.success_rate:.1f}% success rate")

    except Exception as e:  # pragma: no cover
        duration_ms = int((dt.datetime.utcnow() - start_time).total_seconds() * 1000)
        status = 'FAILED'
        error_message = str(e)
        mark_processed(conn, src_path, 0, reject_count, file_hash, status, error_message, file_size_bytes, duration_ms)
        print(f"❌ Failed to process {table_name} file: {error_message}")
        raise

__all__ = [
    'get_partitioning_strategy',
    'already_processed', 'mark_processed',
    'ingest_file_to_bronze',
    'upsert_returns_delta',
]

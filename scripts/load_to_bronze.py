# Ingest raw files into Bronze (Parquet + Delta), with schema validation, partitioning,
# rejects, and manifest tracking in DuckDB.
# Usage: python scripts/load_to_bronze.py --raw data_raw --lake lake --manifest duckdb/warehouse.duckdb
import argparse, pathlib, os, hashlib, json, datetime as dt, sys
import duckdb
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
            row_count BIGINT
        )
    ''')

def already_processed(conn, p): return conn.execute("SELECT 1 FROM manifest_processed_files WHERE src_path = ?", [str(p)]).fetchone() is not None
def mark_processed(conn, p, n): conn.execute("INSERT OR REPLACE INTO manifest_processed_files VALUES (?, ?, ?)", [str(p), dt.datetime.utcnow(), n])

def write_parquet_partitioned(table, base_path, partitioning=None):
    pads.write_dataset(table, base_dir=str(base_path), format='parquet', partitioning=partitioning, existing_data_behavior='overwrite_or_ignore')

def write_delta(table, base_path, mode='append', partition_by=None, merge_schema=False):
    if write_deltalake is None:
        raise RuntimeError('deltalake not installed')
    write_deltalake(str(base_path), table=table, mode=mode, partition_by=partition_by or [], overwrite_schema=False, engine='rust', schema_mode='merge' if merge_schema else 'fail')

def load_customers(raw_root, lake_root, conn, dry_run=False):
    src = raw_root/'customers.csv'
    if not src.exists(): 
        print(f"Customers file not found: {src}")
        return
    if not dry_run and already_processed(conn, src): 
        print(f"Customers file already processed: {src}")
        return
    
    print(f"Processing customers file: {src}")
    
    if dry_run:
        print(f"DRY RUN: Would process {src} ({src.stat().st_size} bytes)")
        return
    
    tbl = pacsv.read_csv(src, read_options=pacsv.ReadOptions(encoding='utf-8'))
    tbl = tbl.cast(customers_schema, safe=False)
    now = pa.scalar(dt.datetime.utcnow(), type=pa.timestamp('us'))
    tbl = tbl.append_column('ingestion_ts', pa.array([now.as_py()]*len(tbl), type=pa.timestamp('us')))
    pq_base = lake_root/'bronze'/'parquet'/'customers'
    dl_base = lake_root/'bronze'/'delta'/'customers'
    write_parquet_partitioned(tbl, pq_base, partitioning=None)
    write_delta(tbl, dl_base, mode='append')
    mark_processed(conn, src, len(tbl))
    print(f"Successfully processed customers: {len(tbl)} rows")

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

    print("✅ Bronze load completed for implemented loaders (extend for all tables).")

if __name__ == '__main__':
    main()

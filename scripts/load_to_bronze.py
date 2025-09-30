# Ingest raw files into Bronze (Parquet + Delta), with schema validation, partitioning,
# rejects, and manifest tracking in DuckDB.
# Usage: python scripts/load_to_bronze.py --raw data_raw --lake lake --manifest duckdb/warehouse.duckdb
import argparse
import pathlib
import datetime as dt
import sys

# Package import bootstrap
PROJECT_ROOT = pathlib.Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Import bronze utilities
from scripts.utils.bronze_database_utils import ensure_dirs, setup_duckdb_connection
from scripts.utils.bronze_partition_utils import update_all_partition_stats
from scripts.utils.bronze_loaders import (
    load_customers, load_products, load_stores, load_suppliers,
    load_exchange_rates, load_events, load_sensors, load_orders_header,
    load_orders_lines, load_shipments, load_returns
)

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
    ap.add_argument('--initial', action='store_true',
                    help='Initial loading mode: process ALL unprocessed events in weekly batches. Default is incremental mode (1 week per run)')
    ap.add_argument('--batch-size-days', type=int, default=7,
                    help='Batch size in days for event processing (default: 7 for weekly batches)')
    return ap.parse_args()


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
        conn = setup_duckdb_connection(args.manifest)
    else:
        conn = None

    load_customers(raw_root, lake_root, conn, args.dry_run)
    load_products(raw_root, lake_root, conn, args.dry_run)
    load_stores(raw_root, lake_root, conn, args.dry_run)
    load_suppliers(raw_root, lake_root, conn, args.dry_run)
    load_exchange_rates(raw_root, lake_root, conn, args.dry_run)
    load_events(raw_root, lake_root, conn, args.dry_run, cutoff_date=cutoff_date, 
                initial_mode=args.initial, batch_size_days=args.batch_size_days)
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

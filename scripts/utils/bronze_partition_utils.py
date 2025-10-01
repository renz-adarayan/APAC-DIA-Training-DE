"""
Bronze partition management utilities.

This module handles partition discovery, pruning, and statistics collection
for the bronze layer data ingestion process.
"""

import pathlib
import datetime as dt
from typing import Optional


def should_prune_partition(partition_name: str, cutoff_date: Optional[dt.date], prefix: str) -> bool:
    """Return True if partition should be pruned based on cutoff date.
    
    Args:
        partition_name: Partition directory name (e.g., 'event_dt=2024-11-04')
        cutoff_date: Optional cutoff date for pruning
        prefix: Partition prefix to match (e.g., 'event_dt=' or 'order_dt=')
    
    Returns:
        bool: True if partition should be pruned (is older than cutoff)
    
    Examples:
        >>> should_prune_partition('event_dt=2024-11-04', dt.date(2024, 11, 10), 'event_dt=')
        True
        >>> should_prune_partition('order_dt=2024-11-02', dt.date(2024, 11, 01), 'order_dt=')
        False
    """
    if not cutoff_date:
        return False
    try:
        part_date_str = partition_name.split('=')[1]
        part_date = dt.date.fromisoformat(part_date_str)
    except Exception:
        return False
    return part_date < cutoff_date


def update_partition_stats_events(raw_root: pathlib.Path, conn):
    """Update partition statistics for events table.
    
    Args:
        raw_root: Path to raw data directory
        conn: DuckDB connection for manifest operations
    """
    events_base = raw_root / 'events'
    if not events_base.exists():
        return
    
    partitions = [p for p in events_base.iterdir() if p.is_dir() and p.name.startswith('event_dt=')]
    for p in partitions:
        files = list(p.glob('*.jsonl'))
        file_count = len(files)
        if file_count == 0:
            continue
        
        total_size = sum(f.stat().st_size for f in files)
        mtimes = [dt.datetime.utcfromtimestamp(f.stat().st_mtime) for f in files]
        
        # Determine processed status and row counts from manifest
        manifest_rows = conn.execute(
            "SELECT SUM(row_count), COUNT(*) FROM manifest_processed_files WHERE src_path IN (%s)" %
            ','.join(['?']*file_count), [str(f) for f in files]
        ).fetchone()
        
        sum_rows = manifest_rows[0] if manifest_rows[0] is not None else None
        processed_files = manifest_rows[1]
        fully_processed = processed_files == file_count
        
        conn.execute('''
            INSERT OR REPLACE INTO partition_stats
            (table_name, partition_path, partition_value, row_count, file_count, total_size_bytes, min_mtime, max_mtime, fully_processed)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', [
            'events', str(p), p.name.split('=')[1], sum_rows, file_count, total_size,
            min(mtimes), max(mtimes), fully_processed
        ])


def update_partition_stats_orders(raw_root: pathlib.Path, conn):
    """Update partition statistics for orders table.
    
    Args:
        raw_root: Path to raw data directory
        conn: DuckDB connection for manifest operations
    """
    orders_base = raw_root / 'orders'
    if not orders_base.exists():
        return
    
    partitions = [p for p in orders_base.iterdir() if p.is_dir() and p.name.startswith('order_dt=')]
    for p in partitions:
        files = list(p.glob('orders_*.csv'))  # header / lines
        if not files:
            continue
        
        total_size = sum(f.stat().st_size for f in files)
        mtimes = [dt.datetime.utcfromtimestamp(f.stat().st_mtime) for f in files]
        
        manifest_rows = conn.execute(
            "SELECT SUM(row_count), COUNT(*) FROM manifest_processed_files WHERE src_path IN (%s)" %
            ','.join(['?']*len(files)), [str(f) for f in files]
        ).fetchone()
        
        sum_rows = manifest_rows[0] if manifest_rows[0] is not None else None
        processed_files = manifest_rows[1]
        fully_processed = processed_files == len(files)
        
        conn.execute('''
            INSERT OR REPLACE INTO partition_stats
            (table_name, partition_path, partition_value, row_count, file_count, total_size_bytes, min_mtime, max_mtime, fully_processed)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', [
            'orders', str(p), p.name.split('=')[1], sum_rows, len(files), total_size,
            min(mtimes), max(mtimes), fully_processed
        ])


def update_partition_stats_sensors(raw_root: pathlib.Path, conn):
    """Update partition statistics for sensors table.
    
    Args:
        raw_root: Path to raw data directory
        conn: DuckDB connection for manifest operations
    """
    sensors_base = raw_root / 'sensors'
    if not sensors_base.exists():
        return
    
    store_partitions = [p for p in sensors_base.iterdir() if p.is_dir() and p.name.startswith('store_id=')]
    for store in store_partitions:
        month_parts = [m for m in store.iterdir() if m.is_dir() and m.name.startswith('month=')]
        for m in month_parts:
            files = list(m.glob('*.csv'))
            if not files:
                continue
            
            total_size = sum(f.stat().st_size for f in files)
            mtimes = [dt.datetime.utcfromtimestamp(f.stat().st_mtime) for f in files]
            
            manifest_rows = conn.execute(
                "SELECT SUM(row_count), COUNT(*) FROM manifest_processed_files WHERE src_path IN (%s)" %
                ','.join(['?']*len(files)), [str(f) for f in files]
            ).fetchone()
            
            sum_rows = manifest_rows[0] if manifest_rows[0] is not None else None
            processed_files = manifest_rows[1]
            fully_processed = processed_files == len(files)
            partition_value = f"{store.name.split('=')[1]}_{m.name.split('=')[1]}"
            
            conn.execute('''
                INSERT OR REPLACE INTO partition_stats
                (table_name, partition_path, partition_value, row_count, file_count, total_size_bytes, min_mtime, max_mtime, fully_processed)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', [
                'sensors', str(m), partition_value, sum_rows, len(files), total_size,
                min(mtimes), max(mtimes), fully_processed
            ])


def update_all_partition_stats(raw_root: pathlib.Path, conn):
    """Update partition statistics for all tables.
    
    Args:
        raw_root: Path to raw data directory
        conn: DuckDB connection for manifest operations
    """
    update_partition_stats_events(raw_root, conn)
    update_partition_stats_orders(raw_root, conn)
    update_partition_stats_sensors(raw_root, conn)


__all__ = [
    'should_prune_partition',
    'update_all_partition_stats',
]

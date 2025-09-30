"""
Bronze database setup utilities.

This module handles database and manifest initialization,
as well as directory structure setup for the bronze layer.
"""

import pathlib
import duckdb


def ensure_dirs(lake_root: pathlib.Path):
    """Create necessary directory structure for the bronze layer.
    
    Args:
        lake_root: Path to the lake root directory
    """
    for sub in ['bronze/parquet', 'bronze/delta']:
        (lake_root / sub).mkdir(parents=True, exist_ok=True)
    (lake_root / '_rejects').mkdir(parents=True, exist_ok=True)


def init_manifest(conn):
    """Initialize manifest tables in DuckDB for tracking processed files and partition statistics.
    
    Args:
        conn: DuckDB connection object
    """
    # Main manifest table for tracking processed files
    conn.execute('''
        CREATE TABLE IF NOT EXISTS manifest_processed_files (
            src_path TEXT PRIMARY KEY,
            processed_at TIMESTAMP,
            row_count BIGINT,
            reject_count BIGINT DEFAULT 0,
            file_hash TEXT,
            status TEXT DEFAULT 'SUCCESS',
            error_message TEXT,
            file_size_bytes BIGINT,
            processing_duration_ms INTEGER
        )
    ''')
    
    # Partition statistics registry
    conn.execute('''
        CREATE TABLE IF NOT EXISTS partition_stats (
            table_name TEXT,
            partition_path TEXT,
            partition_value TEXT,
            row_count BIGINT,
            file_count INTEGER,
            total_size_bytes BIGINT,
            min_mtime TIMESTAMP,
            max_mtime TIMESTAMP,
            fully_processed BOOLEAN,
            collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (table_name, partition_path)
        )
    ''')


def setup_duckdb_connection(manifest_path: str):
    """Set up and configure DuckDB connection with required extensions.
    
    Args:
        manifest_path: Path to the DuckDB manifest database file
        
    Returns:
        DuckDB connection object with delta extension loaded
    """
    # Ensure manifest directory exists
    pathlib.Path(manifest_path).parent.mkdir(parents=True, exist_ok=True)
    
    # Create connection and load extensions
    conn = duckdb.connect(manifest_path)
    conn.execute("INSTALL delta; LOAD delta;")
    
    # Initialize manifest tables
    init_manifest(conn)
    
    return conn

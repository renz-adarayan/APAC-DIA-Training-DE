"""
Bronze batch processing utilities.

This module handles weekly batch processing logic for the bronze layer data ingestion,
including grouping partitions into batches and managing incremental vs initial loading modes.
"""

import pathlib
import datetime as dt
from typing import List
from scripts.utils.bronze_ingestion import already_processed


def group_partitions_by_weeks(partitions: List[pathlib.Path], batch_size_days: int = 7) -> List[List[pathlib.Path]]:
    """Group daily event partitions into weekly batches.
    
    Args:
        partitions: List of partition directories (event_dt=YYYY-MM-DD)
        batch_size_days: Number of days per batch (default 7 for weekly)
    
    Returns:
        List of batches, where each batch is a list of partition directories
    """
    if not partitions:
        return []
    
    # Sort partitions by date (oldest first for processing)
    partitions_sorted = sorted(partitions, key=lambda x: x.name.split('=')[1])
    
    # Group into batches
    batches = []
    current_batch = []
    
    for partition in partitions_sorted:
        current_batch.append(partition)
        
        # When we reach batch_size_days, start a new batch
        if len(current_batch) >= batch_size_days:
            batches.append(current_batch)
            current_batch = []
    
    # Add remaining partitions as the final batch
    if current_batch:
        batches.append(current_batch)
    
    return batches


def get_unprocessed_partitions(partitions: List[pathlib.Path], conn, dry_run: bool = False) -> List[pathlib.Path]:
    """Filter partitions to only include those with unprocessed files.
    
    Args:
        partitions: List of partition directories
        conn: DuckDB connection for manifest checking
        dry_run: If True, treat all partitions as unprocessed
    
    Returns:
        List of partitions that contain unprocessed JSONL files
    """
    if dry_run or not conn:
        return partitions
    
    unprocessed = []
    
    for partition_dir in partitions:
        # Find JSONL files in this partition
        jsonl_files = list(partition_dir.glob('*.jsonl'))
        
        if not jsonl_files:
            continue
        
        # Check if any files in this partition are unprocessed
        has_unprocessed = False
        for jsonl_file in jsonl_files:
            if not already_processed(conn, jsonl_file):
                has_unprocessed = True
                break
        
        if has_unprocessed:
            unprocessed.append(partition_dir)
    
    return unprocessed


def get_next_unprocessed_batch(partitions: List[pathlib.Path], conn, batch_size_days: int = 7, dry_run: bool = False) -> List[pathlib.Path]:
    """Get the next unprocessed batch for incremental loading.
    
    Args:
        partitions: List of all partition directories
        conn: DuckDB connection for manifest checking
        batch_size_days: Number of days per batch
        dry_run: If True, treat all partitions as unprocessed
    
    Returns:
        List of partition directories for the next batch to process
    """
    unprocessed = get_unprocessed_partitions(partitions, conn, dry_run)
    
    if not unprocessed:
        return []
    
    # Get weekly batches
    batches = group_partitions_by_weeks(unprocessed, batch_size_days)
    
    # Return the first batch (earliest dates)
    return batches[0] if batches else []


def get_all_unprocessed_batches(partitions: List[pathlib.Path], conn, batch_size_days: int = 7, dry_run: bool = False) -> List[List[pathlib.Path]]:
    """Get all unprocessed batches for initial loading.
    
    Args:
        partitions: List of all partition directories
        conn: DuckDB connection for manifest checking
        batch_size_days: Number of days per batch
        dry_run: If True, treat all partitions as unprocessed
    
    Returns:
        List of batches, where each batch is a list of partition directories
    """
    unprocessed = get_unprocessed_partitions(partitions, conn, dry_run)
    
    if not unprocessed:
        return []
    
    return group_partitions_by_weeks(unprocessed, batch_size_days)


__all__ = [
    'get_all_unprocessed_batches',
    'get_next_unprocessed_batch',
]

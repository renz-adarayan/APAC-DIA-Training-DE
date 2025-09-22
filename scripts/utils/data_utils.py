"""Data utility functions for data generation."""
import random
import pathlib
from datetime import datetime, timedelta, date
from decimal import Decimal
import numpy as np


def apply_scale_to_targets(base_count, scale):
    """Apply scaling factor to target row counts"""
    return max(1, int(base_count * scale))


def create_partitioned_path(base_path, partition_cols, values):
    """Create partitioned directory structure like Hive/Spark partitioning
    
    Args:
        base_path: Base directory path
        partition_cols: List of partition column names
        values: List of partition values corresponding to columns
    
    Returns:
        pathlib.Path: Full partitioned path
    """
    parts = [f"{col}={val}" for col, val in zip(partition_cols, values)]
    return base_path / "/".join(parts)


def generate_date_range(start_date, end_date):
    """Generate list of dates between start_date and end_date (inclusive)
    
    Args:
        start_date: datetime.date - start date
        end_date: datetime.date - end date
    
    Returns:
        List[datetime.date]: List of dates
    """
    dates = []
    current = start_date
    while current <= end_date:
        dates.append(current)
        current += timedelta(days=1)
    return dates


def inject_foreign_key_violations(valid_ids, violation_rate=0.01, max_invalid_id=999999):
    """Inject controlled foreign key violations into a list of valid IDs
    
    Args:
        valid_ids: List of valid foreign key values
        violation_rate: Fraction of IDs to make invalid (default 1%)
        max_invalid_id: Maximum invalid ID to generate
    
    Returns:
        List: Modified list with some invalid foreign keys
    """
    if not valid_ids:
        return valid_ids
    
    ids = valid_ids.copy()
    num_violations = max(1, int(len(ids) * violation_rate))
    violation_indices = random.sample(range(len(ids)), num_violations)
    
    for idx in violation_indices:
        # Generate invalid ID that doesn't exist in valid range
        invalid_id = random.randint(max(valid_ids) + 1, max_invalid_id)
        ids[idx] = invalid_id
    
    return ids


def generate_business_hours_timestamp(base_date, timezone_offset_hours=10):
    """Generate realistic business hours timestamp for given date
    
    Args:
        base_date: datetime.date - base date
        timezone_offset_hours: UTC offset for local timezone (default +10 for AU)
    
    Returns:
        datetime: Timestamp with realistic business hours distribution
    """
    # Weight business hours (8AM-10PM) more heavily
    hour_weights = [0.01] * 8 + [0.08] * 14 + [0.02] * 2  # 24 hours
    hour = random.choices(range(24), weights=hour_weights)[0]
    minute = random.randint(0, 59)
    second = random.randint(0, 59)
    microsecond = random.randint(0, 999999)
    
    # Create timezone-aware timestamp
    dt = datetime.combine(base_date, datetime.min.time().replace(
        hour=hour, minute=minute, second=second, microsecond=microsecond
    ))
    
    return dt


def ensure_dir(p): 
    """Ensure directory exists, creating it if necessary"""
    pathlib.Path(p).mkdir(parents=True, exist_ok=True)

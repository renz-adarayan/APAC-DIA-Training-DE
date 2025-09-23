"""Sensor data generator module."""
import random
import csv
from datetime import datetime, timedelta, date
from pathlib import Path
from typing import List
import pyarrow as pa

from utils.data_utils import apply_scale_to_targets, create_partitioned_path, ensure_dir
from utils.schema_utils import get_column_names
from utils.constants import TARGET_ROWS


def generate_sensors_data(schema: pa.Schema, scale: float, output_path: Path) -> int:
    """Generate sensors data and write to partitioned CSV files.
    
    Args:
        schema: PyArrow schema for sensors
        scale: Scaling factor for number of records
        output_path: Base path to write the partitioned CSV files
        
    Returns:
        int: Total number of sensor readings generated
    """
    num_sensors = apply_scale_to_targets(TARGET_ROWS['sensors'], scale)
    column_names = get_column_names(schema)
    
    # Calculate realistic store count based on scale
    base_store_count = apply_scale_to_targets(TARGET_ROWS['stores'], scale)
    
    # Sensor configuration
    reading_interval_minutes = 20  # Reading every 20 minutes
    
    # Date range for 2024 (full year)
    start_date = date(2024, 1, 1)
    end_date = date(2024, 12, 31)
    
    total_records_written = 0
    
    # Calculate anomaly counts (0.5% out-of-range as specified)
    num_temp_anomalies = max(1, int(num_sensors * 0.005))
    num_humidity_anomalies = max(1, int(num_sensors * 0.005))
    num_missing_ts = max(1, int(num_sensors * 0.002))  # 0.2% missing timestamps
    
    anomaly_counters = {
        'temp_anomalies': 0,
        'humidity_anomalies': 0,
        'missing_ts': 0
    }
    
    # Generate shelf IDs pool
    shelf_sections = ['A', 'B', 'C', 'D', 'E', 'F']
    shelf_numbers = [f"{i:02d}" for i in range(1, 21)]  # 01-20
    
    # Generate data by store and month for partitioning
    for store_id in range(1, base_store_count + 1):
        # Generate shelf IDs for this store
        store_shelf_count = random.randint(3, 8)
        store_shelves = []
        for i in range(store_shelf_count):
            section = random.choice(shelf_sections)
            number = random.choice(shelf_numbers)
            shelf_id = f"SHELF-{section}{number}"
            store_shelves.append(shelf_id)
        
        # Process each month in 2024
        current_date = start_date
        while current_date <= end_date:
            month_start = current_date.replace(day=1)
            if month_start.month == 12:
                month_end = date(month_start.year + 1, 1, 1) - timedelta(days=1)
            else:
                month_end = date(month_start.year, month_start.month + 1, 1) - timedelta(days=1)
            
            # Don't go beyond our end date
            month_end = min(month_end, end_date)
            
            # Create partition path: sensors/store_id={id}/month={YYYY-MM}
            partition_path = create_partitioned_path(
                output_path, 
                ['store_id', 'month'], 
                [store_id, month_start.strftime('%Y-%m')]
            )
            ensure_dir(partition_path)
            
            # Write sensors data for this store/month partition
            file_path = partition_path / 'sensors.csv'
            month_records = _write_month_data(
                file_path, 
                column_names, 
                store_id, 
                store_shelves, 
                month_start, 
                month_end, 
                reading_interval_minutes,
                anomaly_counters,
                num_temp_anomalies,
                num_humidity_anomalies,
                num_missing_ts
            )
            
            total_records_written += month_records
            
            # Break if we've written enough records
            if total_records_written >= num_sensors:
                return total_records_written
            
            # Move to next month
            if current_date.month == 12:
                current_date = date(current_date.year + 1, 1, 1)
            else:
                current_date = date(current_date.year, current_date.month + 1, 1)
    
    return total_records_written


def _write_month_data(
    file_path: Path,
    column_names: List[str],
    store_id: int,
    store_shelves: List[str],
    month_start: date,
    month_end: date,
    reading_interval_minutes: int,
    anomaly_counters: dict,
    num_temp_anomalies: int,
    num_humidity_anomalies: int,
    num_missing_ts: int
) -> int:
    """Write sensor data for a specific store and month."""
    records_written = 0
    
    with file_path.open('w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
        writer.writerow(column_names)
        
        # Generate readings for each day in the month
        current_date = month_start
        while current_date <= month_end:
            
            # Generate readings for each shelf
            for shelf_id in store_shelves:
                
                # Generate readings throughout the day
                current_time = datetime.combine(current_date, datetime.min.time())
                end_of_day = current_time + timedelta(days=1)
                
                # Initial battery level (starts high, degrades over time)
                days_from_start = (current_date - date(2024, 1, 1)).days
                base_battery = 3200 - (days_from_start * 2)  # Gradual degradation
                battery_noise = random.randint(-100, 100)
                battery_mv = max(2000, base_battery + battery_noise)
                
                while current_time < end_of_day:
                    # Generate realistic temperature (18-26°C for retail)
                    base_temp = 22.0  # Comfortable retail temperature
                    seasonal_variation = 2 * (current_date.month - 6.5) / 6.5  # Slight seasonal variation
                    daily_variation = 1.5 * ((current_time.hour - 12) / 12)  # Slight daily variation
                    temp_noise = random.uniform(-1.0, 1.0)
                    temperature_c = base_temp + seasonal_variation + daily_variation + temp_noise
                    
                    # Inject temperature anomalies (0.5% out-of-range)
                    if (anomaly_counters['temp_anomalies'] < num_temp_anomalies and 
                        random.random() < 0.01):  # Higher chance when we need anomalies
                        if random.random() < 0.5:
                            temperature_c = random.uniform(-5.0, 0.0)  # Too cold
                        else:
                            temperature_c = random.uniform(50.0, 60.0)  # Too hot
                        anomaly_counters['temp_anomalies'] += 1
                    
                    # Generate realistic humidity (30-70% for retail)
                    base_humidity = 50.0
                    humidity_noise = random.uniform(-8.0, 8.0)
                    humidity_pct = max(25.0, min(75.0, base_humidity + humidity_noise))
                    
                    # Inject humidity anomalies (0.5% out-of-range)
                    if (anomaly_counters['humidity_anomalies'] < num_humidity_anomalies and 
                        random.random() < 0.01):  # Higher chance when we need anomalies
                        if random.random() < 0.5:
                            humidity_pct = random.uniform(5.0, 10.0)  # Too low
                        else:
                            humidity_pct = random.uniform(90.0, 95.0)  # Too high
                        anomaly_counters['humidity_anomalies'] += 1
                    
                    # Handle missing sensor_ts anomaly
                    sensor_ts_str = ""
                    if (anomaly_counters['missing_ts'] < num_missing_ts and 
                        random.random() < 0.005):  # Occasional missing timestamp
                        sensor_ts_str = ""  # Empty for missing
                        anomaly_counters['missing_ts'] += 1
                    else:
                        sensor_ts_str = current_time.isoformat()
                    
                    # Format decimal values to match schema precision (5,2)
                    temperature_str = f"{temperature_c:.2f}"
                    humidity_str = f"{humidity_pct:.2f}"
                    
                    # Write sensor reading
                    writer.writerow([
                        sensor_ts_str,          # sensor_ts (timestamp or empty)
                        store_id,               # store_id
                        shelf_id,               # shelf_id
                        temperature_str,        # temperature_c
                        humidity_str,           # humidity_pct
                        battery_mv              # battery_mv
                    ])
                    
                    records_written += 1
                    
                    # Move to next reading time
                    current_time += timedelta(minutes=reading_interval_minutes)
            
            # Move to next day
            current_date += timedelta(days=1)
    
    return records_written

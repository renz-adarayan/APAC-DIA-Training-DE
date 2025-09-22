"""Store data generator module."""
import random
import string
import csv
from datetime import date, timedelta
from pathlib import Path
from typing import List
from faker import Faker
import pyarrow as pa

from utils.data_utils import apply_scale_to_targets
from utils.schema_utils import get_column_names
from utils.constants import TARGET_ROWS, AU_STATES, STATE_TO_REGION


def generate_stores_data(schema: pa.Schema, scale: float, output_path: Path) -> int:
    """Generate stores data and write to CSV file.
    
    Args:
        schema: PyArrow schema for stores
        scale: Scaling factor for number of records
        output_path: Path to write the CSV file
        
    Returns:
        int: Number of stores generated
    """
    fake = Faker('en_AU')
    num_stores = apply_scale_to_targets(TARGET_ROWS['stores'], scale)
    column_names = get_column_names(schema)
    
    # Track store codes for duplicate injection
    generated_store_codes: List[str] = []
    
    # Calculate number of duplicates to inject (0.2% rate similar to customers)
    num_duplicates: int = max(1, int(num_stores * 0.002))
    
    # Define pools
    channels = ['web', 'pos']
    characters = string.ascii_uppercase + string.digits
    
    with output_path.open('w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
        writer.writerow(column_names)
        
        for sid in range(1, num_stores + 1):
            # Store code generation with duplicate injection
            if sid <= num_duplicates and len(generated_store_codes) > 0:
                # Inject duplicate - reuse existing code
                store_code: str = random.choice(generated_store_codes)
            else:
                # Generate new store code following pattern STR-[A-Z0-9]{5}
                random_suffix: str = ''.join(random.choices(characters, k=5))
                store_code = f"STR-{random_suffix}"
                generated_store_codes.append(store_code)
            
            # Generate realistic store name using Faker
            name: str = f"{fake.company()} {random.choice(['Store', 'Outlet', 'Centre', 'Shop'])}"
            channel: str = random.choice(channels)
            state: str = random.choice(AU_STATES)
            region: str = STATE_TO_REGION[state]
            
            # Normal plausible lat/lon centered roughly around Australia
            lat: float = -44 + random.random()*10  # -44 to -34 (approx southern AU)
            lon: float = 112 + random.random()*40  # 112 to 152 (AU span)
            
            # Inject impossible lat/lon for ~0.3% of rows
            if random.random() < 0.003:
                if random.random() < 0.5:
                    lat = 123.456  # Impossible latitude
                else:
                    lon = 987.654  # Impossible longitude
            
            # Open date spread over a decade
            open_dt: date = date(2015,1,1) + timedelta(days=random.randint(0, 365*10))
            
            # ~10% closed stores with valid close date after open date
            close_dt_str: str
            if random.random() < 0.10:
                close_dt: date = open_dt + timedelta(days=random.randint(30, 365*5))
                close_dt_str = close_dt.isoformat()
            else:
                close_dt_str = ''  # Active store (nullable)
            
            # Write row using CSV writer for proper escaping
            writer.writerow([
                sid,                        # store_id
                store_code,                 # store_code
                name,                       # name
                channel,                    # channel
                region,                     # region
                state,                      # state
                f"{lat:.6f}",              # latitude
                f"{lon:.6f}",              # longitude
                open_dt.isoformat(),       # open_dt
                close_dt_str               # close_dt
            ])
    
    return num_stores

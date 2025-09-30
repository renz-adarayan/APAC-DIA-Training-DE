"""Store data generator module."""
import random
import string
import csv
import math
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
    duplicate_injected_keys: List[str] = []  # records each duplicate occurrence
    
    # Calculate number of duplicates to inject (0.2% exact rate) - always round up
    num_duplicates: int = max(1, math.ceil(num_stores * 0.002))
    dupes_created: int = 0  # how many duplicate rows emitted so far
    duplicated_keys = set()  # track which store codes already received one duplicate
    
    # Define pools
    channels = ['web', 'pos']
    characters = string.ascii_uppercase + string.digits
    
    with output_path.open('w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
        writer.writerow(column_names)
        
        for sid in range(1, num_stores + 1):
            if sid == 1:
                # Always start with a unique key.
                random_suffix: str = ''.join(random.choices(characters, k=5))
                store_code = f"STR-{random_suffix}"
                generated_store_codes.append(store_code)
            else:
                duplicates_remaining = num_duplicates - dupes_created
                rows_remaining = num_stores - sid + 1
                # We can inject if we still have duplicate quota AND there exists a key not yet duplicated.
                available_for_dup = [k for k in generated_store_codes if k not in duplicated_keys]
                can_inject = duplicates_remaining > 0 and len(available_for_dup) > 0
                # Must inject if we are running out of rows (ensure quota fulfillment)
                must_inject = can_inject and rows_remaining == duplicates_remaining
                # Probabilistic early injection to spread duplicates; adjust probability if needed.
                should_inject = can_inject and (must_inject or random.random() < 0.35)

                if should_inject:
                    store_code = random.choice(available_for_dup)
                    duplicate_injected_keys.append(store_code)
                    duplicated_keys.add(store_code)
                    dupes_created += 1
                else:
                    # Generate a new unique store code.
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
    
    # Post-generation summary
    if duplicate_injected_keys:
        unique_dupes = sorted(set(duplicate_injected_keys))
        print(
            f"Summary: Injected {len(duplicate_injected_keys)} duplicate occurrences across "
            f"{len(unique_dupes)} unique store_codes: {', '.join(unique_dupes)}"
        )
    else:
        print("Summary: No duplicate store_codes were injected (unexpected).")

    return num_stores

"""Supplier data generator module."""
import random
import string
import csv
import numpy as np
from pathlib import Path
from faker import Faker
import pyarrow as pa

from utils.data_utils import apply_scale_to_targets
from utils.schema_utils import get_column_names
from utils.constants import TARGET_ROWS, SUPPLIER_COUNTRIES


def generate_suppliers_data(schema: pa.Schema, scale: float, output_path: Path) -> int:
    """Generate suppliers data and write to CSV file.
    
    Args:
        schema: PyArrow schema for suppliers
        scale: Scaling factor for number of records
        output_path: Path to write the CSV file
        
    Returns:
        int: Number of suppliers generated
    """
    fake = Faker('en_AU')
    num_suppliers = apply_scale_to_targets(TARGET_ROWS['suppliers'], scale)
    column_names = get_column_names(schema)
    characters: str = string.ascii_uppercase + string.digits

    with output_path.open('w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
        writer.writerow(column_names)
        
        for sid in range(1, num_suppliers + 1):
            # Supplier code pattern SUP-XXXXXX (alphanumeric uppercase)
            code: str = 'SUP-' + ''.join(random.choices(characters, k=6))
            name: str = fake.company().replace(',', ' ')
            country: str = random.choice(SUPPLIER_COUNTRIES)
            
            # Lead time: normal-ish distribution (mean=25, std=12, clipped 1-90)
            lead_time_days: int = int(np.clip(np.random.normal(25, 12), 1, 90))
            
            # Preferred supplier flag (30% probability)
            preferred: bool = random.random() < 0.30
            
            # Write row using CSV writer for proper escaping
            writer.writerow([
                sid,                        # supplier_id
                code,                       # supplier_code
                name,                       # name
                country,                    # country_code
                lead_time_days,             # lead_time_days
                str(preferred).lower()      # preferred (boolean as lowercase string)
            ])
    
    return num_suppliers

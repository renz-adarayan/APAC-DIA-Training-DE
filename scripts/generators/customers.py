"""Customer data generator module."""
import random
import string
import csv
from datetime import datetime, timedelta, date
from pathlib import Path
from typing import List
from faker import Faker
import pyarrow as pa

from utils.data_utils import apply_scale_to_targets
from utils.schema_utils import get_column_names
from utils.constants import TARGET_ROWS, DATA_END_DATE


def generate_customers_data(schema: pa.Schema, scale: float, output_path: Path) -> int:
    """Generate customers data and write to CSV file.
    
    Args:
        schema: PyArrow schema for customers
        scale: Scaling factor for number of records
        output_path: Path to write the CSV file
    """
    fake = Faker('en_AU')
    num_customers = apply_scale_to_targets(TARGET_ROWS['customers'], scale)
    column_names = get_column_names(schema)
    
    # Track natural keys for duplicate injection
    generated_natural_keys: List[str] = []
    
    # Calculate number of duplicates to inject (0.2% exact rate)
    num_duplicates: int = max(1, int(num_customers * 0.002))
    
    with output_path.open('w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
        writer.writerow(column_names)
        
        for i in range(1, num_customers + 1):
            # Natural key generation with duplicate injection
            if i <= num_duplicates and len(generated_natural_keys) > 0:
                # Inject duplicate - reuse existing key
                nk: str = random.choice(generated_natural_keys)
            else:
                # Generate new natural key following pattern CUST-[A-Z0-9]{8}
                characters: str = string.ascii_uppercase + string.digits
                random_suffix: str = ''.join(random.choices(characters, k=8))
                nk = f"CUST-{random_suffix}"
                generated_natural_keys.append(nk)
            
            # Inject anomalies: 1% malformed emails
            email: str = fake.email() if random.random() > 0.01 else 'bad_email'
            
            # Australian coordinates (more accurate bounds)
            lat: float = -44 + random.random() * 10  # -44 to -34 (covers mainland AU)
            lon: float = 112 + random.random() * 40  # 112 to 152 (covers mainland AU)
            
            # Realistic birth date (1955-2007 per assumptions for 18-70 year olds)
            birth: date = date(1955, 1, 1) + timedelta(days=random.randint(0, 18993))  # 1955-2007
            
            # Join timestamp capped to DATA_END_DATE (central constant)
            year_start = datetime(DATA_END_DATE.year, 1, 1)
            max_offset_days = (datetime(DATA_END_DATE.year, 12, 31) - year_start).days
            join_ts: datetime = year_start + timedelta(
                days=random.randint(0, max_offset_days),
                seconds=random.randint(0, 86399)
            )
            
            # Name and contact data
            first_name: str = fake.first_name()
            last_name: str = fake.last_name()
            
            # Phone with some nulls (realistic for optional field)
            phone: str = fake.phone_number() if random.random() > 0.05 else ''
            
            # Address fields
            address_line1: str = fake.street_address()
            address_line2: str = fake.secondary_address() if random.random() < 0.3 else ''
            city: str = fake.city()
            state_region: str = fake.state_abbr()
            postcode: str = fake.postcode()
            country_code: str = 'AU'
            
            # VIP status (10% of customers)
            is_vip: bool = random.random() < 0.10
            
            # GDPR consent (realistic rate ~85% for recent customers)
            gdpr_consent: bool = random.random() < 0.85
            
            # Write row using CSV writer for proper escaping
            writer.writerow([
                i,                              # customer_id
                nk,                             # natural_key
                first_name,                     # first_name
                last_name,                      # last_name
                email,                          # email
                phone,                          # phone
                address_line1,                  # address_line1
                address_line2,                  # address_line2
                city,                           # city
                state_region,                   # state_region
                postcode,                       # postcode
                country_code,                   # country_code
                f"{lat:.6f}",                   # latitude
                f"{lon:.6f}",                   # longitude
                birth.isoformat(),              # birth_date
                join_ts.isoformat(),            # join_ts
                str(is_vip).lower(),            # is_vip
                str(gdpr_consent).lower()       # gdpr_consent
            ])
    
    return num_customers

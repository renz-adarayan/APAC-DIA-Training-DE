"""Customer data generator module."""
import random
import string
import csv
from datetime import datetime, timedelta, date
from faker import Faker

from utils.data_utils import apply_scale_to_targets
from utils.schema_utils import get_column_names
from utils.constants import TARGET_ROWS


def generate_customers_data(schema, scale, output_path):
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
    generated_natural_keys = []
    
    # Calculate number of duplicates to inject (0.2% exact rate)
    num_duplicates = max(1, int(num_customers * 0.002))
    
    with output_path.open('w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
        writer.writerow(column_names)
        
        for i in range(1, num_customers + 1):
            # Natural key generation with duplicate injection
            if i <= num_duplicates and len(generated_natural_keys) > 0:
                # Inject duplicate - reuse existing key
                nk = random.choice(generated_natural_keys)
            else:
                # Generate new natural key following pattern CUST-[A-Z0-9]{8}
                characters = string.ascii_uppercase + string.digits
                random_suffix = ''.join(random.choices(characters, k=8))
                nk = f"CUST-{random_suffix}"
                generated_natural_keys.append(nk)
            
            # Inject anomalies: 1% malformed emails
            email = fake.email() if random.random() > 0.01 else 'bad_email'
            
            # Australian coordinates (more accurate bounds)
            lat = -44 + random.random() * 10  # -44 to -34 (covers mainland AU)
            lon = 112 + random.random() * 40  # 112 to 152 (covers mainland AU)
            
            # Realistic birth date (1955-2007 per assumptions for 18-70 year olds)
            birth = date(1955, 1, 1) + timedelta(days=random.randint(0, 18993))  # 1955-2007
            
            # Join timestamp (2024 focus with some variety)
            join_ts = datetime(2024, 1, 1) + timedelta(
                days=random.randint(0, 400), 
                seconds=random.randint(0, 86399)
            )
            
            # Name and contact data
            first_name = fake.first_name()
            last_name = fake.last_name()
            
            # Phone with some nulls (realistic for optional field)
            phone = fake.phone_number() if random.random() > 0.05 else ''
            
            # Address fields
            address_line1 = fake.street_address()
            address_line2 = fake.secondary_address() if random.random() < 0.3 else ''
            city = fake.city()
            state_region = fake.state_abbr()
            postcode = fake.postcode()
            country_code = 'AU'
            
            # VIP status (10% of customers)
            is_vip = random.random() < 0.10
            
            # GDPR consent (realistic rate ~85% for recent customers)
            gdpr_consent = random.random() < 0.85
            
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

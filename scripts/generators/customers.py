"""Customer data generator module."""
import random
import string
import csv
import math
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

    # Helper to generate deliberately malformed email addresses for data quality tests
    def _malformed_email() -> str:
        base_local = fake.first_name().lower()
        base_domain = fake.domain_name().split('/')[-1].replace('www.', '')
        patterns = [
            lambda: base_local,                              # missing @ and domain
            lambda: f"{base_local}@",                        # missing domain part
            lambda: f"@{base_domain}",                       # missing local part
            lambda: f"{base_local}{base_domain}",            # missing @
            lambda: f"{base_local}@@{base_domain}",          # double @
            lambda: f"{base_local}@{base_domain}.",          # trailing dot
            lambda: f".{base_local}@{base_domain}",          # leading dot in local
            lambda: f"{base_local} @ {base_domain}",         # spaces around @
            lambda: f"{base_local}@{base_domain}..com",      # double dot in TLD
            lambda: f"{base_local}!@{base_domain}",          # illegal char !
            lambda: f"{base_local}@{base_domain.split('.')[0]}",  # missing TLD
            lambda: ''                                       # empty string
        ]
        try:
            return random.choice(patterns)()
        except Exception:
            return 'invalid'  # fallback
    
    # Track natural keys for duplicate injection
    generated_natural_keys: List[str] = []
    duplicate_injected_keys: List[str] = []  # records each duplicate occurrence
    
    # Calculate number of duplicates to inject (0.2% exact rate) - always round up
    num_duplicates: int = max(1, math.ceil(num_customers * 0.002))
    dupes_created: int = 0  # how many duplicate rows emitted so far
    duplicated_keys = set()  # track which natural keys already received one duplicate
    
    with output_path.open('w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
        writer.writerow(column_names)
        
        for i in range(1, num_customers + 1):
            if i == 1:
                # Always start with a unique key.
                characters: str = string.ascii_uppercase + string.digits
                random_suffix: str = ''.join(random.choices(characters, k=8))
                nk = f"CUST-{random_suffix}"
                generated_natural_keys.append(nk)
            else:
                duplicates_remaining = num_duplicates - dupes_created
                rows_remaining = num_customers - i + 1
                # We can inject if we still have duplicate quota AND there exists a key not yet duplicated.
                available_for_dup = [k for k in generated_natural_keys if k not in duplicated_keys]
                can_inject = duplicates_remaining > 0 and len(available_for_dup) > 0
                # Must inject if we are running out of rows (ensure quota fulfillment)
                must_inject = can_inject and rows_remaining == duplicates_remaining
                # Probabilistic early injection to spread duplicates; adjust probability if needed.
                should_inject = can_inject and (must_inject or random.random() < 0.35)

                if should_inject:
                    nk = random.choice(available_for_dup)
                    duplicate_injected_keys.append(nk)
                    duplicated_keys.add(nk)
                    dupes_created += 1
                else:
                    # Generate a new unique key.
                    characters: str = string.ascii_uppercase + string.digits
                    random_suffix: str = ''.join(random.choices(characters, k=8))
                    nk = f"CUST-{random_suffix}"
                    generated_natural_keys.append(nk)
            
            # Inject anomalies: 1% malformed emails (varied patterns instead of single token)
            email: str = fake.email() if random.random() > 0.01 else _malformed_email()
            
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
    
    # Post-generation summary
    if duplicate_injected_keys:
        unique_dupes = sorted(set(duplicate_injected_keys))
        print(
            f"Summary: Injected {len(duplicate_injected_keys)} duplicate occurrences across "
            f"{len(unique_dupes)} unique natural_keys: {', '.join(unique_dupes)}"
        )
    else:
        print("Summary: No duplicate natural_keys were injected (unexpected).")

    return num_customers

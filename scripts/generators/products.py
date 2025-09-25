"""Product data generator module."""
import csv
import random
import string
from datetime import date, timedelta
from pathlib import Path
import pyarrow as pa

from utils.data_utils import apply_scale_to_targets
from utils.schema_utils import get_column_names
from utils.constants import TARGET_ROWS, CATEGORY_HIERARCHY, BASE_PRICE_RANGES, DATA_END_DATE


def generate_products_data(schema: pa.Schema, scale: float, output_path: Path) -> int:
    """Generate products data and write to CSV file.
    
    Args:
        schema: PyArrow schema for products
        scale: Scaling factor for number of records
        output_path: Path to write the CSV file
        
    Returns:
        int: Number of products generated
    """
    num_products = apply_scale_to_targets(TARGET_ROWS['products'], scale)
    product_cols = get_column_names(schema)
    
    categories = list(CATEGORY_HIERARCHY.keys())
    characters = string.ascii_uppercase + string.digits
    
    # Price anomaly configuration
    price_anomaly_rate = 0.005
    num_price_anomalies = max(1, int(num_products * price_anomaly_rate))
    anomaly_indices = set(random.sample(range(1, num_products + 1), num_price_anomalies))
    
    with output_path.open('w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
        writer.writerow(product_cols)
        
        for pid in range(1, num_products + 1):
            sku = 'SKU-' + ''.join(random.choices(characters, k=6))
            cat = random.choice(categories)
            subcat = random.choice(CATEGORY_HIERARCHY[cat])
            name = f"{cat} {subcat} Item {pid}"
            # Cap introduced date so it does not exceed 2024-12-31
            cap_today = DATA_END_DATE
            introduced = cap_today - timedelta(days=random.randint(0, 365 * 5))
            
            # 10% discontinued products
            is_disc = random.random() < 0.10
            if is_disc:
                # 20% of discontinued missing discontinued_dt (anomaly for business rule)
                if random.random() < 0.20:
                    discontinued_dt_str = ''
                else:
                    discontinued_dt = introduced + timedelta(days=random.randint(30, 365*2))
                    # Guard future date overshoot
                    if discontinued_dt > cap_today:
                        discontinued_dt = cap_today - timedelta(days=random.randint(0,30))
                    discontinued_dt_str = discontinued_dt.isoformat()
            else:
                discontinued_dt_str = ''

            # Base price distribution by category
            low, high = BASE_PRICE_RANGES[cat]
            price_val = random.uniform(low, high)
            # Currency skew toward AUD with some USD/EUR
            currency = random.choices(['AUD','USD','EUR'], weights=[0.8,0.15,0.05])[0]

            # Inject price anomalies (missing or invalid)
            if pid in anomaly_indices:
                if random.random() < 0.5:
                    # Missing (empty string) -> will break strict decimal parse; accepted as anomaly
                    price_str = ''
                else:
                    # Invalid numeric (negative or too many decimals)
                    if random.random() < 0.5:
                        price_str = f"-{price_val:.4f}"  # negative
                    else:
                        price_str = f"{price_val:.6f}"   # too many decimals for scale=4
            else:
                price_str = f"{price_val:.4f}"  # Valid scale 4

            # Write row using CSV writer for proper escaping
            writer.writerow([
                pid,                          # product_id
                sku,                          # sku
                name,                         # name
                cat,                          # category
                subcat,                       # subcategory
                price_str,                    # current_price
                currency,                     # currency
                str(is_disc).lower(),         # is_discontinued (boolean -> lowercase)
                introduced.isoformat(),       # introduced_dt
                discontinued_dt_str           # discontinued_dt
            ])
    
    return num_products

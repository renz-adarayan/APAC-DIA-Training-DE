"""Events data generator module."""
import random
import json
from datetime import datetime, timedelta, date
from pathlib import Path
from faker import Faker
import pyarrow as pa

from utils.data_utils import apply_scale_to_targets, ensure_dir, generate_date_range
from utils.constants import TARGET_ROWS, EVENT_TYPES, EVENT_WEIGHTS, DATA_END_DATE


def generate_events_data(schema: pa.Schema, scale: float, output_path: Path) -> int:
    """Generate events data and write to JSONL files with date partitioning.
    
    Args:
        schema: PyArrow schema for events
        scale: Scaling factor for number of records
        output_path: Path to write the JSONL files
        
    Returns:
        int: Number of events generated
    """
    fake = Faker('en_AU')
    num_events = apply_scale_to_targets(TARGET_ROWS['events'], scale)
    
    # Use centralized DATA_END_DATE constant
    events_end_date = DATA_END_DATE
    events_start_date = events_end_date - timedelta(days=90)
    events_date_range = generate_date_range(events_start_date, events_end_date)
    
    # Create events directory
    events_dir = output_path / "events"
    ensure_dir(events_dir)
    
    # Generate customer pool (assuming customers exist with these IDs)
    # Scale customer IDs based on scale factor
    max_customer_id = apply_scale_to_targets(TARGET_ROWS['customers'], scale)
    customer_ids = list(range(1, max_customer_id + 1))
    
    # Product pool for product-related events
    max_product_id = apply_scale_to_targets(TARGET_ROWS['products'], scale)
    
    # Order pool for purchase events
    max_order_id = apply_scale_to_targets(TARGET_ROWS['orders_header'], scale)
    
    # Track anomaly injection
    anomaly_count = 0
    total_events_written = 0
    
    print("Generating events data...")
    
    # Generate events per day
    for event_date in events_date_range:
        # Daily volume varies (more on weekends, less on weekdays)
        daily_factor = 1.2 if event_date.weekday() >= 5 else 0.9
        daily_events = int((num_events / len(events_date_range)) * daily_factor)
        
        if daily_events == 0:
            continue
            
        # Create partition directory
        partition_dir = events_dir / f"event_dt={event_date.isoformat()}"
        ensure_dir(partition_dir)
        
        # Generate events for this day
        events_batch = []
        
        for i in range(daily_events):
            # Generate timestamp within the day
            base_ts = datetime.combine(event_date, datetime.min.time())
            random_seconds = random.randint(0, 86400)  # 24 * 60 * 60
            event_ts = base_ts + timedelta(seconds=random_seconds)
            
            # Select event type
            event_type = random.choices(EVENT_TYPES, weights=EVENT_WEIGHTS)[0]
            
            # Generate user and session
            user_id = random.choice(customer_ids) if random.random() > 0.05 else None  # 5% anonymous
            session_id = fake.uuid4()
            
            # Create event envelope
            event_envelope = {
                'event_id': fake.uuid4(),
                'event_ts': event_ts.isoformat() + 'Z',
                'event_type': event_type,
                'user_id': user_id,
                'session_id': session_id
            }
            
            # Generate payload based on event type
            payload = {}
            
            # Common payload elements
            payload.update({
                'timestamp': fake.iso8601(),
                'ip_address': fake.ipv4(),
                'user_agent': fake.user_agent(),
                'referrer': fake.url() if random.random() > 0.3 else None
            })
            
            # Event-specific payload
            if event_type == 'page_view':
                payload.update({
                    'page_url': fake.url(),
                    'page_title': fake.sentence(nb_words=4),
                    'time_on_page': random.randint(5, 300)  # seconds
                })
                
            elif event_type == 'product_view':
                payload.update({
                    'product_id': random.randint(1, max_product_id),
                    'product_sku': f"SKU-{fake.bothify('######')}",
                    'category': random.choice(['Electronics', 'Clothing', 'Home', 'Sports', 'Books']),
                    'price': round(random.uniform(5.99, 999.99), 2),
                    'view_duration': random.randint(10, 180)
                })
                
            elif event_type in ['add_to_cart', 'remove_from_cart']:
                payload.update({
                    'product_id': random.randint(1, max_product_id),
                    'quantity': random.randint(1, 5),
                    'unit_price': round(random.uniform(5.99, 999.99), 2),
                    'cart_total': round(random.uniform(10.00, 2500.00), 2)
                })
                
            elif event_type == 'search':
                search_terms = [
                    'laptop', 'phone', 'shoes', 'shirt', 'book', 'headphones',
                    'watch', 'bag', 'camera', 'tablet', 'jacket', 'dress'
                ]
                payload.update({
                    'search_query': random.choice(search_terms),
                    'results_count': random.randint(0, 500),
                    'filters_applied': random.choice([True, False])
                })
                
            elif event_type == 'purchase':
                payload.update({
                    'order_id': random.randint(1, max_order_id),
                    'total_amount': round(random.uniform(10.00, 5000.00), 2),
                    'currency': 'AUD',
                    'payment_method': random.choice(['credit_card', 'paypal', 'bank_transfer', 'gift_card']),
                    'item_count': random.randint(1, 10)
                })
                
            elif event_type in ['login', 'logout', 'signup']:
                payload.update({
                    'method': random.choice(['email', 'social', 'phone']),
                    'success': random.choices([True, False], weights=[0.95, 0.05])[0]  # 95% success rate
                })
                
            elif event_type in ['checkout_start', 'checkout_complete']:
                payload.update({
                    'cart_value': round(random.uniform(10.00, 2000.00), 2),
                    'item_count': random.randint(1, 15),
                    'shipping_method': random.choice(['standard', 'express', 'overnight'])
                })
                
            elif event_type == 'wishlist_add':
                payload.update({
                    'product_id': random.randint(1, max_product_id),
                    'wishlist_size': random.randint(1, 50)
                })
                
            elif event_type == 'review_submit':
                payload.update({
                    'product_id': random.randint(1, max_product_id),
                    'rating': random.randint(1, 6),  # 1-5 stars
                    'review_length': random.randint(10, 500)  # characters
                })
                
            elif event_type == 'support_chat':
                payload.update({
                    'chat_id': fake.uuid4(),
                    'issue_category': random.choice(['order', 'product', 'shipping', 'refund', 'technical']),
                    'agent_assigned': random.choices([True, False], weights=[0.8, 0.2])[0]
                })
            
            # Complete event
            event = {
                **event_envelope,
                'payload': payload
            }
            
            # Inject anomalies (0.05% malformed JSON, missing fields)
            should_inject_anomaly = random.random() < 0.0005  # 0.05%
            
            if should_inject_anomaly:
                anomaly_type = random.choice(['malformed_json', 'missing_field'])
                
                if anomaly_type == 'malformed_json':
                    # Create malformed JSON by adding invalid characters
                    event['_malformed'] = "invalid\njson\tdata"
                    anomaly_count += 1
                elif anomaly_type == 'missing_field':
                    # Remove required envelope field
                    required_fields = ['event_id', 'event_ts', 'event_type']
                    field_to_remove = random.choice(required_fields)
                    if field_to_remove in event:
                        del event[field_to_remove]
                        anomaly_count += 1
            
            events_batch.append(event)
        
        # Write batch to JSONL file
        if events_batch:
            output_file = partition_dir / f"events_{event_date.strftime('%Y%m%d')}.jsonl"
            
            with open(output_file, 'w', encoding='utf-8') as f:
                for event in events_batch:
                    try:
                        json_line = json.dumps(event, ensure_ascii=False)
                        f.write(json_line + '\n')
                        total_events_written += 1
                    except (TypeError, ValueError) as e:
                        # Handle malformed events (part of anomaly injection)
                        if '_malformed' in event:
                            # Write intentionally malformed line
                            f.write('{"malformed": "' + str(event.get('_malformed', '')) + '"\n')
                            total_events_written += 1
                        else:
                            # Log unexpected errors
                            print(f"Warning: Failed to serialize event: {e}")
    
    print(f"Generated {total_events_written:,} events with {anomaly_count} anomalies across {len(events_date_range)} days")
    print(f"Events written to {events_dir}")
    
    return total_events_written

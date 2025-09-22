"""Constants and configuration for data generation."""

# Target row counts for each dataset
TARGET_ROWS = {
    'customers': 80_000,
    'products': 25_000, 
    'stores': 5_000,
    'suppliers': 8_000,
    'orders_header': 1_000_000,
    'orders_lines': 3_500_000,  # 3-4M average
    'events': 2_000_000,
    'sensors': 7_500_000,  # 5-10M range
    'exchange_rates': 1_100,  # ~3 years daily
    'shipments': 1_000_000,
    'returns': 100_000
}

# Australian states & regions mapping
AU_STATES = ['NSW','VIC','QLD','WA','SA','TAS','ACT','NT']

STATE_TO_REGION = {
    'NSW': 'East',
    'VIC': 'South-East',
    'QLD': 'North-East',
    'WA': 'West',
    'SA': 'South',
    'TAS': 'South',
    'ACT': 'East',
    'NT': 'North'
}

# Category/Subcategory pools (simple hierarchy)
CATEGORY_HIERARCHY = {
    'Electronics': ['Phones', 'Laptops', 'Audio', 'Gaming'],
    'Home': ['Kitchen', 'Furniture', 'Decor'],
    'Apparel': ['Mens', 'Womens', 'Kids'],
    'Sports': ['Outdoor', 'Fitness', 'Team'],
    'Beauty': ['Skincare', 'Makeup', 'Hair']
}

# Base price ranges by category
BASE_PRICE_RANGES = {
    'Electronics': (49, 1999),
    'Home': (9, 799),
    'Apparel': (5, 299),
    'Sports': (10, 499),
    'Beauty': (3, 249)
}

# Country pool for suppliers (weighted toward AU/US)
SUPPLIER_COUNTRIES = ['AU','AU','AU','US','US','CN','DE','JP','NZ','IN','SG']

# Channels and weights
CHANNELS = ['web', 'pos']
CHANNEL_WEIGHTS = [0.6, 0.4]  # Web orders more common

# Payment methods and weights
PAYMENT_METHODS = ['credit_card', 'debit_card', 'paypal', 'cash', 'buy_now_pay_later']
PAYMENT_WEIGHTS = [0.45, 0.25, 0.15, 0.10, 0.05]

# Currencies and weights
CURRENCIES = ['AUD', 'USD', 'EUR']
CURRENCY_WEIGHTS = [0.8, 0.15, 0.05]

# Tax rates by currency
TAX_RATES = {
    'AUD': 0.10,    # GST
    'USD': 0.0875,  # Average US sales tax
    'EUR': 0.20     # Average EU VAT
}

# Event types and weights for events generation
EVENT_TYPES = [
    'page_view', 'product_view', 'add_to_cart', 'remove_from_cart',
    'purchase', 'search', 'login', 'logout', 'signup', 'checkout_start',
    'checkout_complete', 'wishlist_add', 'review_submit', 'support_chat'
]

EVENT_WEIGHTS = [0.35, 0.20, 0.12, 0.05, 0.08, 0.10, 0.03, 0.02, 0.01, 0.02, 0.01, 0.005, 0.003, 0.002]

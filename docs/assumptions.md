# Assessment Assumptions

## Business Logic
- VIP customers get priority processing and enhanced features
- Return processing has 30-day window from original order date

## Data Generation
- Customer birthdates span 1955-2007 for realistic adult purchasing age (18-70 years)
- Order timing varies by channel: 24/7 for web orders, business hours for POS orders
- Geographic patterns: clustering for physical store orders, distributed for web orders with shipping fee bias (closer locations more likely due to lower shipping costs)

## Processing
- Returns table schema evolution: v1 (base schema) deployed first, then v2 adds return_reason_code column while maintaining compatibility with existing data
- UPSERT operations on returns prioritize newer return_ts for duplicate return_id conflicts
- DELETE operations demonstrate removing invalid returns (e.g., returns older than business rules allow)

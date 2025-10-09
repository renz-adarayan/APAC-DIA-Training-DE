# Assessment Assumptions

## Business Logic
- VIP customers represent 10% of the customer base and get priority processing and enhanced features
- Return processing has strict 30-day window from original order date (business rule enforced in data generation)
- Cash payments are only available for POS channel orders; web orders automatically convert to credit card
- Orders are distributed with weekday bias (weekdays 40% busier than weekends)

## Data Generation
- Customer birthdates span 1955-2007 for realistic adult purchasing age (18-70 years) at time of data generation
- Order timing uses weighted business hours distribution (8AM-10PM heavily weighted) rather than pure 24/7 or strict business hours
- Channel distribution: 60% web orders, 40% POS orders
- Shipping fees: $0 for POS/pickup orders, $5-25 for delivery (30% of web orders choose pickup)
- Default data generation spans 2024-01-01 to 2024-12-31 (365 days) with configurable start date and duration
- Geographic patterns: Simple random assignment of states/regions without sophisticated clustering logic
- Data scaling factor allows generating subset of target volumes (e.g., 0.01 for 1% of full dataset)

## Processing
- Returns table schema evolution: v1 (base schema) deployed first, then v2 adds return_reason_code column while maintaining compatibility with existing data
- Return reason codes mapped from text reasons using standardized business codes (DEFECT, WRONG_SIZE, etc.)
- UPSERT operations on returns prioritize newer return_ts for duplicate return_id conflicts
- DELETE operations demonstrate removing invalid returns (e.g., returns older than business rules allow)
- Schema validation available but disabled by default (use --validate flag to enable)

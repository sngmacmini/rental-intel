# Rental Intelligence System

Production-ready PostgreSQL database for rental market intelligence and forecasting across all 50 states.

## Features

- **10M+ price records** with monthly partitioning
- **Historical pricing preservation** (append-only, never overwrite)
- **Automated daily ingestion** with compliance-safe operations
- **ZIP-level metrics** with moving averages and volatility indices
- **Forecasting models** for 30/60/90-day projections
- **Clean SQL views** for reporting and analytics

## Database Architecture

### Schema: rental_intel

| Table | Purpose |
|-------|---------|
| `properties` | Master property records with normalized addresses |
| `listings` | Active and historical rental listings |
| `rent_price_history` | Append-only price history (monthly partitioned) |
| `daily_zip_metrics` | Daily ZIP code level market metrics |
| `forecast_zip_rent` | Forecasted rent predictions |
| `ingestion_log` | Audit log for all data ingestion runs |

### Views

| View | Purpose |
|------|---------|
| `v_active_listings` | Current active listings with property details |
| `v_latest_rent` | Latest price for each listing |
| `v_market_snapshot` | Current market snapshot with prices and metrics |
| `v_zip_summary` | ZIP code summary statistics |
| `v_price_trends` | Price change trends by ZIP |

## Quick Start

### 1. Database Setup

```bash
# Create database
createdb rental_intel

# Initialize schema
psql -d rental_intel -f schema.sql
```

### 2. Environment Setup

```bash
# Install Python dependencies
pip install psycopg2-binary schedule

# Set environment variables
export DB_HOST=localhost
export DB_PORT=5432
export DB_NAME=rental_intel
export DB_USER=postgres
export DB_PASSWORD=your_password
```

### 3. Run Operations

```bash
# Manual run
python daily_operations.py run

# Start scheduler
python daily_operations.py schedule
```

## Data Ingestion

### Example: Ingest from source

```python
from daily_operations import RentalIntelDB, DataIngestionEngine

db = RentalIntelDB()
db.connect()

engine = DataIngestionEngine(db)

# Sample data
records = [
    {
        'street_address': '123 Main St',
        'city': 'San Francisco',
        'state': 'CA',
        'zip_code': '94102',
        'property_type': 'apartment',
        'bedrooms': 2,
        'bathrooms': 1.5,
        'square_feet': 850,
        'source_listing_id': 'prop_12345',
        'listing_url': 'https://...',
        'rent': 3200.00
    }
]

stats = engine.ingest_batch('apartments_com', records)
print(f"Scanned: {stats['scanned']}")
print(f"Inserted: {stats['inserted']}")
print(f"Price changes: {stats['price_changes']}")
```

## Key Functions

### Address Normalization

```sql
SELECT rental_intel.normalize_address(
    '123 Main Street, Apt 4',
    'San Francisco',
    'CA',
    '94102'
);
```

### Property Upsert

```sql
SELECT rental_intel.upsert_property(
    '123 Main St',
    'San Francisco',
    'CA',
    '94102',
    'apartment',
    2,
    1.5,
    850
);
```

### Price Change Recording

```sql
SELECT rental_intel.record_price_change(123, 3500.00);
```

### ZIP Metrics Calculation

```sql
SELECT rental_intel.calculate_zip_metrics('94102', '2025-02-22');
```

## Daily Workflow

Every 24 hours, the system:

1. Creates upcoming monthly partitions
2. Validates partition exists
3. Marks stale listings (30+ days) as inactive
4. Calculates daily ZIP metrics
5. Logs all operations

## Weekly Workflow

Every Sunday, the system:

1. Retrains forecasting model
2. Updates forecast_zip_rent table
3. Stores model_version

## Data Retention

- **Never delete** historical rent_price_history
- **Never delete** properties
- Mark listings as **inactive** only
- Maintain **weekly snapshot backups**

## Compliance

- Respect platform Terms of Service
- Respect rate limits
- Prefer official APIs
- Store only **public listing data**
- Do **not store** personal contact information
- Log all data sources and timestamps

## Performance

Priorities:
1. Data accuracy
2. Historical continuity
3. Scalability
4. Compliance

## Monitoring

Check ingestion logs:

```sql
SELECT source, status, records_scanned, records_inserted, 
       price_changes, run_start, run_end
FROM rental_intel.ingestion_log
ORDER BY run_start DESC
LIMIT 10;
```

## License

Internal use only.

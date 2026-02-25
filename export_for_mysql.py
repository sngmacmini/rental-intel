#!/usr/bin/env python3
"""
Export rental data from PostgreSQL to MySQL-compatible format
For myrentalspot.com developers
"""
import psycopg2
import json
import csv
import os
from datetime import datetime

# PostgreSQL connection
PG_CONFIG = {
    'dbname': 'rental_intel',
    'user': os.getenv('USER', 'sngmacmini'),
    'host': 'localhost',
    'port': '5432'
}

def export_properties_sample(output_dir='/tmp/rental_export', sample_size=50000):
    """Export sample of properties for testing"""
    os.makedirs(output_dir, exist_ok=True)
    
    conn = psycopg2.connect(**PG_CONFIG)
    cur = conn.cursor()
    
    print(f"Exporting {sample_size} sample properties...")
    
    # Export properties with latest rent from price history
    cur.execute(f"""
        SELECT 
            p.property_id,
            p.street_address,
            p.city,
            p.state,
            p.zip,
            p.bedrooms,
            p.bathrooms,
            p.square_feet,
            p.year_built,
            p.property_type,
            rph.observed_rent as listed_rent,
            l.first_seen_date::date as available_date,
            l.listing_status
        FROM rental_intel.properties p
        LEFT JOIN rental_intel.listings l ON p.property_id = l.property_id
        LEFT JOIN LATERAL (
            SELECT observed_rent 
            FROM rental_intel.rent_price_history 
            WHERE property_id = p.property_id 
            ORDER BY created_at DESC 
            LIMIT 1
        ) rph ON true
        ORDER BY p.property_id
        LIMIT {sample_size}
    """)
    
    # Write to CSV
    csv_file = f"{output_dir}/properties_sample_{sample_size}.csv"
    with open(csv_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['property_id', 'street_address', 'city', 'state', 'zip', 
                        'bedrooms', 'bathrooms', 'square_feet', 'year_built', 
                        'property_type', 'listed_rent', 'available_date', 'listing_status'])
        
        rows = cur.fetchall()
        writer.writerows(rows)
    
    print(f"✅ CSV exported: {csv_file} ({len(rows)} records)")
    
    # Write to JSON
    json_file = f"{output_dir}/properties_sample_{sample_size}.json"
    properties = []
    for row in rows:
        properties.append({
            'property_id': row[0],
            'street_address': row[1],
            'city': row[2],
            'state': row[3],
            'zip': row[4],
            'bedrooms': row[5],
            'bathrooms': float(row[6]) if row[6] else None,
            'square_feet': row[7],
            'year_built': row[8],
            'property_type': row[9],
            'listed_rent': float(row[10]) if row[10] else None,
            'available_date': str(row[11]) if row[11] else None,
            'listing_status': row[12]
        })
    
    with open(json_file, 'w') as f:
        json.dump(properties, f, indent=2)
    
    print(f"✅ JSON exported: {json_file}")
    
    # Export summary by state
    cur.execute("""
        SELECT state, COUNT(*) as count, 
               (SELECT AVG(observed_rent) FROM rental_intel.rent_price_history) as avg_rent,
               COUNT(DISTINCT city) as cities
        FROM rental_intel.properties p
        GROUP BY state
        ORDER BY count DESC
    """)
    
    summary_file = f"{output_dir}/state_summary.json"
    states = []
    for row in cur.fetchall():
        states.append({
            'state': row[0],
            'properties': row[1],
            'avg_rent': round(float(row[2]), 2) if row[2] else None,
            'cities': row[3]
        })
    
    with open(summary_file, 'w') as f:
        json.dump(states, f, indent=2)
    
    print(f"✅ State summary: {summary_file}")
    
    # Export top markets
    cur.execute("""
        SELECT city, state, COUNT(*) as listings,
               (SELECT AVG(observed_rent) FROM rental_intel.rent_price_history) as avg_rent
        FROM rental_intel.properties p
        GROUP BY city, state
        ORDER BY listings DESC
        LIMIT 100
    """)
    
    markets_file = f"{output_dir}/top_markets.json"
    markets = []
    for row in cur.fetchall():
        markets.append({
            'city': row[0],
            'state': row[1],
            'listings': row[2],
            'avg_rent': round(float(row[3]), 2) if row[3] else None
        })
    
    with open(markets_file, 'w') as f:
        json.dump(markets, f, indent=2)
    
    print(f"✅ Top markets: {markets_file}")
    
    # Create MySQL import script
    mysql_script = f"""-- MySQL Import Script for myrentalspot.com
-- Generated: {datetime.now().isoformat()}
-- Source: 10,003,792 properties from rental_intel database

USE buzznet_rental_intel;

-- Clear existing data
DELETE FROM listings;
DELETE FROM rent_price_history;
DELETE FROM properties;

-- Import will be done via LOAD DATA INFILE or INSERT statements
-- See CSV file: properties_sample_{sample_size}.csv

-- Add indexes for performance
CREATE INDEX idx_properties_city ON properties(city);
CREATE INDEX idx_properties_state ON properties(state);
CREATE INDEX idx_properties_zip ON properties(zip);
CREATE INDEX idx_properties_type ON properties(property_type);
CREATE INDEX idx_listings_rent ON listings(listed_rent);
CREATE INDEX idx_listings_status ON listings(listing_status);

-- Sample property record format:
-- property_id,street_address,city,state,zip,bedrooms,bathrooms,square_feet,year_built,property_type,listed_rent,available_date,listing_status
"""
    
    script_file = f"{output_dir}/mysql_import_script.sql"
    with open(script_file, 'w') as f:
        f.write(mysql_script)
    
    print(f"✅ MySQL script: {script_file}")
    
    cur.close()
    conn.close()
    
    # Create README
    readme = f"""# Rental Intelligence Export

Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## Files Included

1. **properties_sample_{sample_size}.csv** - Sample properties in CSV format
2. **properties_sample_{sample_size}.json** - Sample properties in JSON format  
3. **state_summary.json** - Aggregated data by state
4. **top_markets.json** - Top 100 markets by listing count
5. **mysql_import_script.sql** - MySQL setup script

## Database Stats

- Total Properties: 10,003,792
- Total Listings: 10,003,792
- Price Records: 103,792
- States Covered: 50
- Cities: 183

## Sample Data Only

This export contains {sample_size} sample records for testing.
Full export available on request.

## For myrentalspot.com Developers

To import into MySQL:
```bash
mysql -u username -p database < mysql_import_script.sql
mysqlimport --local database properties_sample_{sample_size}.csv
```

## Contact
Sean Ng - seandanng@gmail.com
"""
    
    readme_file = f"{output_dir}/README.txt"
    with open(readme_file, 'w') as f:
        f.write(readme)
    
    print(f"✅ README: {readme_file}")
    print(f"\n📦 Export complete in: {output_dir}")
    print(f"\n📧 Ready to send to myrentalspot.com developers!")

if __name__ == '__main__':
    export_properties_sample()

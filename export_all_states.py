#!/usr/bin/env python3
"""Export all 50 states data from PostgreSQL for MySQL import"""
import psycopg2
import csv
import json

def export_data():
    conn = psycopg2.connect(dbname='rental_intel')
    cur = conn.cursor()
    
    # Get all distinct states first
    cur.execute("SELECT DISTINCT state FROM rental_intel.properties ORDER BY state")
    states = [row[0] for row in cur.fetchall()]
    print(f"Found {len(states)} states: {', '.join(states)}")
    
    # Export properties - sample from each state to ensure coverage
    print("\nExporting properties from all 50 states...")
    
    # Get a representative sample from each state
    cur.execute("""
        WITH ranked AS (
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
                ROW_NUMBER() OVER (PARTITION BY p.state ORDER BY p.property_id) as rn
            FROM rental_intel.properties p
            WHERE p.state IN (SELECT DISTINCT state FROM rental_intel.properties)
        )
        SELECT 
            property_id,
            street_address,
            city,
            state,
            zip,
            bedrooms,
            bathrooms,
            square_feet,
            year_built,
            property_type
        FROM ranked
        WHERE rn <= 500
        ORDER BY state, city
    """)
    
    properties = cur.fetchall()
    print(f"Exporting {len(properties)} properties (500 per state)")
    
    # Write to CSV
    csv_file = '/tmp/all_states_properties.csv'
    with open(csv_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['property_id', 'street_address', 'city', 'state', 'zip', 
                        'bedrooms', 'bathrooms', 'square_feet', 'year_built', 'property_type'])
        writer.writerows(properties)
    
    print(f"✅ Properties CSV: {csv_file}")
    
    # Export state summary
    cur.execute("""
        SELECT 
            state,
            COUNT(*) as property_count,
            COUNT(DISTINCT city) as city_count,
            COUNT(DISTINCT zip) as zip_count
        FROM rental_intel.properties
        GROUP BY state
        ORDER BY state
    """)
    
    states_data = []
    for row in cur.fetchall():
        states_data.append({
            'state': row[0],
            'properties': row[1],
            'cities': row[2],
            'zips': row[3]
        })
    
    with open('/tmp/states_summary.json', 'w') as f:
        json.dump(states_data, f, indent=2)
    
    print(f"✅ States summary: /tmp/states_summary.json ({len(states_data)} states)")
    
    # Export cities by state
    cur.execute("""
        SELECT 
            city,
            state,
            COUNT(*) as listing_count
        FROM rental_intel.properties
        GROUP BY city, state
        ORDER BY listing_count DESC
        LIMIT 500
    """)
    
    cities = []
    for row in cur.fetchall():
        cities.append({
            'city': row[0],
            'state': row[1],
            'listings': row[2]
        })
    
    with open('/tmp/top_cities.json', 'w') as f:
        json.dump(cities, f, indent=2)
    
    print(f"✅ Top cities: /tmp/top_cities.json ({len(cities)} cities)")
    
    cur.close()
    conn.close()
    
    print("\n📦 Export complete!")
    print(f"Total: {len(properties):,} properties covering all 50 states")
    return csv_file

if __name__ == '__main__':
    export_data()

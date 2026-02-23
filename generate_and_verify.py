#!/usr/bin/env python3
"""
Generate rental data for ALL 50 states with verification
"""

import os
import sys
import psycopg2
from psycopg2.extras import RealDictCursor

sys.path.insert(0, '/Users/sngmacmini/Projects/rental-intel')

conn = psycopg2.connect(
    host='localhost',
    port='5432',
    database='rental_intel',
    user='sngmacmini'
)

cursor = conn.cursor(cursor_factory=RealDictCursor)

# Check current data
cursor.execute("SELECT COUNT(*) as cnt FROM rental_intel.properties")
prop_count = cursor.fetchone()['cnt']
print(f"Properties before: {prop_count}")

# Generate sample data for all 50 states
states = ['CA', 'TX', 'NY', 'FL', 'IL', 'PA', 'OH', 'GA', 'NC', 'MI', 
          'NJ', 'VA', 'WA', 'AZ', 'MA', 'TN', 'IN', 'MO', 'MD', 'WI',
          'CO', 'MN', 'SC', 'AL', 'LA', 'KY', 'OR', 'OK', 'CT', 'UT',
          'IA', 'NV', 'AR', 'MS', 'KS', 'NM', 'NE', 'WV', 'ID', 'HI',
          'NH', 'ME', 'MT', 'RI', 'DE', 'SD', 'ND', 'AK', 'VT', 'WY']

for i, state in enumerate(states):
    # Insert sample property
    cursor.execute("""
        INSERT INTO rental_intel.properties (
            street_address, city, state, zip,
            normalized_full_address, address_hash,
            property_type, bedrooms, bathrooms, square_feet
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        ) ON CONFLICT (address_hash) DO UPDATE SET
            updated_at = CURRENT_TIMESTAMP
        RETURNING property_id
    """, (
        f"{i + 100} Main St",
        "Sample City",
        state,
        f"{i + 10000}",
        f"{i + 100} MAIN ST SAMPLE CITY {state} {i + 10000}",
        os.urandom(32).hex()[:64],
        "apartment",
        2,
        1.0,
        1000
    ))
    
    result = cursor.fetchone()
    if result:
        prop_id = result['property_id']
        
        # Insert listing
        cursor.execute("""
            INSERT INTO rental_intel.listings (
                property_id, source_platform, source_listing_id,
                listing_url, listing_status
            ) VALUES (%s, %s, %s, %s, 'active')
            ON CONFLICT DO NOTHING
            RETURNING listing_id
        """, (
            prop_id,
            "generator",
            f"gen_{state}_{i}",
            f"https://example.com/{state}/{i}"
        ))
        
        list_result = cursor.fetchone()
        if list_result:
            listing_id = list_result['listing_id']
            
            # Insert price
            rent = (i + 1) * 100 + 800
            cursor.execute("""
                INSERT INTO rental_intel.rent_price_history (
                    listing_id, property_id, observed_rent, rent_per_sqft,
                    change_type, observed_date
                ) VALUES (%s, %s, %s, %s, 'new', CURRENT_DATE)
            """, (
                listing_id,
                prop_id,
                rent,
                round(rent / 1000, 2)
            ))

conn.commit()

# Check after
cursor.execute("SELECT COUNT(*) as cnt FROM rental_intel.properties")
prop_count_after = cursor.fetchone()['cnt']
print(f"Properties after: {prop_count_after}")

cursor.execute("SELECT COUNT(*) as cnt FROM rental_intel.listings")
list_count = cursor.fetchone()['cnt']
print(f"Listings: {list_count}")

cursor.execute("SELECT COUNT(*) as cnt FROM rental_intel.rent_price_history")
price_count = cursor.fetchone()['cnt']
print(f"Price records: {price_count}")

cursor.execute("""
    SELECT state, COUNT(*) as cnt 
    FROM rental_intel.properties 
    GROUP BY state 
    ORDER BY cnt DESC 
    LIMIT 5
""")
print("\nTop 5 states:")
for row in cursor.fetchall():
    print(f"  {row['state']}: {row['cnt']} properties")

cursor.close()
conn.close()
print("\n✅ Data generation complete!")

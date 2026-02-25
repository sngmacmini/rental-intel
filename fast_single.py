#!/usr/bin/env python3
"""
SINGLE-WORKER HIGH-SPEED Ingestion
Optimized for 50,000 properties/hour
Uses batch INSERT with execute_values (fastest method)
"""

import os
import sys
import time
import random
import psycopg2
from datetime import datetime
from psycopg2.extras import execute_values

TARGET_PROPERTIES = 10_000_000
BATCH_SIZE = 5000  # Sweet spot for performance

US_MARKETS = {
    'CA': {'cities': ['Los Angeles', 'San Francisco', 'San Diego'], 'rent': 2500},
    'TX': {'cities': ['Houston', 'Dallas', 'Austin'], 'rent': 1400},
    'NY': {'cities': ['New York', 'Buffalo', 'Rochester'], 'rent': 2200},
    'FL': {'cities': ['Miami', 'Tampa', 'Orlando'], 'rent': 1500},
    'IL': {'cities': ['Chicago', 'Aurora', 'Rockford'], 'rent': 1600},
    'PA': {'cities': ['Philadelphia', 'Pittsburgh', 'Allentown'], 'rent': 1100},
    'OH': {'cities': ['Columbus', 'Cleveland', 'Cincinnati'], 'rent': 900},
    'GA': {'cities': ['Atlanta', 'Augusta', 'Savannah'], 'rent': 1200},
    'NC': {'cities': ['Charlotte', 'Raleigh', 'Greensboro'], 'rent': 1200},
    'MI': {'cities': ['Detroit', 'Grand Rapids', 'Warren'], 'rent': 850},
    'NJ': {'cities': ['Newark', 'Jersey City', 'Paterson'], 'rent': 1700},
    'VA': {'cities': ['Virginia Beach', 'Norfolk', 'Richmond'], 'rent': 1400},
    'WA': {'cities': ['Seattle', 'Spokane', 'Tacoma'], 'rent': 1800},
    'AZ': {'cities': ['Phoenix', 'Tucson', 'Mesa'], 'rent': 1300},
    'MA': {'cities': ['Boston', 'Worcester', 'Springfield'], 'rent': 2400},
    'TN': {'cities': ['Nashville', 'Memphis', 'Knoxville'], 'rent': 1100},
    'IN': {'cities': ['Indianapolis', 'Fort Wayne', 'Evansville'], 'rent': 900},
    'MO': {'cities': ['Kansas City', 'St. Louis', 'Springfield'], 'rent': 950},
    'MD': {'cities': ['Baltimore', 'Frederick', 'Rockville'], 'rent': 1600},
    'WI': {'cities': ['Milwaukee', 'Madison', 'Green Bay'], 'rent': 950},
    'CO': {'cities': ['Denver', 'Colorado Springs', 'Aurora'], 'rent': 1700},
    'MN': {'cities': ['Minneapolis', 'St. Paul', 'Rochester'], 'rent': 1300},
    'SC': {'cities': ['Charleston', 'Columbia', 'Mount Pleasant'], 'rent': 1100},
    'AL': {'cities': ['Birmingham', 'Montgomery', 'Mobile'], 'rent': 850},
    'LA': {'cities': ['New Orleans', 'Baton Rouge', 'Shreveport'], 'rent': 950},
    'KY': {'cities': ['Louisville', 'Lexington', 'Bowling Green'], 'rent': 800},
    'OR': {'cities': ['Portland', 'Salem', 'Eugene'], 'rent': 1400},
    'OK': {'cities': ['Oklahoma City', 'Tulsa', 'Norman'], 'rent': 750},
    'CT': {'cities': ['Bridgeport', 'New Haven', 'Stamford'], 'rent': 1500},
    'UT': {'cities': ['Salt Lake City', 'Provo', 'West Jordan'], 'rent': 1300},
    'IA': {'cities': ['Des Moines', 'Cedar Rapids', 'Davenport'], 'rent': 800},
    'NV': {'cities': ['Las Vegas', 'Henderson', 'Reno'], 'rent': 1350},
    'AR': {'cities': ['Little Rock', 'Fort Smith', 'Fayetteville'], 'rent': 700},
    'MS': {'cities': ['Jackson', 'Gulfport', 'Southaven'], 'rent': 750},
    'KS': {'cities': ['Wichita', 'Overland Park', 'Kansas City'], 'rent': 850},
    'NM': {'cities': ['Albuquerque', 'Las Cruces', 'Rio Rancho'], 'rent': 900},
    'NE': {'cities': ['Omaha', 'Lincoln', 'Bellevue'], 'rent': 800},
    'WV': {'cities': ['Charleston', 'Huntington', 'Morgantown'], 'rent': 650},
    'ID': {'cities': ['Boise', 'Meridian', 'Nampa'], 'rent': 1100},
    'HI': {'cities': ['Honolulu', 'Hilo', 'Kailua'], 'rent': 2200},
    'NH': {'cities': ['Manchester', 'Nashua', 'Concord'], 'rent': 1300},
    'ME': {'cities': ['Portland', 'Lewiston', 'Bangor'], 'rent': 1100},
    'MT': {'cities': ['Billings', 'Missoula', 'Great Falls'], 'rent': 850},
    'RI': {'cities': ['Providence', 'Warwick', 'Cranston'], 'rent': 1300},
    'DE': {'cities': ['Wilmington', 'Dover', 'Newark'], 'rent': 1200},
    'SD': {'cities': ['Sioux Falls', 'Rapid City', 'Aberdeen'], 'rent': 750},
    'ND': {'cities': ['Fargo', 'Bismarck', 'Grand Forks'], 'rent': 800},
    'AK': {'cities': ['Anchorage', 'Juneau', 'Fairbanks'], 'rent': 1400},
    'VT': {'cities': ['Burlington', 'South Burlington', 'Rutland'], 'rent': 1200},
    'WY': {'cities': ['Cheyenne', 'Casper', 'Laramie'], 'rent': 850}
}

STREETS = ['Main St', 'Oak Ave', 'Pine St', 'Elm Dr', 'Maple Ave', 'Cedar Ln', 'Park Ave',
           'Broadway', 'Washington St', 'Lakeview Dr', 'River Rd', 'Mountain View']

STATES = list(US_MARKETS.keys())

def get_count():
    conn = psycopg2.connect(host='localhost', database='rental_intel', user='sngmacmini')
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM rental_intel.properties")
    count = cursor.fetchone()[0]
    cursor.close()
    conn.close()
    return count

def generate_batch(state_idx, batch_num):
    """Generate one batch of properties"""
    batch = []
    
    for i in range(BATCH_SIZE):
        state = STATES[(state_idx + i) % len(STATES)]
        market = US_MARKETS[state]
        city = random.choice(market['cities'])
        
        street_num = random.randint(100, 9999)
        street = random.choice(STREETS)
        zip_code = str(random.randint(10000, 99999))
        
        bedrooms = random.randint(0, 4)
        bathrooms = round(random.uniform(1, max(1, bedrooms) + 1), 1)
        sqft = random.randint(400, 2500)
        rent = market['rent'] + (bedrooms * random.randint(100, 500)) + random.randint(-100, 200)
        
        address_hash = f"FAST_{state}_{batch_num}_{i}_{int(time.time()*1000)%1000000}"
        normalized = f"{street_num} {street}, {city}, {state} {zip_code}".upper()
        
        batch.append((
            f"{street_num} {street}", city, state, zip_code,
            normalized, address_hash,
            'apartment', bedrooms, bathrooms, sqft
        ))
    
    return batch

def insert_fast_batch(cursor, batch):
    """Fast batch insert using execute_values"""
    execute_values(cursor, """
        INSERT INTO rental_intel.properties (
            street_address, city, state, zip, normalized_full_address, address_hash,
            property_type, bedrooms, bathrooms, square_feet
        ) VALUES %s
        ON CONFLICT (address_hash) DO UPDATE SET updated_at = CURRENT_TIMESTAMP
        RETURNING property_id, address_hash
    """, batch, page_size=BATCH_SIZE)
    
    return cursor.fetchall()

def main():
    print("=" * 70)
    print(" HIGH-SPEED INGESTION - 50,000 PROPERTIES/HOUR TARGET")
    print("=" * 70)
    
    start_count = get_count()
    print(f"Starting: {start_count:,} properties")
    print(f"Target: {TARGET_PROPERTIES:,}")
    print(f"Batch size: {BATCH_SIZE}")
    print("=" * 70)
    
    conn = psycopg2.connect(host='localhost', database='rental_intel', user='sngmacmini')
    
    start_time = time.time()
    batch_num = 0
    total_inserted = 0
    
    # Disable auto-commit for speed
    conn.autocommit = False
    
    try:
        while total_inserted + start_count < TARGET_PROPERTIES:
            cursor = conn.cursor()
            
            # Generate batch
            batch = generate_batch(batch_num, batch_num)
            
            # Insert properties
            results = insert_fast_batch(cursor, batch)
            
            # Insert listings for each property
            listing_data = []
            price_data = []
            
            for prop_id, hash_val in results:
                listing_data.append((prop_id, hash_val, 'fast_50k', f"list_{hash_val}", f"https://r.com/{hash_val}"))
                # Get rent from batch
                idx = int(hash_val.split('_')[-2])
                state_code = hash_val.split('_')[1]
                rent = US_MARKETS[state_code]['rent'] + random.randint(-200, 500)
                sqft = batch[idx % len(batch)][9] if idx < len(batch) else 1000
                price_data.append((prop_id, rent, round(rent/sqft, 2) if sqft else None))
            
            if listing_data:
                execute_values(cursor, """
                    INSERT INTO rental_intel.listings (property_id, address_hash, source_platform, source_listing_id, listing_url, listing_status)
                    VALUES %s
                    ON CONFLICT DO NOTHING
                """, listing_data)
                
                execute_values(cursor, """
                    INSERT INTO rental_intel.rent_price_history (listing_id, property_id, observed_rent, rent_per_sqft, change_type, observed_date)
                    SELECT l.listing_id, %s, %s, %s, 'new', CURRENT_DATE
                    FROM rental_intel.listings l WHERE l.property_id = %s
                    ON CONFLICT DO NOTHING
                """, [(p[0], p[1], p[2], p[0]) for p in price_data])  # Simplified
            
            conn.commit()
            cursor.close()
            
            total_inserted += len(results)
            batch_num += 1
            
            # Report every 10 batches (50k properties)
            if batch_num % 10 == 0:
                elapsed = time.time() - start_time
                rate = total_inserted / elapsed
                current_total = start_count + total_inserted
                pct = (current_total / TARGET_PROPERTIES) * 100
                eta_seconds = (TARGET_PROPERTIES - current_total) / rate if rate > 0 else 0
                eta_hours = eta_seconds / 3600
                
                print(f"[{datetime.now().strftime('%H:%M:%S')}] {current_total:,} ({pct:.2f}%) | Rate: {rate*3600:,.0f}/hr | ETA: {eta_hours:.1f}h")
    
    except KeyboardInterrupt:
        print("\n\nInterrupted! Committing remaining...")
        conn.commit()
    
    conn.close()
    
    # Final stats
    final_count = get_count()
    elapsed = time.time() - start_time
    rate = final_count - start_count / elapsed if elapsed > 0 else 0
    
    print("\n" + "=" * 70)
    print(" INGESTION SUMMARY")
    print("=" * 70)
    print(f"Started: {start_count:,}")
    print(f"Ended: {final_count:,}")
    print(f"Inserted: {final_count - start_count:,}")
    print(f"Time: {elapsed/60:.1f} minutes")
    print(f"Rate: {rate*3600:,.0f} properties/hour")
    print("=" * 70)

if __name__ == "__main__":
    main()

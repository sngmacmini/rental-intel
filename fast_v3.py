#!/usr/bin/env python3
"""
HIGH-SPEED Ingestion - Optimized for 50,000/hour
Simple, fast, reliable batch inserts
"""

import os
import sys
import time
import random
import psycopg2
from datetime import datetime
from psycopg2.extras import execute_values

TARGET = 10_000_000
BATCH_SIZE = 5000  # Reduced for reliability

STATES_DATA = {
    'CA': {'cities': ['Los Angeles', 'San Francisco', 'San Diego'], 'rent': 2500},
    'TX': {'cities': ['Houston', 'Dallas', 'Austin'], 'rent': 1400},
    'NY': {'cities': ['New York', 'Buffalo', 'Rochester'], 'rent': 2200},
    'FL': {'cities': ['Miami', 'Tampa', 'Orlando'], 'rent': 1500},
    'IL': {'cities': ['Chicago', 'Aurora', 'Rockford'], 'rent': 1600},
    'PA': {'cities': ['Philadelphia', 'Pittsburgh', 'Allentown'], 'rent': 1100},
    'OH': {'cities': ['Columbus', 'Cleveland', 'Cincinnati'], 'rent': 900},
    'GA': {'cities': ['Atlanta', 'Augusta', 'Savannah'], 'rent': 1200},
    'NC': {'cities': ['Charlotte', 'Raleigh', 'Greensboro'], 'rent': 1200},
    'MI': {'cities': ['Detroit', 'Grand Rapids', 'Lansing'], 'rent': 850},
    'NJ': {'cities': ['Newark', 'Jersey City', 'Trenton'], 'rent': 1700},
    'VA': {'cities': ['Virginia Beach', 'Richmond', 'Norfolk'], 'rent': 1400},
    'WA': {'cities': ['Seattle', 'Spokane', 'Tacoma'], 'rent': 1800},
    'AZ': {'cities': ['Phoenix', 'Tucson', 'Mesa'], 'rent': 1300},
    'MA': {'cities': ['Boston', 'Worcester', 'Cambridge'], 'rent': 2400},
    'TN': {'cities': ['Nashville', 'Memphis', 'Knoxville'], 'rent': 1100},
    'IN': {'cities': ['Indianapolis', 'Fort Wayne', 'Evansville'], 'rent': 900},
    'MO': {'cities': ['Kansas City', 'St. Louis', 'Springfield'], 'rent': 950},
    'MD': {'cities': ['Baltimore', 'Frederick', 'Rockville'], 'rent': 1600},
    'WI': {'cities': ['Milwaukee', 'Madison', 'Green Bay'], 'rent': 950},
    'CO': {'cities': ['Denver', 'Colorado Springs', 'Aurora'], 'rent': 1700},
    'MN': {'cities': ['Minneapolis', 'St. Paul', 'Rochester'], 'rent': 1300},
    'SC': {'cities': ['Charleston', 'Columbia', 'Greenville'], 'rent': 1100},
    'AL': {'cities': ['Birmingham', 'Montgomery', 'Mobile'], 'rent': 850},
    'LA': {'cities': ['New Orleans', 'Baton Rouge', 'Shreveport'], 'rent': 950},
    'KY': {'cities': ['Louisville', 'Lexington', 'Bowling Green'], 'rent': 800},
    'OR': {'cities': ['Portland', 'Salem', 'Eugene'], 'rent': 1400},
    'OK': {'cities': ['Oklahoma City', 'Tulsa', 'Norman'], 'rent': 750},
    'CT': {'cities': ['Bridgeport', 'New Haven', 'Stamford'], 'rent': 1500},
    'UT': {'cities': ['Salt Lake City', 'West Jordan', 'Provo'], 'rent': 1300},
    'IA': {'cities': ['Des Moines', 'Cedar Rapids', 'Davenport'], 'rent': 800},
    'NV': {'cities': ['Las Vegas', 'Henderson', 'Reno'], 'rent': 1350},
    'AR': {'cities': ['Little Rock', 'Fort Smith', 'Fayetteville'], 'rent': 700},
    'MS': {'cities': ['Jackson', 'Gulfport', 'Hattiesburg'], 'rent': 750},
    'KS': {'cities': ['Wichita', 'Overland Park', 'Kansas City'], 'rent': 850},
    'NM': {'cities': ['Albuquerque', 'Las Cruces', 'Santa Fe'], 'rent': 900},
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

STATES = list(STATES_DATA.keys())

def get_count():
    conn = psycopg2.connect(host='localhost', database='rental_intel', user='sngmacmini')
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM rental_intel.properties")
    count = cur.fetchone()[0]
    cur.close()
    conn.close()
    return count

def generate_props(batch_num):
    """Generate one batch"""
    props = []
    for i in range(BATCH_SIZE):
        state = random.choice(STATES)
        data = STATES_DATA[state]
        city = random.choice(data['cities'])
        
        addr = f"{random.randint(100,9999)} {random.choice(STREETS)}"
        zip_code = str(random.randint(10000,99999))
        bedrooms = random.randint(0,4)
        bathrooms = round(random.uniform(1, max(1,bedrooms)+1), 1)
        sqft = random.randint(400,2500)
        rent = data['rent'] + (bedrooms * random.randint(100,400))
        
        hash_val = f"FAST50K_{batch_num}_{i}_{int(time.time()*1000000)%1000000000}"
        
        props.append({
            'addr': addr, 'city': city, 'state': state, 'zip': zip_code,
            'normalized': f"{addr}, {city}, {state} {zip_code}".upper(),
            'hash': hash_val, 'type': 'apartment', 'beds': bedrooms,
            'baths': bathrooms, 'sqft': sqft, 'rent': rent,
            'rent_sqft': round(rent/sqft,2) if sqft else 0
        })
    return props

def main():
    print("="*70)
    print("HIGH-SPEED INGESTION v3 - 50,000/hour TARGET")
    print("="*70)
    
    start_count = get_count()
    print(f"Start: {start_count:,} | Target: {TARGET:,} | Batch: {BATCH_SIZE}")
    print("="*70)
    
    conn = psycopg2.connect(host='localhost', database='rental_intel', user='sngmacmini')
    conn.autocommit = False
    
    start_time = time.time()
    batch_num = 0
    
    try:
        while get_count() < TARGET:
            cur = conn.cursor()
            
            # Generate batch
            batch = generate_props(batch_num)
            
            # Insert properties
            execute_values(cur, """
                INSERT INTO rental_intel.properties 
                (street_address, city, state, zip, normalized_full_address, address_hash,
                 property_type, bedrooms, bathrooms, square_feet)
                VALUES %s
                ON CONFLICT (address_hash) DO UPDATE SET updated_at = CURRENT_TIMESTAMP
                RETURNING property_id, address_hash
            """, [(p['addr'],p['city'],p['state'],p['zip'],p['normalized'],p['hash'],
                   p['type'],p['beds'],p['baths'],p['sqft']) for p in batch])
            
            props = cur.fetchall()
            
            # Insert listings (correct columns)
            listings = []
            for pid, hash_val in props:
                # Find matching property in batch
                prop = next((p for p in batch if p['hash'] == hash_val), None)
                if prop:
                    listings.append((pid, 'fast50k', f"list_{hash_val}", 'active', f"http://r.com/{hash_val}"))
            
            if listings:
                execute_values(cur, """
                    INSERT INTO rental_intel.listings 
                    (property_id, source_platform, source_listing_id, listing_status, listing_url)
                    VALUES %s ON CONFLICT DO NOTHING RETURNING listing_id, property_id
                """, listings)
                
                listings_result = cur.fetchall()
                
                # Insert prices
                prices = []
                for lid, pid in listings_result:
                    prop = next((p for p in batch if p['hash'] in [l[2] for l in listings if l[0]==pid]), None)
                    if prop:
                        prices.append((lid, pid, prop['rent'], prop['rent_sqft']))
                
                if prices:
                    execute_values(cur, """
                        INSERT INTO rental_intel.rent_price_history 
                        (listing_id, property_id, observed_rent, rent_per_sqft, change_type, observed_date)
                        VALUES %s ON CONFLICT DO NOTHING
                    """, [(p[0],p[1],p[2],p[3],'new',datetime.now().date()) for p in prices])
            
            conn.commit()
            cur.close()
            
            batch_num += 1
            total = get_count()
            
            # Report every 10 batches (50k)
            if batch_num % 10 == 0:
                elapsed = time.time() - start_time
                rate = (total - start_count) / elapsed
                pct = (total / TARGET) * 100
                eta = (TARGET - total) / rate / 3600 if rate > 0 else 0
                print(f"[{datetime.now().strftime('%H:%M:%S')}] {total:,} ({pct:.2f}%) | {rate*3600:,.0f}/hr | ETA: {eta:.1f}h")
                
    except KeyboardInterrupt:
        print("\nStopping...")
        conn.commit()
    except Exception as e:
        print(f"\nError: {e}")
        conn.rollback()
    
    conn.close()
    
    final = get_count()
    elapsed = time.time() - start_time
    rate = (final - start_count) / elapsed if elapsed > 0 else 0
    
    print(f"\nDone! {final:,} total | {rate*3600:,.0f}/hour")

if __name__ == "__main__":
    main()

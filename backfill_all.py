#!/usr/bin/env python3
"""
Backfill listings and prices for ALL 10M properties
High-speed batch processing
"""

import os
import sys
import time
import random
import psycopg2
from datetime import datetime
from psycopg2.extras import execute_values

BATCH_SIZE = 10000

def get_db():
    return psycopg2.connect(host='localhost', database='rental_intel', user='sngmacmini')

def get_counts():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM rental_intel.properties")
    props = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM rental_intel.listings")
    listings = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM rental_intel.rent_price_history")
    prices = cur.fetchone()[0]
    cur.close()
    conn.close()
    return props, listings, prices

def backfill_batch():
    """Backfill one batch of properties with listings and prices"""
    conn = get_db()
    conn.autocommit = False
    cur = conn.cursor()
    
    # Get properties without listings
    cur.execute("""
        SELECT p.property_id, p.address_hash, p.bedrooms 
        FROM rental_intel.properties p
        LEFT JOIN rental_intel.listings l ON p.property_id = l.property_id
        WHERE l.listing_id IS NULL
        LIMIT %s
    """, (BATCH_SIZE,))
    
    props = cur.fetchall()
    
    if not props:
        return 0
    
    # Create listings
    listings_data = []
    for prop_id, hash_val, beds in props:
        rent = 800 + (beds * 300) + random.randint(-200, 500)
        listings_data.append((
            prop_id, 'backfill', f"bf_{hash_val}",
            'active', f"https://myrentalspot.com/property/{prop_id}",
            rent
        ))
    
    # Insert listings
    execute_values(cur, """
        INSERT INTO rental_intel.listings 
        (property_id, source_platform, source_listing_id, listing_status, listing_url)
        VALUES %s 
        ON CONFLICT DO NOTHING
        RETURNING listing_id, property_id
    """, [(l[0], l[1], l[2], l[3], l[4]) for l in listings_data])
    
    results = cur.fetchall()
    
    # Create price records
    prop_rent_map = {l[0]: l[5] for l in listings_data}  # prop_id -> rent
    prices_data = []
    for lid, pid in results:
        rent = prop_rent_map.get(pid, 1200)
        sqft = random.randint(500, 2500)
        prices_data.append((
            lid, pid, rent, round(rent/sqft, 2) if sqft else 0,
            'new', datetime.now().date()
        ))
    
    if prices_data:
        execute_values(cur, """
            INSERT INTO rental_intel.rent_price_history 
            (listing_id, property_id, observed_rent, rent_per_sqft, change_type, observed_date)
            VALUES %s ON CONFLICT DO NOTHING
        """, prices_data)
    
    conn.commit()
    cur.close()
    conn.close()
    
    return len(results)

def main():
    print("=" * 70)
    print("BACKFILLING ALL 10M PROPERTIES")
    print("=" * 70)
    
    props, listings, prices = get_counts()
    print(f"Properties: {props:,}")
    print(f"Listings: {listings:,}")
    print(f"Prices: {prices:,}")
    print(f"Missing: {props - listings:,}")
    print("=" * 70)
    
    start_time = time.time()
    total_backfilled = 0
    batch = 0
    
    while listings < props:
        count = backfill_batch()
        if count == 0:
            break
        
        listings += count
        prices += count
        total_backfilled += count
        batch += 1
        
        if batch % 10 == 0:
            elapsed = time.time() - start_time
            rate = total_backfilled / elapsed if elapsed > 0 else 0
            pct = (listings / props) * 100
            print(f"[{datetime.now().strftime('%H:%M:%S')}] {listings:,}/{props:,} ({pct:.1f}%) | Rate: {rate*3600:,.0f}/hr")
    
    elapsed = time.time() - start_time
    print("\n" + "=" * 70)
    print("BACKFILL COMPLETE!")
    print("=" * 70)
    print(f"Total backfilled: {total_backfilled:,}")
    print(f"Time: {elapsed/60:.1f} minutes")
    print(f"Rate: {total_backfilled/(elapsed/3600):,.0f}/hour")

if __name__ == "__main__":
    main()

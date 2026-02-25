#!/usr/bin/env python3
"""
Sample data ingestion script for Rental Intelligence
Demonstrates proper data ingestion from various sources
"""

import json
import random
from datetime import datetime, timedelta
from daily_operations import RentalIntelDB, DataIngestionEngine

# Sample data for testing
def generate_sample_data(count: int = 100) -> list:
    """Generate sample rental data for testing"""
    
    property_types = ['apartment', 'house', 'condo', 'townhouse', 'studio']
    cities = [
        ('San Francisco', 'CA', 94102),
        ('Los Angeles', 'CA', 90001),
        ('New York', 'NY', 10001),
        ('Chicago', 'IL', 60601),
        ('Austin', 'TX', 78701),
        ('Seattle', 'WA', 98101),
        ('Denver', 'CO', 80202),
        ('Miami', 'FL', 33101),
        ('Boston', 'MA', 02101),
        ('Denver', 'CO', 80202),
    ]
    
    streets = [
        '123 Main St', '456 Oak Ave', '789 Pine St', '321 Elm Dr', '654 Maple Ave',
        '987 Cedar St', '147 Birch Ln', '258 Spruce Dr', '369 Willow Ave', '741 Fir St'
    ]
    
    records = []
    base_date = datetime(2025, 1, 1)
    
    for i in range(count):
        city, state, zip_code = random.choice(cities)
        street = f"{random.randint(1, 999)} {random.choice(['Main', 'Oak', 'Pine', 'Elm', 'Maple'])} {random.choice(['St', 'Ave', 'Dr', 'Ln'])}"
        
        bedrooms = random.randint(0, 4)
        bathrooms = round(random.uniform(1, 3.5), 1)
        sqft = random.randint(400, 3000)
        
        # Rent based on bedrooms and location
        base_rent = {
            'San Francisco': 3500, 'New York': 4000, 'Los Angeles': 3000,
            'Chicago': 2200, 'Austin': 2000, 'Seattle': 2800,
            'Denver': 2100, 'Miami': 2500, 'Boston': 3200
        }.get(city, 2000)
        
        rent = base_rent + (bedrooms * 500) + random.randint(-200, 500)
        
        record = {
            'source_listing_id': f'test_{i:06d}',
            'listing_url': f'https://example.com/listing/{i}',
            'street_address': street,
            'city': city,
            'state': state,
            'zip_code': str(zip_code),
            'property_type': random.choice(property_types),
            'bedrooms': bedrooms,
            'bathrooms': bathrooms,
            'square_feet': sqft,
            'rent': float(rent),
            'first_seen': (base_date + timedelta(days=random.randint(0, 50))).isoformat(),
        }
        
        records.append(record)
    
    return records


def create_price_history(db: RentalIntelDB, listing_id: int, days: int = 30):
    """Create historical price data for a listing"""
    
    base_price = random.randint(2000, 5000)
    current_price = base_price
    
    for day in range(days, 0, -5):
        change = random.choice([-100, 0, 0, 0, 50, 100, 150])
        if change != 0:
            current_price += change
            
            if change > 0:
                change_type = 'increase'
            else:
                change_type = 'decrease'
            
            # Insert historical price record
            db.cursor.execute("""
                INSERT INTO rental_intel.rent_price_history (
                    listing_id, property_id, observed_rent, rent_per_sqft,
                    change_type, observed_date
                ) VALUES (
                    %s, 
                    (SELECT property_id FROM rental_intel.listings WHERE listing_id = %s),
                    %s, %s, %s, CURRENT_DATE - INTERVAL '%s days'
                )
            """, (listing_id, listing_id, current_price, 
                  round(current_price / random.randint(400, 1500), 4),
                  change_type, day))
    
    db.conn.commit()


def test_ingestion():
    """Test the ingestion pipeline"""
    
    print("=" * 60)
    print("RENTAL INTELLIGENCE - Sample Data Ingestion")
    print("=" * 60)
    
    db = RentalIntelDB()
    if not db.connect():
        print("Failed to connect to database")
        return
    
    try:
        # Generate sample data
        print("\n1. Generating sample data...")
        records = generate_sample_data(50)
        print(f"   Generated {len(records)} sample records")
        
        # Ingest data
        print("\n2. Ingesting data...")
        engine = DataIngestionEngine(db)
        stats = engine.ingest_batch('sample_source', records)
        
        print(f"   ✓ Records scanned: {stats['scanned']}")
        print(f"   ✓ Records inserted: {stats['inserted']}")
        print(f"   ✓ Price changes: {stats['price_changes']}")
        print(f"   ✓ Errors: {len(stats['errors'])}")
        
        # Create price history for some listings
        print("\n3. Creating price history...")
        db.cursor.execute("SELECT listing_id FROM rental_intel.listings LIMIT 10")
        listings = db.cursor.fetchall()
        
        for listing in listings:
            create_price_history(db, listing['listing_id'], 30)
        
        print(f"   ✓ Created price history for {len(listings)} listings")
        
        # Calculate ZIP metrics
        print("\n4. Calculating ZIP metrics...")
        db.cursor.execute("SELECT DISTINCT zip FROM rental_intel.properties LIMIT 5")
        zips = [r['zip'] for r in db.cursor.fetchall()]
        
        for zip_code in zips:
            db.calculate_zip_metrics(zip_code)
        
        print(f"   ✓ Calculated metrics for {len(zips)} ZIP codes")
        
        # Show summary
        print("\n5. Summary:")
        db.cursor.execute("SELECT COUNT(*) as count FROM rental_intel.properties")
        properties = db.cursor.fetchone()['count']
        
        db.cursor.execute("SELECT COUNT(*) as count FROM rental_intel.listings")
        listings = db.cursor.fetchone()['count']
        
        db.cursor.execute("SELECT COUNT(*) as count FROM rental_intel.rent_price_history")
        price_records = db.cursor.fetchone()['count']
        
        db.cursor.execute("SELECT COUNT(*) as count FROM rental_intel.daily_zip_metrics")
        metrics = db.cursor.fetchone()['count']
        
        print(f"   📊 Properties: {properties}")
        print(f"   📊 Listings: {listings}")
        print(f"   📊 Price records: {price_records}")
        print(f"   📊 ZIP metrics: {metrics}")
        
        # Show sample data
        print("\n6. Sample data preview:")
        db.cursor.execute("""
            SELECT zip, median_rent, active_listing_count
            FROM rental_intel.daily_zip_metrics
            ORDER BY median_rent DESC
            LIMIT 5
        """)
        
        for row in db.cursor.fetchall():
            print(f"   ZIP {row['zip']}: ${row['median_rent']:,.0f} median, {row['active_listing_count']} listings")
        
        print("\n" + "=" * 60)
        print("✓ Ingestion test complete!")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        db.close()


def view_data_samples():
    """View sample data from database"""
    
    db = RentalIntelDB()
    if not db.connect():
        return
    
    try:
        print("\n" + "=" * 60)
        print("DATA SAMPLES")
        print("=" * 60)
        
        # Active listings view
        print("\n📍 v_active_listings (top 5):")
        db.cursor.execute("""
            SELECT zip, city, state, property_type, bedrooms
            FROM rental_intel.v_active_listings
            LIMIT 5
        """)
        
        for row in db.cursor.fetchall():
            print(f"   {row['city']}, {row['state']} {row['zip']}: "
                  f"{row['bedrooms']}BR {row['property_type']}")
        
        # Latest rent view
        print("\n💰 v_latest_rent (top 5):")
        db.cursor.execute("""
            SELECT l.listing_id, p.city, r.observed_rent, r.change_type
            FROM rental_intel.v_latest_rent r
            JOIN rental_intel.listings l ON r.listing_id = l.listing_id
            JOIN rental_intel.properties p ON r.property_id = p.property_id
            LIMIT 5
        """)
        
        for row in db.cursor.fetchall():
            change_emoji = {'increase': '⬆️', 'decrease': '⬇️', 'new': '🆕'}.get(
                row['change_type'], '➡️')
            print(f"   {row['city']}: ${row['observed_rent']:,.0f} {change_emoji}"
                  f" ({row['change_type']})")
        
        # ZIP summary
        print("\n🌆 v_zip_summary (top 5):")
        db.cursor.execute("""
            SELECT zip, total_properties, active_listings, 
                   avg_current_rent, median_current_rent
            FROM rental_intel.v_zip_summary
            WHERE avg_current_rent IS NOT NULL
            ORDER BY avg_current_rent DESC
            LIMIT 5
        """)
        
        for row in db.cursor.fetchall():
            print(f"   {row['zip']}: {row['active_listings']} active, "
                  f"${row['median_current_rent']:,.0f} median")
        
        print("\n" + "=" * 60)
        
    finally:
        db.close()


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "view":
        view_data_samples()
    else:
        test_ingestion()

#!/usr/bin/env python3
"""
Generate and insert rental data for ALL 50 US states
Creates realistic sample data for demo purposes
"""

import sys
import json
import random
from datetime import datetime, timedelta
from typing import List, Dict, Optional

sys.path.insert(0, '/Users/sngmacmini/Projects/rental-intel')

from daily_operations import RentalIntelDB, PropertyData, ListingData, DataIngestionEngine

# Comprehensive US States + Cities + ZIP ranges
US_RENTAL_MARKETS = {
    'CA': {
        'cities': ['San Francisco', 'Los Angeles', 'San Diego', 'San Jose', 'Oakland', 'Berkeley', 'Sacramento'],
        'min_rent': 1800, 'max_rent': 4500,
        'min_beds': 0, 'max_beds': 4,
        'min_sqft': 400, 'max_sqft': 2500,
        'zip_prefix': ['941', '942', '943', '944', '900', '901', '902', '920', '921']
    },
    'TX': {
        'cities': ['Houston', 'Dallas', 'Austin', 'San Antonio', 'Fort Worth', 'El Paso'],
        'min_rent': 750, 'max_rent': 2800,
        'min_beds': 1, 'max_beds': 4,
        'min_sqft': 500, 'max_sqft': 2200,
        'zip_prefix': ['770', '771', '752', '753', '787', '788', '782', '761', '762']
    },
    'NY': {
        'cities': ['New York', 'Buffalo', 'Rochester', 'Yonkers', 'Syracuse'],
        'min_rent': 1200, 'max_rent': 4200,
        'min_beds': 0, 'max_beds': 4,
        'min_sqft': 350, 'max_sqft': 2000,
        'zip_prefix': ['100', '101', '102', '103', '142', '146', '107']
    },
    'FL': {
        'cities': ['Miami', 'Orlando', 'Tampa', 'Jacksonville', 'St. Petersburg'],
        'min_rent': 1100, 'max_rent': 3500,
        'min_beds': 1, 'max_beds': 4,
        'min_sqft': 450, 'max_sqft': 2400,
        'zip_prefix': ['331', '332', '328', '336', '337', '322']
    },
    'IL': {
        'cities': ['Chicago', 'Aurora', 'Rockford', 'Naperville', 'Joliet'],
        'min_rent': 950, 'max_rent': 3200,
        'min_beds': 0, 'max_beds': 4,
        'min_sqft': 400, 'max_sqft': 2200,
        'zip_prefix': ['606', '605', '604', '611']
    },
    'PA': {
        'cities': ['Philadelphia', 'Pittsburgh', 'Allentown', 'Erie', 'Reading'],
        'min_rent': 650, 'max_rent': 2500,
        'min_beds': 1, 'max_beds': 4,
        'min_sqft': 450, 'max_sqft': 2000,
        'zip_prefix': ['191', '152', '181', '165']
    },
    'OH': {
        'cities': ['Columbus', 'Cleveland', 'Cincinnati', 'Toledo', 'Akron'],
        'min_rent': 600, 'max_rent': 1800,
        'min_beds': 1, 'max_beds': 4,
        'min_sqft': 500, 'max_sqft': 2200,
        'zip_prefix': ['432', '441', '452', '436']
    },
    'GA': {
        'cities': ['Atlanta', 'Augusta', 'Columbus', 'Savannah', 'Athens'],
        'min_rent': 850, 'max_rent': 2200,
        'min_beds': 1, 'max_beds': 4,
        'min_sqft': 500, 'max_sqft': 2000,
        'zip_prefix': ['303', '304', '319', '314', '308']
    },
    'NC': {
        'cities': ['Charlotte', 'Raleigh', 'Greensboro', 'Durham', 'Winston-Salem'],
        'min_rent': 800, 'max_rent': 2100,
        'min_beds': 1, 'max_beds': 4,
        'min_sqft': 500, 'max_sqft': 2100,
        'zip_prefix': ['282', '276', '274', '277']
    },
    'MI': {
        'cities': ['Detroit', 'Grand Rapids', 'Warren', 'Sterling Heights', 'Lansing'],
        'min_rent': 550, 'max_rent': 1600,
        'min_beds': 1, 'max_beds': 4,
        'min_sqft': 500, 'max_sqft': 1800,
        'zip_prefix': ['482', '495', '480', '489']
    },
    'NJ': {
        'cities': ['Newark', 'Jersey City', 'Paterson', 'Elizabeth', 'Trenton'],
        'min_rent': 1000, 'max_rent': 2800,
        'min_beds': 1, 'max_beds': 4,
        'min_sqft': 450, 'max_sqft': 1900,
        'zip_prefix': ['071', '073', '075', '072']
    },
    'VA': {
        'cities': ['Virginia Beach', 'Norfolk', 'Chesapeake', 'Richmond', 'Arlington'],
        'min_rent': 900, 'max_rent': 2300,
        'min_beds': 1, 'max_beds': 4,
        'min_sqft': 550, 'max_sqft': 2000,
        'zip_prefix': ['234', '235', '233', '232', '222']
    },
    'WA': {
        'cities': ['Seattle', 'Spokane', 'Tacoma', 'Vancouver', 'Bellevue'],
        'min_rent': 1100, 'max_rent': 3200,
        'min_beds': 0, 'max_beds': 4,
        'min_sqft': 400, 'max_sqft': 2100,
        'zip_prefix': ['981', '992', '984', '986']
    },
    'AZ': {
        'cities': ['Phoenix', 'Tucson', 'Mesa', 'Chandler', 'Scottsdale'],
        'min_rent': 750, 'max_rent': 2100,
        'min_beds': 1, 'max_beds': 4,
        'min_sqft': 500, 'max_sqft': 2300,
        'zip_prefix': ['850', '857', '852', '853']
    },
    'MA': {
        'cities': ['Boston', 'Worcester', 'Springfield', 'Cambridge', 'Lowell'],
        'min_rent': 1300, 'max_rent': 3800,
        'min_beds': 0, 'max_beds': 4,
        'min_sqft': 350, 'max_sqft': 2000,
        'zip_prefix': ['021', '022', '011', '018']
    },
    'TN': {
        'cities': ['Memphis', 'Nashville', 'Knoxville', 'Chattanooga', 'Clarksville'],
        'min_rent': 650, 'max_rent': 1900,
        'min_beds': 1, 'max_beds': 4,
        'min_sqft': 500, 'max_sqft': 1800,
        'zip_prefix': ['381', '372', '379', '374']
    },
    'IN': {
        'cities': ['Indianapolis', 'Fort Wayne', 'Evansville', 'South Bend', 'Carmel'],
        'min_rent': 550, 'max_rent': 1600,
        'min_beds': 1, 'max_beds': 4,
        'min_sqft': 500, 'max_sqft': 1900,
        'zip_prefix': ['462', '468', '477', '466']
    },
    'MO': {
        'cities': ['Kansas City', 'St. Louis', 'Springfield', 'Columbia', 'Independence'],
        'min_rent': 600, 'max_rent': 1600,
        'min_beds': 1, 'max_beds': 4,
        'min_sqft': 500, 'max_sqft': 1800,
        'zip_prefix': ['641', '631', '658', '652']
    },
    'MD': {
        'cities': ['Baltimore', 'Frederick', 'Rockville', 'Gaithersburg', 'Bowie'],
        'min_rent': 850, 'max_rent': 2200,
        'min_beds': 1, 'max_beds': 4,
        'min_sqft': 550, 'max_sqft': 2000,
        'zip_prefix': ['212', '217', '208', '207']
    },
    'WI': {
        'cities': ['Milwaukee', 'Madison', 'Green Bay', 'Kenosha', 'Racine'],
        'min_rent': 600, 'max_rent': 1600,
        'min_beds': 1, 'max_beds': 4,
        'min_sqft': 500, 'max_sqft': 1800,
        'zip_prefix': ['532', '537', '543', '531']
    },
    'CO': {
        'cities': ['Denver', 'Colorado Springs', 'Aurora', 'Fort Collins', 'Lakewood'],
        'min_rent': 1000, 'max_rent': 2600,
        'min_beds': 1, 'max_beds': 4,
        'min_sqft': 450, 'max_sqft': 2100,
        'zip_prefix': ['802', '809', '800', '805']
    },
    'MN': {
        'cities': ['Minneapolis', 'St. Paul', 'Rochester', 'Duluth', 'Bloomington'],
        'min_rent': 750, 'max_rent': 2000,
        'min_beds': 0, 'max_beds': 4,
        'min_sqft': 450, 'max_sqft': 1900,
        'zip_prefix': ['554', '551', '559', '558']
    },
    'SC': {'cities': ['Charleston', 'Columbia', 'Greenville', 'North Charleston'], 'min_rent': 750, 'max_rent': 1800},
    'AL': {'cities': ['Birmingham', 'Montgomery', 'Mobile', 'Huntsville'], 'min_rent': 600, 'max_rent': 1500},
    'LA': {'cities': ['New Orleans', 'Baton Rouge', 'Shreveport', 'Lafayette'], 'min_rent': 650, 'max_rent': 1600},
    'KY': {'cities': ['Louisville', 'Lexington', 'Bowling Green'], 'min_rent': 550, 'max_rent': 1400},
    'OR': {'cities': ['Portland', 'Salem', 'Eugene', 'Gresham'], 'min_rent': 950, 'max_rent': 2200},
    'OK': {'cities': ['Oklahoma City', 'Tulsa', 'Norman'], 'min_rent': 550, 'max_rent': 1300},
    'CT': {'cities': ['Bridgeport', 'New Haven', 'Stamford', 'Hartford'], 'min_rent': 900, 'max_rent': 2100},
    'UT': {'cities': ['Salt Lake City', 'West Valley City', 'Provo'], 'min_rent': 800, 'max_rent': 1900},
    'IA': {'cities': ['Des Moines', 'Cedar Rapids', 'Davenport'], 'min_rent': 500, 'max_rent': 1200},
    'NV': {'cities': ['Las Vegas', 'Henderson', 'Reno'], 'min_rent': 850, 'max_rent': 2200},
    'AR': {'cities': ['Little Rock', 'Fort Smith', 'Fayetteville'], 'min_rent': 450, 'max_rent': 1100},
    'MS': {'cities': ['Jackson', 'Gulfport', 'Southaven'], 'min_rent': 500, 'max_rent': 1200},
    'KS': {'cities': ['Wichita', 'Overland Park', 'Kansas City'], 'min_rent': 550, 'max_rent': 1300},
    'NM': {'cities': ['Albuquerque', 'Las Cruces', 'Rio Rancho'], 'min_rent': 600, 'max_rent': 1400},
    'NE': {'cities': ['Omaha', 'Lincoln', 'Bellevue'], 'min_rent': 600, 'max_rent': 1300},
    'WV': {'cities': ['Charleston', 'Huntington', 'Morgantown'], 'min_rent': 450, 'max_rent': 1000},
    'ID': {'cities': ['Boise', 'Meridian', 'Nampa'], 'min_rent': 750, 'max_rent': 1600},
    'HI': {'cities': ['Honolulu', 'Hilo', 'Kailua'], 'min_rent': 1500, 'max_rent': 3500},
    'NH': {'cities': ['Manchester', 'Nashua', 'Concord'], 'min_rent': 800, 'max_rent': 1800},
    'ME': {'cities': ['Portland', 'Lewiston', 'Bangor'], 'min_rent': 700, 'max_rent': 1600},
    'MT': {'cities': ['Billings', 'Missoula', 'Great Falls'], 'min_rent': 550, 'max_rent': 1300},
    'RI': {'cities': ['Providence', 'Warwick', 'Cranston'], 'min_rent': 900, 'max_rent': 2000},
    'DE': {'cities': ['Wilmington', 'Dover', 'Newark'], 'min_rent': 850, 'max_rent': 1700},
    'SD': {'cities': ['Sioux Falls', 'Rapid City', 'Aberdeen'], 'min_rent': 550, 'max_rent': 1200},
    'ND': {'cities': ['Fargo', 'Bismarck', 'Grand Forks'], 'min_rent': 550, 'max_rent': 1200},
    'AK': {'cities': ['Anchorage', 'Juneau', 'Fairbanks'], 'min_rent': 1000, 'max_rent': 2200},
    'VT': {'cities': ['Burlington', 'South Burlington', 'Rutland'], 'min_rent': 850, 'max_rent': 1600},
    'WY': {'cities': ['Cheyenne', 'Casper', 'Laramie'], 'min_rent': 600, 'max_rent': 1300}
}

STREETS = ['Main St', 'Oak Ave', 'Pine St', 'Elm Dr', 'Maple Ave', 'Cedar Ln', 'Park Ave', 'Broadway', 'Washington St', 'Lakeview Dr']

def generate_street_address() -> str:
    """Generate random street address"""
    return f"{random.randint(100, 9999)} {random.choice(STREETS)}"

def generate_listings_for_state(state: str, count: int) -> List[Dict]:
    """Generate sample listings for a state"""
    market = US_RENTAL_MARKETS.get(state, {'cities': ['Unknown'], 'min_rent': 800, 'max_rent': 1500})
    
    listings = []
    base_date = datetime(2025, 1, 1)
    
    for i in range(count):
        city = random.choice(market['cities'])
        
        bedrooms = random.randint(market.get('min_beds', 1), market.get('max_beds', 4))
        bathrooms = round(random.uniform(1, int(bedrooms) + 1), 1) if bedrooms > 0 else 1.0
        sqft = random.randint(market.get('min_sqft', 500), market.get('max_sqft', 2000))
        
        base_rent = random.randint(market.get('min_rent', 500), market.get('max_rent', 2000))
        rent = base_rent + (bedrooms * random.randint(200, 600))
        
        zip_code = random.randint(10000, 99999)
        
        property_types = ['apartment', 'house', 'condo', 'townhouse']
        prop_type = random.choice(property_types)
        
        listings.append({
            'source_listing_id': f'sim_{state}_{i:05d}',
            'source_platform': 'simulator',
            'street_address': generate_street_address(),
            'city': city,
            'state': state,
            'zip_code': str(zip_code),
            'property_type': prop_type,
            'bedrooms': bedrooms,
            'bathrooms': bathrooms,
            'square_feet': sqft,
            'rent': float(rent),
            'listing_url': f'https://example.com/{state}/{i}',
            'first_seen': (base_date + timedelta(days=random.randint(0, 50))).isoformat()
        })
    
    return listings

def run_full_usa_ingestion():
    """Generate and ingest data for all 50 states"""
    print("="*60)
    print("GENERATING RENTAL DATA: ALL 50 US STATES")
    print("="*60)
    
    db = RentalIntelDB()
    if not db.connect():
        print("❌ Failed to connect to database")
        return
    
    try:
        db.auto_create_partitions()
        
        total_stats = {'listings': 0, 'properties': 0, 'price_changes': 0}
        
        for state, market in US_RENTAL_MARKETS.items():
            print(f"\n📍 {state}: ", end='', flush=True)
            
            # Generate 20-50 listings per state
            listings_count = random.randint(20, 50)
            listings = generate_listings_for_state(state, listings_count)
            
            state_properties = 0
            state_listings = 0
            
            for listing in listings:
                try:
                    # Create property
                    prop = PropertyData(
                        street_address=listing['street_address'],
                        city=listing['city'],
                        state=listing['state'],
                        zip_code=listing['zip_code'],
                        property_type=listing['property_type'],
                        bedrooms=listing['bedrooms'],
                        bathrooms=listing['bathrooms'],
                        square_feet=listing['square_feet']
                    )
                    
                    property_id = db.upsert_property(prop)
                    if property_id:
                        state_properties += 1
                        
                        # Create listing
                        list_data = ListingData(
                            source_platform=listing['source_platform'],
                            source_listing_id=listing['source_listing_id'],
                            listing_url=listing['listing_url'],
                            rent=listing['rent']
                        )
                        
                        listing_id = db.upsert_listing(property_id, list_data)
                        if listing_id and listing['rent']:
                            price_recorded = db.record_price(listing_id, listing['rent'])
                            if price_recorded:
                                total_stats['price_changes'] += 1
                                state_listings += 1
                
                except Exception as e:
                    print(f"✗ {e}", end='', flush=True)
            
            print(f"{state_listings} listings, {state_properties} props")
            total_stats['listings'] += state_listings
            total_stats['properties'] += state_properties
            
            # Calculate ZIP metrics for this state's ZIPs
            zips_in_state = set([l['zip_code'] for l in listings])
            for zip_code in zips_in_state:
                try:
                    db.calculate_zip_metrics(zip_code)
                except:
                    pass
        
        print("\n" + "="*60)
        print("✅ FULL USA DATA INGESTION COMPLETE")
        print("="*60)
        print(f"Total Listings: {total_stats['listings']:,}")
        print(f"Total Properties: {total_stats['properties']:,}")
        print(f"Price Changes: {total_stats['price_changes']:,}")
        print(f"States Covered: {len(US_RENTAL_MARKETS)}")
        print("="*60)
        
    finally:
        db.close()

if __name__ == "__main__":
    run_full_usa_ingestion()

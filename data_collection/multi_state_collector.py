#!/usr/bin/env python3
"""
Multi-State Rental Data Collector
Collects rental listings from all 50 US states
"""

import os
import json
import time
import logging
from datetime import datetime
from typing import List, Dict, Optional
from dataclasses import dataclass
from abc import ABC, abstractmethod
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('multi_state_collector')

@dataclass
class ListingData:
    """Standardized listing data"""
    source: str
    source_id: str
    street_address: str
    city: str
    state: str
    zipcode: str
    property_type: Optional[str] = None
    bedrooms: Optional[int] = None
    bathrooms: Optional[float] = None
    sqft: Optional[int] = None
    rent: Optional[float] = None
    listing_url: Optional[str] = None
    first_seen: Optional[str] = None

class BaseCollector(ABC):
    """Abstract base class for data collectors"""
    
    def __init__(self, state_code: str):
        self.state_code = state_code
        self.rate_limit_delay = 1.0
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        })
    
    @abstractmethod
    def collect(self, city: Optional[str] = None) -> List[ListingData]:
        """Collect listings for state/city"""
        pass
    
    def _rate_limit(self):
        """Respect rate limits"""
        time.sleep(self.rate_limit_delay)

# Major US cities by state (Top 10-20 per state)
US_CITIES = {
    'CA': ['Los Angeles', 'San Diego', 'San Jose', 'San Francisco', 'Fresno', 'Sacramento', 'Long Beach', 'Oakland', 'Bakersfield', 'Anaheim'],
    'TX': ['Houston', 'Dallas', 'Austin', 'San Antonio', 'Fort Worth', 'El Paso', 'Arlington', 'Corpus Christi', 'Plano', 'Lubbock'],
    'NY': ['New York', 'Buffalo', 'Rochester', 'Yonkers', 'Syracuse', 'Albany', 'New Rochelle', 'Mount Vernon', 'Schenectady', 'Utica'],
    'FL': ['Jacksonville', 'Miami', 'Tampa', 'Orlando', 'St. Petersburg', 'Hialeah', 'Tallahassee', 'Fort Lauderdale', 'Port St. Lucie', 'Cape Coral'],
    'IL': ['Chicago', 'Aurora', 'Rockford', 'Joliet', 'Naperville', 'Springfield', 'Peoria', 'Elgin', 'Waukegan', 'Cicero'],
    'PA': ['Philadelphia', 'Pittsburgh', 'Allentown', 'Erie', 'Reading', 'Scranton', 'Bethlehem', 'Lancaster', 'Harrisburg', 'Altoona'],
    'OH': ['Columbus', 'Cleveland', 'Cincinnati', 'Toledo', 'Akron', 'Dayton', 'Parma', 'Canton', 'Youngstown', 'Lorain'],
    'GA': ['Atlanta', 'Augusta', 'Columbus', 'Savannah', 'Athens', 'Sandy Springs', 'Roswell', 'Johns Creek', 'Warner Robins', 'Albany'],
    'NC': ['Charlotte', 'Raleigh', 'Greensboro', 'Durham', 'Winston-Salem', 'Fayetteville', 'Cary', 'Wilmington', 'High Point', 'Concord'],
    'MI': ['Detroit', 'Grand Rapids', 'Warren', 'Sterling Heights', 'Ann Arbor', 'Lansing', 'Flint', 'Dearborn', 'Livonia', 'Troy'],
    'NJ': ['Newark', 'Jersey City', 'Paterson', 'Elizabeth', 'Edison', 'Woodbridge', 'Lakewood', 'Toms River', 'Hamilton', 'Clifton'],
    'VA': ['Virginia Beach', 'Norfolk', 'Chesapeake', 'Arlington', 'Richmond', 'Newport News', 'Alexandria', 'Hampton', 'Roanoke', 'Portsmouth'],
    'WA': ['Seattle', 'Spokane', 'Tacoma', 'Vancouver', 'Bellevue', 'Kent', 'Everett', 'Renton', 'Yakima', 'Federal Way'],
    'AZ': ['Phoenix', 'Tucson', 'Mesa', 'Chandler', 'Gilbert', 'Glendale', 'Scottsdale', 'Tempe', 'Peoria', 'Surprise'],
    'MA': ['Boston', 'Worcester', 'Springfield', 'Cambridge', 'Lowell', 'Brockton', 'New Bedford', 'Quincy', 'Lynn', 'Fall River'],
    'TN': ['Memphis', 'Nashville', 'Knoxville', 'Chattanooga', 'Clarksville', 'Murfreesboro', 'Jackson', 'Franklin', 'Johnson City', 'Bartlett'],
    'IN': ['Indianapolis', 'Fort Wayne', 'Evansville', 'South Bend', 'Carmel', 'Fishers', 'Bloomington', 'Hammond', 'Gary', 'Lafayette'],
    'MO': ['Kansas City', 'St. Louis', 'Springfield', 'Columbia', 'Independence', 'Lee\'s Summit', 'O\'Fallon', 'St. Joseph', 'St. Charles', 'St. Peters'],
    'MD': ['Baltimore', 'Frederick', 'Rockville', 'Gaithersburg', 'Bowie', 'Hagerstown', 'Annapolis', 'College Park', 'Salisbury', 'Laurel'],
    'WI': ['Milwaukee', 'Madison', 'Green Bay', 'Kenosha', 'Racine', 'Appleton', 'Waukesha', 'Eau Claire', 'Oshkosh', 'Janesville'],
    'CO': ['Denver', 'Colorado Springs', 'Aurora', 'Fort Collins', 'Lakewood', 'Thornton', 'Arvada', 'Westminster', 'Pueblo', 'Centennial'],
    'MN': ['Minneapolis', 'St. Paul', 'Rochester', 'Duluth', 'Bloomington', 'Brooklyn Park', 'Plymouth', 'St. Cloud', 'Eagan', 'Woodbury'],
    'SC': ['Charleston', 'Columbia', 'North Charleston', 'Mount Pleasant', 'Rock Hill', 'Greenville', 'Summerville', 'Goose Creek', 'Hilton Head Island', 'Sumter'],
    'AL': ['Birmingham', 'Montgomery', 'Mobile', 'Huntsville', 'Tuscaloosa', 'Hoover', 'Dothan', 'Auburn', 'Decatur', 'Madison'],
    'LA': ['New Orleans', 'Baton Rouge', 'Shreveport', 'Lafayette', 'Lake Charles', 'Kenner', 'Bossier City', 'Monroe', 'Alexandria', 'Houma'],
    'KY': ['Louisville', 'Lexington', 'Bowling Green', 'Owensboro', 'Covington', 'Hopkinsville', 'Richmond', 'Florence', 'Georgetown', 'Henderson'],
    'OR': ['Portland', 'Salem', 'Eugene', 'Gresham', 'Hillsboro', 'Beaverton', 'Bend', 'Medford', 'Springfield', 'Corvallis'],
    'OK': ['Oklahoma City', 'Tulsa', 'Norman', 'Broken Arrow', 'Lawton', 'Edmond', 'Moore', 'Midwest City', 'Enid', 'Stillwater'],
    'CT': ['Bridgeport', 'New Haven', 'Stamford', 'Hartford', 'Waterbury', 'Norwalk', 'Danbury', 'New Britain', 'West Hartford', 'Greenwich'],
    'UT': ['Salt Lake City', 'West Valley City', 'Provo', 'West Jordan', 'Orem', 'Sandy', 'Ogden', 'St. George', 'Layton', 'South Jordan'],
    'IA': ['Des Moines', 'Cedar Rapids', 'Davenport', 'Sioux City', 'Iowa City', 'Waterloo', 'Ames', 'West Des Moines', 'Council Bluffs', 'Dubuque'],
    'NV': ['Las Vegas', 'Henderson', 'North Las Vegas', 'Reno', 'Sparks', 'Carson City', 'Fernley', 'Elko', 'Mesquite', 'Boulder City'],
    'AR': ['Little Rock', 'Fort Smith', 'Fayetteville', 'Springdale', 'Jonesboro', 'North Little Rock', 'Conway', 'Rogers', 'Pine Bluff', 'Bentonville'],
    'MS': ['Jackson', 'Gulfport', 'Southaven', 'Hattiesburg', 'Biloxi', 'Meridian', 'Tupelo', 'Greenville', 'Olive Branch', 'Horn Lake'],
    'KS': ['Wichita', 'Overland Park', 'Kansas City', 'Olathe', 'Topeka', 'Lawrence', 'Shawnee', 'Manhattan', 'Lenexa', 'Salina'],
    'NM': ['Albuquerque', 'Las Cruces', 'Rio Rancho', 'Santa Fe', 'Roswell', 'Farmington', 'Clovis', 'Hobbs', 'Alamogordo', 'Carlsbad'],
    'NE': ['Omaha', 'Lincoln', 'Bellevue', 'Grand Island', 'Kearney', 'Fremont', 'Hastings', 'Norfolk', 'North Platte', 'Columbus'],
    'WV': ['Charleston', 'Huntington', 'Morgantown', 'Parkersburg', 'Wheeling', 'Beckley', 'Fairmont', 'Martinsburg', 'Clarksburg', 'South Charleston'],
    'ID': ['Boise', 'Meridian', 'Nampa', 'Idaho Falls', 'Pocatello', 'Caldwell', 'Coeur d\'Alene', 'Twin Falls', 'Lewiston', 'Post Falls'],
    'HI': ['Honolulu', 'East Honolulu', 'Pearl City', 'Hilo', 'Waipahu', 'Kailua', 'Kaneohe', 'Kahului', 'Mililani Town', 'Ewa Gentry'],
    'NH': ['Manchester', 'Nashua', 'Concord', 'Dover', 'Rochester', 'Keene', 'Derry', 'Portsmouth', 'Laconia', 'Lebanon'],
    'ME': ['Portland', 'Lewiston', 'Bangor', 'South Portland', 'Auburn', 'Biddeford', 'Sanford', 'Saco', 'Augusta', 'Westbrook'],
    'MT': ['Billings', 'Missoula', 'Great Falls', 'Bozeman', 'Butte', 'Helena', 'Kalispell', 'Havre', 'Anaconda', 'Miles City'],
    'RI': ['Providence', 'Warwick', 'Cranston', 'Pawtucket', 'East Providence', 'Woonsocket', 'Cumberland', 'Coventry', 'North Providence', 'South Kingstown'],
    'DE': ['Wilmington', 'Dover', 'Newark', 'Middletown', 'Smyrna', 'Milford', 'Seaford', 'Georgetown', 'Elsmere', 'New Castle'],
    'SD': ['Sioux Falls', 'Rapid City', 'Aberdeen', 'Brookings', 'Watertown', 'Mitchell', 'Yankton', 'Pierre', 'Huron', 'Spearfish'],
    'ND': ['Fargo', 'Bismarck', 'Grand Forks', 'Minot', 'West Fargo', 'Williston', 'Mandan', 'Dickinson', 'Jamestown', 'Wahpeton'],
    'AK': ['Anchorage', 'Juneau', 'Fairbanks', 'Badger', 'Knik-Fairview', 'College', 'Sitka', 'Lakes', 'Ketchikan', 'Wasilla'],
    'VT': ['Burlington', 'South Burlington', 'Rutland', 'Essex Junction', 'Barre', 'Montpelier', 'Winooski', 'St. Johnsbury', 'Brattleboro', 'Middlebury'],
    'WY': ['Cheyenne', 'Casper', 'Laramie', 'Gillette', 'Rock Springs', 'Sheridan', 'Green River', 'Evanston', 'Riverton', 'Jackson']
}

class MultiStateCollector:
    """Orchestrates rental data collection across all 50 states"""
    
    def __init__(self):
        self.collectors = {}
        self.results = []
        
    def register_collector(self, state: str, collector: BaseCollector):
        """Register a collector for a state"""
        self.collectors[state] = collector
        logger.info(f"Registered collector for {state}")
    
    def collect_state(self, state: str, cities: List[str] = None) -> List[ListingData]:
        """Collect data for a single state"""
        logger.info(f"Starting collection for {state}")
        
        if state not in US_CITIES:
            logger.warning(f"No cities defined for {state}")
            return []
        
        cities_to_collect = cities or US_CITIES[state]
        state_results = []
        
        # Use Craigslist scraper for demo
        try:
            from craigslist_collector import CraigslistCollector
            collector = CraigslistCollector(state)
            
            for city in cities_to_collect[:5]:  # Limit to 5 cities per state for demo
                try:
                    listings = collector.collect(city)
                    state_results.extend(listings)
                    logger.info(f"Collected {len(listings)} from {city}, {state}")
                    time.sleep(1)  # Rate limit
                except Exception as e:
                    logger.error(f"Failed to collect {city}: {e}")
                    
        except Exception as e:
            logger.error(f"Failed to initialize collector for {state}: {e}")
        
        return state_results
    
    def collect_all_states(self, max_workers: int = 5) -> Dict[str, List[ListingData]]:
        """Collect data from all 50 states in parallel"""
        logger.info("Starting multi-state collection...")
        
        all_results = {}
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_state = {
                executor.submit(self.collect_state, state): state 
                for state in US_CITIES.keys()
            }
            
            for future in as_completed(future_to_state):
                state = future_to_state[future]
                try:
                    results = future.result()
                    all_results[state] = results
                    logger.info(f"✓ {state}: {len(results)} listings")
                except Exception as e:
                    logger.error(f"✗ {state}: {e}")
                    all_results[state] = []
        
        return all_results
    
    def get_summary(self, results: Dict[str, List[ListingData]]) -> Dict:
        """Generate collection summary"""
        total_listings = sum(len(listings) for listings in results.values())
        states_with_data = sum(1 for listings in results.values() if len(listings) > 0)
        
        return {
            'total_listings': total_listings,
            'states_collected': states_with_data,
            'states_total': len(results),
            'by_state': {state: len(listings) for state, listings in results.items()}
        }


def main():
    """CLI entry point"""
    import sys
    
    collector = MultiStateCollector()
    
    if len(sys.argv) > 1 and sys.argv[1] == 'all':
        # Collect all 50 states
        results = collector.collect_all_states()
        summary = collector.get_summary(results)
        
        print("\n" + "="*60)
        print("MULTI-STATE COLLECTION COMPLETE")
        print("="*60)
        print(f"Total listings: {summary['total_listings']:,}")
        print(f"States with data: {summary['states_collected']}/{summary['states_total']}")
        print("\nTop 10 states:")
        sorted_states = sorted(summary['by_state'].items(), key=lambda x: x[1], reverse=True)
        for state, count in sorted_states[:10]:
            print(f"  {state}: {count:,} listings")
            
    else:
        # Collect single state (default CA)
        state = sys.argv[1] if len(sys.argv) > 1 else 'CA'
        results = collector.collect_state(state)
        print(f"\nCollected {len(results)} listings from {state}")


if __name__ == '__main__':
    main()

#!/usr/bin/env python3
"""
Craigslist Rental Listing Scraper
Collects apartment/housing rentals from Craigslist for any city
"""

import re
import time
import random
import logging
import requests
from datetime import datetime
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from typing import List, Optional
from multi_state_collector import BaseCollector, ListingData

logger = logging.getLogger('craigslist_collector')

# Craigslist subdomains for major cities
CRAIGSLIST_DOMAINS = {
    'sfbay': ['San Francisco', 'Oakland', 'San Jose', 'Berkeley'],
    'losangeles': ['Los Angeles', 'Burbank', 'Long Beach', 'Santa Monica'],
    'sandiego': ['San Diego', 'Chula Vista'],
    'seattle': ['Seattle', 'Bellevue', 'Tacoma'],
    'portland': ['Portland', 'Beaverton'],
    'denver': ['Denver', 'Boulder'],
    'phoenix': ['Phoenix', 'Tempe', 'Scottsdale'],
    'dallas': ['Dallas', 'Fort Worth', 'Arlington'],
    'houston': ['Houston', 'Galveston'],
    'austin': ['Austin', 'Round Rock'],
    'santabarbara': ['Santa Barbara'],
    'orangecounty': ['Irvine', 'Anaheim', 'Santa Ana'],
    'inlandempire': ['Riverside', 'San Bernardino'],
    ' Sacramento': ['Sacramento'],
    'fresno': ['Fresno'],
    'bakersfield': ['Bakersfield'],
    'reno': ['Reno'],
    'lasvegas': ['Las Vegas', 'Henderson'],
    'atlanta': ['Atlanta'],
    'boston': ['Boston', 'Cambridge'],
    'chicago': ['Chicago'],
    'miami': ['Miami', 'Fort Lauderdale'],
    'minneapolis': ['Minneapolis', 'St. Paul'],
    'newyork': ['New York', 'Brooklyn', 'Queens'],
    'philadelphia': ['Philadelphia'],
    'washingtondc': ['Washington DC'],
    'baltimore': ['Baltimore'],
    'detroit': ['Detroit'],
    'detroit': ['Detroit'],
    'stlouis': ['St. Louis'],
    'kansascity': ['Kansas City'],
    'orlando': ['Orlando'],
    'tampa': ['Tampa'],
    'jacksonville': ['Jacksonville'],
    'neworleans': ['New Orleans'],
    'raleigh': ['Raleigh', 'Durham'],
    'charlotte': ['Charlotte'],
    'nashville': ['Nashville'],
    'memphis': ['Memphis'],
    'indianapolis': ['Indianapolis'],
    'columbus': ['Columbus'],
    'cleveland': ['Cleveland'],
    'cincinnati': ['Cincinnati'],
    'pittsburgh': ['Pittsburgh'],
    'milwaukee': ['Milwaukee'],
    'honolulu': ['Honolulu'],
    'anchorage': ['Anchorage'],
}

class CraigslistCollector(BaseCollector):
    """Scraper for Craigslist rental listings"""
    
    def __init__(self, state_code: str):
        super().__init__(state_code)
        self.rate_limit_delay = 2.0  # Be respectful
        self.base_delay = 2.0
        
    def _find_domain(self, city: str) -> Optional[str]:
        """Find Craigslist domain for a city"""
        # Direct match
        for domain, cities in CRAIGSLIST_DOMAINS.items():
            if city in cities:
                return domain
        
        # Try searching by city name in URL format
        city_slug = city.lower().replace(' ', '')
        return city_slug
    
    def _extract_price(self, text: str) -> Optional[float]:
        """Extract rent from text like '$1,200'"""
        if not text:
            return None
        match = re.search(r'\$([\d,]+)', text)
        if match:
            price_str = match.group(1).replace(',', '')
            try:
                return float(price_str)
            except:
                return None
        return None
    
    def _extract_bedrooms(self, text: str) -> Optional[int]:
        """Extract bedroom count like '2br' or '2br/2ba'"""
        if not text:
            return None
        # Match patterns: 2br, 2 br, 2br/2ba, 2br 2ba, etc.
        match = re.search(r'(\d+)\s*br', text.lower())
        if match:
            try:
                return int(match.group(1))
            except:
                return None
        # Look for "studio"
        if 'studio' in text.lower():
            return 0
        return None
    
    def _extract_bathrooms(self, text: str) -> Optional[float]:
        """Extract bathroom count like '2ba'"""
        if not text:
            return None
        match = re.search(r'(\d+(?:\.\d+)?)\s*ba', text.lower())
        if match:
            try:
                return float(match.group(1))
            except:
                return None
        return None
    
    def _extract_sqft(self, text: str) -> Optional[int]:
        """Extract square footage like '850ft2'"""
        if not text:
            return None
        match = re.search(r'([\d,]+)\s*ft\u00b2?', text.lower())
        if match:
            try:
                return int(match.group(1).replace(',', ''))
            except:
                return None
        return None
    
    def _parse_address(self, location: str) -> tuple:
        """Parse city, state from location string"""
        # Simple parsing - could be enhanced
        parts = location.split(',')
        if len(parts) >= 2:
            city = parts[0].strip()
            state = parts[1].strip()[:2].upper() if len(parts[1].strip()) > 2 else parts[1].strip().upper()
            return city, state
        return location.strip(), self.state_code
    
    def collect(self, city: Optional[str] = None) -> List[ListingData]:
        """Collect rental listings from Craigslist for a city"""
        logger.info(f"Collecting Craigslist listings for {city}, {self.state_code}")
        
        domain = self._find_domain(city) if city else self.state_code.lower()
        if not domain:
            logger.warning(f"No Craigslist domain found for {city}")
            return []
        
        listings = []
        
        # Construct search URL
        base_url = f"https://{domain}.craigslist.org"
        search_url = f"{base_url}/search/apa"
        
        try:
            logger.info(f"Fetching {search_url}")
            response = self.session.get(search_url, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Find all listing rows
            result_rows = soup.find_all('li', class_='cl-page')
            
            for row in result_rows[:50]:  # Limit to 50 per city for demo
                try:
                    # Extract listing info
                    link_tag = row.find('a', class_='titlestring')
                    if not link_tag:
                        continue
                    
                    listing_url = link_tag.get('href', '')
                    if listing_url.startswith('/'):
                        listing_url = urljoin(base_url, listing_url)
                    
                    # Get title which often contains details
                    title = link_tag.get_text(strip=True)
                    
                    # Extract price from meta
                    meta_tag = row.find('div', class_='meta')
                    price_text = meta_tag.get_text() if meta_tag else ''
                    rent = self._extract_price(price_text) or self._extract_price(title)
                    
                    # Extract bedrooms/bathrooms/sqft from title
                    br = self._extract_bedrooms(title)
                    ba = self._extract_bathrooms(title)
                    sqft = self._extract_sqft(title)
                    
                    # Get location
                    location_span = row.find('span', class_='result-hood')
                    location = location_span.get_text(strip='()') if location_span else city
                    
                    city_parsed, state = self._parse_address(location)
                    
                    # Get listing ID from URL
                    listing_id = re.search(r'/d/[^/]+/(\d+)\.html', listing_url)
                    listing_id = listing_id.group(1) if listing_id else f"cl_{int(time.time()*1000)}_{random.randint(1000,9999)}"
                    
                    listing = ListingData(
                        source='craigslist',
                        source_id=listing_id,
                        street_address=title[:100],  # Use title as address placeholder
                        city=city_parsed or city,
                        state=state or self.state_code,
                        zipcode='',  # Would need geocoding
                        property_type='apartment',  # CL mostly apartments
                        bedrooms=br,
                        bathrooms=ba,
                        sqft=sqft,
                        rent=rent,
                        listing_url=listing_url,
                        first_seen=datetime.now().isoformat()
                    )
                    
                    listings.append(listing)
                    
                except Exception as e:
                    logger.error(f"Failed to parse listing: {e}")
                    continue
            
            logger.info(f"Collected {len(listings)} listings from Craigslist {domain}")
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed for {domain}: {e}")
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
        
        # Rate limiting
        delay = self.base_delay + random.uniform(1, 3)
        logger.debug(f"Rate limiting: sleeping {delay:.1f}s")
        time.sleep(delay)
        
        return listings


def scrape_city(city: str, state: str) -> List[ListingData]:
    """Helper function to scrape a single city"""
    collector = CraigslistCollector(state)
    return collector.collect(city)


if __name__ == '__main__':
    # Test
    logging.basicConfig(level=logging.INFO)
    
    # Test San Francisco
    listings = scrape_city('San Francisco', 'CA')
    print(f"Found {len(listings)} listings in SF")
    
    if listings:
        print("\nSample listing:")
        l = listings[0]
        print(f"  {l.street_address}")
        print(f"  {l.city}, {l.state}")
        print(f"  ${l.rent:,.0f} | {l.bedrooms}br/{l.bathrooms}ba | {l.sqft}ft²")
        print(f"  {l.listing_url}")

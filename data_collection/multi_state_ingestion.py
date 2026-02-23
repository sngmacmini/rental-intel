#!/usr/bin/env python3
"""
Multi-State Data Ingestion Pipeline
Collects and ingests rental data from all 50 states into PostgreSQL
"""

import os
import sys
import json
import random
import logging
import argparse
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from dataclasses import dataclass

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from daily_operations import RentalIntelDB, PropertyData, ListingData
from multi_state_collector import MultiStateCollector, US_CITIES, ListingData as CollectorListing

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('multi_state_ingestion')

class MultiStateIngestionPipeline:
    """Pipeline for ingesting rental data from all 50 states"""
    
    def __init__(self):
        self.db = RentalIntelDB()
        self.collector = MultiStateCollector()
        self.stats = {
            'states_processed': 0,
            'cities_processed': 0,
            'listings_collected': 0,
            'properties_upserted': 0,
            'listings_upserted': 0,
            'price_changes': 0,
            'errors': []
        }
        
    def connect(self) -> bool:
        """Connect to database"""
        return self.db.connect()
    
    def close(self):
        """Close database connection"""
        self.db.close()
    
    def ingest_city(self, city: str, state: str, sample_mode: bool = False) -> Dict:
        """Ingest data for a single city"""
        logger.info(f"Processing {city}, {state}")
        
        city_stats = {
            'listings': 0,
            'properties': 0,
            'errors': 0
        }
        
        try:
            # Collect from Craigslist
            from craigslist_collector import CraigslistCollector
            collector = CraigslistCollector(state)
            listings = collector.collect(city)
            
            if sample_mode:
                listings = random.sample(listings, min(10, len(listings))) if len(listings) > 10 else listings
            
            for listing in listings:
                try:
                    # Create property
                    prop = PropertyData(
                        street_address=listing.street_address,
                        city=listing.city,
                        state=listing.state,
                        zip_code=listing.zipcode or f"{state}0000",
                        property_type=listing.property_type or 'apartment',
                        bedrooms=listing.bedrooms,
                        bathrooms=listing.bathrooms,
                        square_feet=listing.sqft
                    )
                    
                    property_id = self.db.upsert_property(prop)
                    if not property_id:
                        city_stats['errors'] += 1
                        continue
                    
                    # Create listing
                    list_data = ListingData(
                        source_platform=listing.source,
                        source_listing_id=listing.source_id,
                        listing_url=listing.listing_url,
                        rent=listing.rent
                    )
                    
                    listing_id = self.db.upsert_listing(property_id, list_data)
                    if listing_id and listing.rent:
                        price_recorded = self.db.record_price(listing_id, listing.rent)
                        if price_recorded:
                            self.stats['price_changes'] += 1
                        
                        city_stats['listings'] += 1
                        self.stats['properties_upserted'] += 1
                        self.stats['listings_upserted'] += 1
                        
                except Exception as e:
                    logger.error(f"Failed to ingest listing in {city}: {e}")
                    city_stats['errors'] += 1
            
            # Calculate ZIP metrics after all listings
            if listings:
                zip_code = listings[0].zipcode or f"{state}0000"
                self.db.calculate_zip_metrics(zip_code)
            
            self.stats['listings_collected'] += len(listings)
            
        except Exception as e:
            logger.error(f"Failed to process {city}: {e}")
            city_stats['errors'] += 1
            self.stats['errors'].append(f"{city}, {state}: {str(e)}")
        
        return city_stats
    
    def ingest_state(self, state: str, max_cities: int = 3, sample_mode: bool = False) -> Dict:
        """Ingest data for an entire state"""
        logger.info(f"\n{'='*60}")
        logger.info(f"PROCESSING STATE: {state}")
        logger.info(f"{'='*60}")
        
        if state not in US_CITIES:
            logger.warning(f"No cities defined for {state}")
            return {'state': state, 'cities': 0, 'listings': 0}
        
        state_stats = {
            'state': state,
            'cities': 0,
            'listings': 0,
            'errors': 0
        }
        
        cities = US_CITIES[state][:max_cities]  # Limit cities per state
        
        for city in cities:
            city_stats = self.ingest_city(city, state, sample_mode)
            state_stats['cities'] += 1
            state_stats['listings'] += city_stats['listings']
            state_stats['errors'] += city_stats['errors']
            self.stats['cities_processed'] += 1
            
            logger.info(f"  {city}: {city_stats['listings']} listings, {city_stats['errors']} errors")
        
        self.stats['states_processed'] += 1
        
        logger.info(f"State {state} complete: {state_stats['listings']} listings from {state_stats['cities']} cities")
        
        return state_stats
    
    def ingest_all_states(self, max_cities: int = 3, sample_mode: bool = True) -> Dict:
        """Ingest data from ALL 50 states"""
        logger.info("\n" + "="*60)
        logger.info("STARTING MULTI-STATE INGESTION")
        logger.info("Target: All 50 US states")
        logger.info("="*60 + "\n")
        
        start_time = datetime.now()
        
        # Process each state
        for state in sorted(US_CITIES.keys()):
            try:
                self.ingest_state(state, max_cities, sample_mode)
            except Exception as e:
                logger.error(f"Failed to process state {state}: {e}")
                self.stats['errors'].append(f"State {state}: {str(e)}")
        
        elapsed = (datetime.now() - start_time).total_seconds()
        
        return {
            'elapsed_seconds': elapsed,
            'states': self.stats['states_processed'],
            'cities': self.stats['cities_processed'],
            'listings': self.stats['listings_collected'],
            'properties': self.stats['properties_upserted'],
            'price_changes': self.stats['price_changes'],
            'errors': len(self.stats['errors'])
        }
    
    def generate_report(self) -> str:
        """Generate ingestion report"""
        report = f"""
{'='*60}
MULTI-STATE INGESTION REPORT
{'='*60}

States Processed: {self.stats['states_processed']}
Cities Processed: {self.stats['cities_processed']}
Listings Collected: {self.stats['listings_collected']:,}
Properties Upserted: {self.stats['properties_upserted']:,}
Listings Upserted: {self.stats['listings_upserted']:,}
Price Changes: {self.stats['price_changes']:,}
Errors: {len(self.stats['errors'])}

{'='*60}
"""
        return report


def main():
    parser = argparse.ArgumentParser(description='Multi-state rental data ingestion')
    parser.add_argument('--state', '-s', help='Process single state (e.g., CA)')
    parser.add_argument('--city', '-c', help='Process single city (e.g., "San Francisco")')
    parser.add_argument('--all', '-a', action='store_true', help='Process all 50 states')
    parser.add_argument('--max-cities', '-m', type=int, default=3, help='Max cities per state')
    parser.add_argument('--sample', action='store_true', help='Sample mode (10 listings per city)')
    
    args = parser.parse_args()
    
    pipeline = MultiStateIngestionPipeline()
    
    if not pipeline.connect():
        print("Failed to connect to database")
        sys.exit(1)
    
    try:
        if args.city and args.state:
            # Single city mode
            stats = pipeline.ingest_city(args.city, args.state, args.sample)
            print(f"\n{args.city}, {args.state}: {stats['listings']} listings ingested")
            
        elif args.state:
            # Single state mode
            stats = pipeline.ingest_state(args.state, args.max_cities, args.sample)
            print(f"\nState {args.state}: {stats['listings']} listings from {stats['cities']} cities")
            
        elif args.all:
            # All states mode
            summary = pipeline.ingest_all_states(args.max_cities, args.sample)
            print(pipeline.generate_report())
            print(f"\nCompleted in {summary['elapsed_seconds']:.1f} seconds")
            
        else:
            # Demo mode - just CA cities
            print("Demo mode: Processing California cities...")
            for city in ['San Francisco', 'Los Angeles', 'San Diego']:
                stats = pipeline.ingest_city(city, 'CA', True)
                print(f"  {city}: {stats['listings']} listings")
    
    finally:
        pipeline.close()


if __name__ == '__main__':
    main()

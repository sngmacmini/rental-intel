#!/usr/bin/env python3
"""
Rental Intelligence Daily Operations
Automated daily data ingestion and metric calculation
"""

import os
import json
import hashlib
import logging
from datetime import datetime, timedelta, date
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass
import psycopg2
from psycopg2.extras import execute_values, RealDictCursor
import schedule
import time

# Configuration
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'rental_intel')
DB_USER = os.getenv('DB_USER', 'sngmacmini')  # macOS default user
DB_PASSWORD = os.getenv('DB_PASSWORD', '')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('rental_intel')

@dataclass
class PropertyData:
    """Standardized property data structure"""
    street_address: str
    city: str
    state: str
    zip_code: str
    property_type: Optional[str] = None
    bedrooms: Optional[int] = None
    bathrooms: Optional[float] = None
    square_feet: Optional[int] = None
    
@dataclass
class ListingData:
    """Standardized listing data structure"""
    source_platform: str
    source_listing_id: str
    listing_url: Optional[str] = None
    rent: Optional[float] = None

class RentalIntelDB:
    """Database operations handler"""
    
    def __init__(self):
        self.conn = None
        self.cursor = None
        
    def connect(self) -> bool:
        """Establish database connection"""
        try:
            self.conn = psycopg2.connect(
                host=DB_HOST,
                port=DB_PORT,
                database=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD
            )
            self.cursor = self.conn.cursor(cursor_factory=RealDictCursor)
            logger.info("Database connection established")
            return True
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            return False
    
    def close(self):
        """Close database connection"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed")
    
    def upsert_property(self, prop: PropertyData) -> Optional[int]:
        """Insert or update property, return property_id"""
        try:
            # Normalize address
            normalized = self._normalize_address(
                prop.street_address, prop.city, prop.state, prop.zip_code
            )
            address_hash = hashlib.sha256(normalized.encode()).hexdigest()
            
            # Insert property
            self.cursor.execute("""
                INSERT INTO rental_intel.properties (
                    street_address, city, state, zip,
                    normalized_full_address, address_hash,
                    property_type, bedrooms, bathrooms, square_feet
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (address_hash) DO UPDATE SET
                    property_type = COALESCE(EXCLUDED.property_type, rental_intel.properties.property_type),
                    bedrooms = COALESCE(EXCLUDED.bedrooms, rental_intel.properties.bedrooms),
                    bathrooms = COALESCE(EXCLUDED.bathrooms, rental_intel.properties.bathrooms),
                    square_feet = COALESCE(EXCLUDED.square_feet, rental_intel.properties.square_feet),
                    updated_at = CURRENT_TIMESTAMP
                RETURNING property_id
            """, (
                prop.street_address, prop.city, prop.state, prop.zip_code,
                normalized, address_hash,
                prop.property_type, prop.bedrooms, prop.bathrooms, prop.square_feet
            ))
            
            result = self.cursor.fetchone()
            self.conn.commit()
            return result['property_id'] if result else None
            
        except Exception as e:
            logger.error(f"Failed to upsert property: {e}")
            self.conn.rollback()
            return None
    
    def upsert_listing(self, property_id: int, listing: ListingData) -> Optional[int]:
        """Insert or update listing, return listing_id"""
        try:
            self.cursor.execute("""
                INSERT INTO rental_intel.listings (
                    property_id, source_platform, source_listing_id,
                    listing_url, listing_status, last_verified_date
                ) VALUES (%s, %s, %s, %s, 'active', CURRENT_TIMESTAMP)
                ON CONFLICT (source_platform, source_listing_id) DO UPDATE SET
                    property_id = EXCLUDED.property_id,
                    listing_status = 'active',
                    last_verified_date = CURRENT_TIMESTAMP
                RETURNING listing_id
            """, (property_id, listing.source_platform, listing.source_listing_id, listing.listing_url))
            
            result = self.cursor.fetchone()
            self.conn.commit()
            return result['listing_id'] if result else None
            
        except Exception as e:
            logger.error(f"Failed to upsert listing: {e}")
            self.conn.rollback()
            return None
    
    def record_price(self, listing_id: int, rent: float) -> bool:
        """Record price change if different from last"""
        try:
            # Get last price
            self.cursor.execute("""
                SELECT observed_rent FROM rental_intel.rent_price_history
                WHERE listing_id = %s
                ORDER BY observed_date DESC
                LIMIT 1
            """, (listing_id,))
            
            last_price = self.cursor.fetchone()
            
            # Determine change type
            if last_price is None:
                change_type = 'new'
            elif rent > last_price['observed_rent']:
                change_type = 'increase'
            elif rent < last_price['observed_rent']:
                change_type = 'decrease'
            else:
                return False  # No change
            
            # Get property_id
            self.cursor.execute(
                "SELECT property_id FROM rental_intel.listings WHERE listing_id = %s",
                (listing_id,)
            )
            prop_result = self.cursor.fetchone()
            property_id = prop_result['property_id']
            
            # Calculate rent per sqft
            rent_per_sqft = None
            if property_id:
                self.cursor.execute(
                    "SELECT square_feet FROM rental_intel.properties WHERE property_id = %s",
                    (property_id,)
                )
                prop_data = self.cursor.fetchone()
                if prop_data and prop_data['square_feet']:
                    rent_per_sqft = round(rent / prop_data['square_feet'], 4)
            
            # Insert price record
            self.cursor.execute("""
                INSERT INTO rental_intel.rent_price_history (
                    listing_id, property_id, observed_rent, rent_per_sqft,
                    change_type, observed_date
                ) VALUES (%s, %s, %s, %s, %s, CURRENT_DATE)
            """, (listing_id, property_id, rent, rent_per_sqft, change_type))
            
            self.conn.commit()
            return True
            
        except Exception as e:
            logger.error(f"Failed to record price: {e}")
            self.conn.rollback()
            return False
    
    def mark_stale_listings(self, days: int = 30) -> int:
        """Mark listings inactive after N days"""
        try:
            self.cursor.execute("""
                UPDATE rental_intel.listings
                SET listing_status = 'inactive',
                    updated_at = CURRENT_TIMESTAMP
                WHERE listing_status = 'active'
                AND last_verified_date < CURRENT_DATE - INTERVAL '%s days'
                RETURNING listing_id
            """, (days,))
            
            stale_count = self.cursor.rowcount
            self.conn.commit()
            logger.info(f"Marked {stale_count} listings as inactive")
            return stale_count
            
        except Exception as e:
            logger.error(f"Failed to mark stale listings: {e}")
            self.conn.rollback()
            return 0
    
    def calculate_zip_metrics(self, zip_code: str, metric_date: date = None) -> bool:
        """Calculate daily metrics for a ZIP code"""
        if metric_date is None:
            metric_date = date.today()
        
        try:
            self.cursor.execute("SELECT rental_intel.calculate_zip_metrics(%s, %s)", 
                              (zip_code, metric_date))
            self.conn.commit()
            return True
            
        except Exception as e:
            logger.error(f"Failed to calculate ZIP metrics for {zip_code}: {e}")
            self.conn.rollback()
            return False
    
    def get_all_zips(self) -> List[str]:
        """Get all unique ZIP codes"""
        try:
            self.cursor.execute("SELECT DISTINCT zip FROM rental_intel.properties")
            results = self.cursor.fetchall()
            return [r['zip'] for r in results]
            
        except Exception as e:
            logger.error(f"Failed to get ZIP codes: {e}")
            return []
    
    def auto_create_partitions(self) -> List[str]:
        """Create partitions for next 3 months"""
        try:
            self.cursor.execute("SELECT * FROM rental_intel.auto_create_partitions()")
            results = self.cursor.fetchall()
            self.conn.commit()
            return [r['month_created'] for r in results]
            
        except Exception as e:
            logger.error(f"Failed to create partitions: {e}")
            self.conn.rollback()
            return []
    
    def log_ingestion_start(self, source: str) -> int:
        """Start ingestion logging, return log_id"""
        try:
            self.cursor.execute("""
                INSERT INTO rental_intel.ingestion_log (source, status)
                VALUES (%s, 'running')
                RETURNING log_id
            """, (source,))
            
            result = self.cursor.fetchone()
            self.conn.commit()
            return result['log_id']
            
        except Exception as e:
            logger.error(f"Failed to start ingestion log: {e}")
            return 0
    
    def log_ingestion_end(self, log_id: int, stats: Dict):
        """Complete ingestion logging"""
        try:
            self.cursor.execute("""
                UPDATE rental_intel.ingestion_log
                SET run_end = CURRENT_TIMESTAMP,
                    records_scanned = %s,
                    records_inserted = %s,
                    records_updated = %s,
                    price_changes = %s,
                    errors = %s,
                    status = %s
                WHERE log_id = %s
            """, (
                stats.get('scanned', 0),
                stats.get('inserted', 0),
                stats.get('updated', 0),
                stats.get('price_changes', 0),
                json.dumps(stats.get('errors', [])),
                stats.get('status', 'completed'),
                log_id
            ))
            
            self.conn.commit()
            
        except Exception as e:
            logger.error(f"Failed to end ingestion log: {e}")
            self.conn.rollback()
    
    @staticmethod
    def _normalize_address(street: str, city: str, state: str, zip_code: str) -> str:
        """Normalize address for consistent hashing"""
        combined = f"{street} {city} {state} {zip_code}"
        return ' '.join(combined.upper().split())


class DataIngestionEngine:
    """Engine for ingesting rental data from various sources"""
    
    def __init__(self, db: RentalIntelDB):
        self.db = db
        self.stats = {
            'scanned': 0,
            'inserted': 0,
            'updated': 0,
            'price_changes': 0,
            'errors': [],
            'status': 'running'
        }
    
    def ingest_batch(self, source: str, records: List[Dict]) -> Dict:
        """Ingest a batch of records"""
        log_id = self.db.log_ingestion_start(source)
        
        try:
            for record in records:
                self.stats['scanned'] += 1
                
                try:
                    # Create property
                    prop = PropertyData(
                        street_address=record['street_address'],
                        city=record['city'],
                        state=record['state'],
                        zip_code=record['zip_code'],
                        property_type=record.get('property_type'),
                        bedrooms=record.get('bedrooms'),
                        bathrooms=record.get('bathrooms'),
                        square_feet=record.get('square_feet')
                    )
                    
                    property_id = self.db.upsert_property(prop)
                    if not property_id:
                        continue
                    
                    # Create listing
                    listing = ListingData(
                        source_platform=source,
                        source_listing_id=record['source_listing_id'],
                        listing_url=record.get('listing_url'),
                        rent=record.get('rent')
                    )
                    
                    listing_id = self.db.upsert_listing(property_id, listing)
                    if listing_id:
                        if listing.rent:
                            price_recorded = self.db.record_price(listing_id, listing.rent)
                            if price_recorded:
                                self.stats['price_changes'] += 1
                        
                        self.stats['inserted'] += 1
                        
                except Exception as e:
                    self.stats['errors'].append(str(e))
                    logger.error(f"Failed to process record: {e}")
            
            self.stats['status'] = 'completed'
            
        except Exception as e:
            self.stats['status'] = 'failed'
            self.stats['errors'].append(str(e))
            logger.error(f"Batch ingestion failed: {e}")
        
        finally:
            self.db.log_ingestion_end(log_id, self.stats)
        
        return self.stats


def run_daily_operations():
    """Execute complete daily workflow"""
    logger.info("=== Starting Daily Operations ===")
    start_time = datetime.now()
    
    db = RentalIntelDB()
    if not db.connect():
        logger.error("Failed to connect to database")
        return False
    
    try:
        # Step 1: Create partitions
        logger.info("Creating monthly partitions...")
        partitions = db.auto_create_partitions()
        for p in partitions:
            logger.info(f"  {p}")
        
        # Step 2: Mark stale listings
        logger.info("Marking stale listings...")
        stale_count = db.mark_stale_listings(30)
        logger.info(f"  Marked {stale_count} listings inactive")
        
        # Step 3: Calculate ZIP metrics
        logger.info("Calculating ZIP code metrics...")
        zips = db.get_all_zips()
        for zip_code in zips:
            db.calculate_zip_metrics(zip_code)
        logger.info(f"  Processed {len(zips)} ZIP codes")
        
        elapsed = (datetime.now() - start_time).total_seconds()
        logger.info(f"=== Daily Operations Complete ({elapsed:.1f}s) ===")
        return True
        
    except Exception as e:
        logger.error(f"Daily operations failed: {e}")
        return False
        
    finally:
        db.close()


def run_weekly_forecast_update():
    """Execute weekly forecast update"""
    logger.info("=== Starting Weekly Forecast Update ===")
    # TODO: Implement ML model training and prediction
    logger.info("Forecast update not yet implemented")


def schedule_jobs():
    """Schedule automated jobs"""
    logger.info("Setting up scheduled jobs...")
    
    # Daily at 6 AM
    schedule.every().day.at("06:00").do(run_daily_operations)
    
    # Weekly on Sunday at 3 AM
    schedule.every().sunday.at("03:00").do(run_weekly_forecast_update)
    
    logger.info("Jobs scheduled. Running scheduler...")
    
    while True:
        schedule.run_pending()
        time.sleep(60)


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        if sys.argv[1] == "run":
            run_daily_operations()
        elif sys.argv[1] == "schedule":
            schedule_jobs()
    else:
        run_daily_operations()

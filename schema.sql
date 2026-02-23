-- Rental Intelligence Database Schema
-- PostgreSQL 15+ with partitioning support
-- Schema: rental_intel

-- Create schema if not exists
CREATE SCHEMA IF NOT EXISTS rental_intel;

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS btree_gist;

-- ============================================
-- CORE TABLES
-- ============================================

-- Properties table: Master list of properties
CREATE TABLE rental_intel.properties (
    property_id BIGSERIAL PRIMARY KEY,
    normalized_full_address TEXT NOT NULL,
    street_address TEXT NOT NULL,
    city TEXT NOT NULL,
    state TEXT NOT NULL,
    zip TEXT NOT NULL,
    county TEXT,
    latitude DECIMAL(10, 8),
    longitude DECIMAL(11, 8),
    property_type TEXT CHECK (property_type IN ('apartment', 'house', 'condo', 'townhouse', 'studio', 'loft')),
    bedrooms INTEGER,
    bathrooms DECIMAL(4, 2),
    square_feet INTEGER,
    year_built INTEGER,
    address_hash TEXT UNIQUE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Listings table: Active and historical listings
CREATE TABLE rental_intel.listings (
    listing_id BIGSERIAL PRIMARY KEY,
    property_id BIGINT NOT NULL REFERENCES rental_intel.properties(property_id) ON DELETE CASCADE,
    source_platform TEXT NOT NULL,
    source_listing_id TEXT NOT NULL,
    listing_status TEXT DEFAULT 'active' CHECK (listing_status IN ('active', 'inactive', 'removed', 'expired')),
    listing_url TEXT,
    first_seen_date TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    last_verified_date TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(source_platform, source_listing_id)
);

-- Rent price history: Partitioned by month, append-only
CREATE TABLE rental_intel.rent_price_history (
    price_history_id BIGSERIAL,
    listing_id BIGINT NOT NULL REFERENCES rental_intel.listings(listing_id) ON DELETE CASCADE,
    property_id BIGINT NOT NULL REFERENCES rental_intel.properties(property_id) ON DELETE CASCADE,
    observed_rent DECIMAL(10, 2) NOT NULL,
    rent_per_sqft DECIMAL(8, 4),
    change_type TEXT CHECK (change_type IN ('new', 'increase', 'decrease', 'unchanged')),
    observed_date DATE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (price_history_id, observed_date)
) PARTITION BY RANGE (observed_date);

-- Daily ZIP metrics: Aggregated metrics by ZIP code
CREATE TABLE rental_intel.daily_zip_metrics (
    metric_id BIGSERIAL PRIMARY KEY,
    zip TEXT NOT NULL,
    metric_date DATE NOT NULL,
    median_rent DECIMAL(10, 2),
    average_rent DECIMAL(10, 2),
    rent_per_sqft DECIMAL(8, 4),
    active_listing_count INTEGER DEFAULT 0,
    avg_7_day DECIMAL(10, 2),
    avg_30_day DECIMAL(10, 2),
    avg_90_day DECIMAL(10, 2),
    inventory_growth_rate DECIMAL(5, 4),
    price_volatility_index DECIMAL(8, 4),
    property_type_breakdown JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(zip, metric_date)
);

-- Forecast ZIP rent: Future rent predictions
CREATE TABLE rental_intel.forecast_zip_rent (
    forecast_id BIGSERIAL PRIMARY KEY,
    zip TEXT NOT NULL,
    forecast_date DATE NOT NULL,
    target_date DATE NOT NULL,
    projected_median_rent DECIMAL(10, 2),
    projected_high DECIMAL(10, 2),
    projected_low DECIMAL(10, 2),
    confidence_score DECIMAL(5, 4),
    model_version TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(zip, target_date)
);

-- Ingestion log: Track all ingestion runs
CREATE TABLE rental_intel.ingestion_log (
    log_id BIGSERIAL PRIMARY KEY,
    source TEXT NOT NULL,
    run_start TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    run_end TIMESTAMP WITH TIME ZONE,
    records_scanned INTEGER DEFAULT 0,
    records_inserted INTEGER DEFAULT 0,
    records_updated INTEGER DEFAULT 0,
    price_changes INTEGER DEFAULT 0,
    errors JSONB,
    status TEXT DEFAULT 'running' CHECK (status IN ('running', 'completed', 'failed')),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- ============================================
-- INDEXES
-- ============================================

-- Properties indexes
CREATE INDEX idx_properties_zip ON rental_intel.properties(zip);
CREATE INDEX idx_properties_city_state ON rental_intel.properties(city, state);
CREATE INDEX idx_properties_location ON rental_intel.properties USING gist (point(longitude, latitude));
CREATE INDEX idx_properties_address_hash ON rental_intel.properties(address_hash);
CREATE INDEX idx_properties_type ON rental_intel.properties(property_type);

-- Listings indexes
CREATE INDEX idx_listings_property_id ON rental_intel.listings(property_id);
CREATE INDEX idx_listings_status ON rental_intel.listings(listing_status) WHERE listing_status = 'active';
CREATE INDEX idx_listings_platform ON rental_intel.listings(source_platform);
CREATE INDEX idx_listings_verified_date ON rental_intel.listings(last_verified_date);

-- Rent price history indexes (auto-created on partitions)
CREATE INDEX idx_price_history_listing ON rental_intel.rent_price_history(listing_id);
CREATE INDEX idx_price_history_property ON rental_intel.rent_price_history(property_id);
CREATE INDEX idx_price_history_observed ON rental_intel.rent_price_history(observed_date);

-- ZIP metrics indexes
CREATE INDEX idx_zip_metrics_zip_date ON rental_intel.daily_zip_metrics(zip, metric_date);
CREATE INDEX idx_zip_metrics_date ON rental_intel.daily_zip_metrics(metric_date);

-- Forecast indexes
CREATE INDEX idx_forecast_zip_date ON rental_intel.forecast_zip_rent(zip, forecast_date);
CREATE INDEX idx_forecast_target ON rental_intel.forecast_zip_rent(target_date);

-- Ingestion log indexes
CREATE INDEX idx_ingestion_log_source ON rental_intel.ingestion_log(source);
CREATE INDEX idx_ingestion_log_date ON rental_intel.ingestion_log(run_start);

-- ============================================
-- VIEWS FOR REPORTING
-- ============================================

-- View 1: Current active listings
CREATE OR REPLACE VIEW rental_intel.v_active_listings AS
SELECT 
    p.property_id,
    p.normalized_full_address,
    p.street_address,
    p.city,
    p.state,
    p.zip,
    p.property_type,
    p.bedrooms,
    p.bathrooms,
    p.square_feet,
    l.listing_id,
    l.source_platform,
    l.listing_status,
    l.last_verified_date,
    l.source_listing_id
FROM rental_intel.properties p
JOIN rental_intel.listings l ON p.property_id = l.property_id
WHERE l.listing_status = 'active';

-- View 2: Latest rent per listing
CREATE OR REPLACE VIEW rental_intel.v_latest_rent AS
SELECT DISTINCT ON (listing_id)
    listing_id,
    property_id,
    observed_rent,
    rent_per_sqft,
    change_type,
    observed_date
FROM rental_intel.rent_price_history
ORDER BY listing_id, observed_date DESC;

-- View 3: Current market snapshot with latest prices
CREATE OR REPLACE VIEW rental_intel.v_market_snapshot AS
SELECT 
    al.*,
    lr.observed_rent AS current_rent,
    lr.rent_per_sqft AS current_rent_per_sqft,
    lr.observed_date AS last_price_update,
    zd.median_rent AS zip_median_rent,
    zd.active_listing_count AS zip_listing_count
FROM rental_intel.v_active_listings al
LEFT JOIN rental_intel.v_latest_rent lr ON al.listing_id = lr.listing_id
LEFT JOIN rental_intel.daily_zip_metrics zd 
    ON al.zip = zd.zip 
    AND zd.metric_date = CURRENT_DATE;

-- View 4: ZIP code summary
CREATE OR REPLACE VIEW rental_intel.v_zip_summary AS
SELECT 
    zip,
    COUNT(DISTINCT p.property_id) AS total_properties,
    COUNT(DISTINCT CASE WHEN l.listing_status = 'active' THEN l.listing_id END) AS active_listings,
    AVG(latest.observed_rent) AS avg_current_rent,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY latest.observed_rent) AS median_current_rent
FROM rental_intel.properties p
LEFT JOIN rental_intel.listings l ON p.property_id = l.property_id
LEFT JOIN rental_intel.v_latest_rent latest ON l.listing_id = latest.listing_id
GROUP BY zip;

-- View 5: Price change trends
CREATE OR REPLACE VIEW rental_intel.v_price_trends AS
SELECT 
    p.zip,
    p.city,
    p.state,
    rph.observed_date,
    COUNT(*) AS price_changes,
    COUNT(*) FILTER (WHERE rph.change_type = 'increase') AS increases,
    COUNT(*) FILTER (WHERE rph.change_type = 'decrease') AS decreases,
    AVG(rph.observed_rent) AS avg_rent,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY rph.observed_rent) AS median_rent
FROM rental_intel.rent_price_history rph
JOIN rental_intel.properties p ON rph.property_id = p.property_id
GROUP BY p.zip, p.city, p.state, rph.observed_date
ORDER BY rph.observed_date DESC;

-- View 6: Market Snapshot by State/City/ZIP (user requested)
CREATE OR REPLACE VIEW rental_intel.v_market_snapshot AS
SELECT 
    p.state,
    p.city,
    p.zip,
    COUNT(l.listing_id) AS active_listings,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY r.observed_rent) AS median_rent,
    AVG(r.observed_rent) AS avg_rent
FROM rental_intel.properties p
JOIN rental_intel.listings l ON p.property_id = l.property_id
JOIN rental_intel.v_latest_rent r ON l.listing_id = r.listing_id
WHERE l.listing_status = 'active'
GROUP BY p.state, p.city, p.zip;

-- View 7: ZIP Trend 30-Day (user requested)
CREATE OR REPLACE VIEW rental_intel.v_zip_trend_30_day AS
SELECT 
    zip,
    metric_date,
    median_rent,
    LAG(median_rent, 30) OVER (PARTITION BY zip ORDER BY metric_date) AS rent_30_days_ago,
    median_rent - LAG(median_rent, 30) OVER (PARTITION BY zip ORDER BY metric_date) AS rent_change_30_day
FROM rental_intel.daily_zip_metrics;

-- ============================================
-- FUNCTIONS
-- ============================================

-- Function to normalize address
CREATE OR REPLACE FUNCTION rental_intel.normalize_address(
    street TEXT,
    city TEXT,
    state TEXT,
    zip TEXT
) RETURNS TEXT AS $$
BEGIN
    RETURN UPPER(TRIM(REGEXP_REPLACE(
        COALESCE(street, '') || ' ' || 
        COALESCE(city, '') || ' ' || 
        COALESCE(state, '') || ' ' || 
        COALESCE(zip, ''),
        '\s+', ' ', 'g'
    )));
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- Function to generate address hash
CREATE OR REPLACE FUNCTION rental_intel.generate_address_hash()
RETURNS TRIGGER AS $$
BEGIN
    NEW.normalized_full_address := rental_intel.normalize_address(
        NEW.street_address, NEW.city, NEW.state, NEW.zip
    );
    NEW.address_hash := ENCODE(DIGEST(NEW.normalized_full_address, 'sha256'), 'hex');
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger for address normalization
CREATE TRIGGER trg_normalize_address
    BEFORE INSERT OR UPDATE ON rental_intel.properties
    FOR EACH ROW
    EXECUTE FUNCTION rental_intel.generate_address_hash();

-- Function to upsert property
CREATE OR REPLACE FUNCTION rental_intel.upsert_property(
    p_street TEXT,
    p_city TEXT,
    p_state TEXT,
    p_zip TEXT,
    p_type TEXT DEFAULT NULL,
    p_bedrooms INTEGER DEFAULT NULL,
    p_bathrooms DECIMAL DEFAULT NULL,
    p_sqft INTEGER DEFAULT NULL
) RETURNS BIGINT AS $$
DECLARE
    v_property_id BIGINT;
    v_normalized TEXT;
    v_hash TEXT;
BEGIN
    v_normalized := rental_intel.normalize_address(p_street, p_city, p_state, p_zip);
    v_hash := ENCODE(DIGEST(v_normalized, 'sha256'), 'hex');
    
    INSERT INTO rental_intel.properties (
        street_address, city, state, zip, 
        normalized_full_address, address_hash,
        property_type, bedrooms, bathrooms, square_feet
    ) VALUES (
        p_street, p_city, p_state, p_zip,
        v_normalized, v_hash,
        p_type, p_bedrooms, p_bathrooms, p_sqft
    )
    ON CONFLICT (address_hash) DO UPDATE SET
        property_type = COALESCE(EXCLUDED.property_type, rental_intel.properties.property_type),
        bedrooms = COALESCE(EXCLUDED.bedrooms, rental_intel.properties.bedrooms),
        bathrooms = COALESCE(EXCLUDED.bathrooms, rental_intel.properties.bathrooms),
        square_feet = COALESCE(EXCLUDED.square_feet, rental_intel.properties.square_feet),
        updated_at = CURRENT_TIMESTAMP
    RETURNING property_id INTO v_property_id;
    
    RETURN v_property_id;
END;
$$ LANGUAGE plpgsql;

-- Function to record price change
CREATE OR REPLACE FUNCTION rental_intel.record_price_change(
    p_listing_id BIGINT,
    p_new_rent DECIMAL
) RETURNS VOID AS $$
DECLARE
    v_last_rent DECIMAL;
    v_change_type TEXT;
    v_property_id BIGINT;
BEGIN
    SELECT property_id INTO v_property_id
    FROM rental_intel.listings WHERE listing_id = p_listing_id;
    
    SELECT observed_rent INTO v_last_rent
    FROM rental_intel.rent_price_history
    WHERE listing_id = p_listing_id
    ORDER BY observed_date DESC
    LIMIT 1;
    
    IF v_last_rent IS NULL THEN
        v_change_type := 'new';
    ELSIF p_new_rent > v_last_rent THEN
        v_change_type := 'increase';
    ELSIF p_new_rent < v_last_rent THEN
        v_change_type := 'decrease';
    ELSE
        v_change_type := 'unchanged';
    END IF;
    
    IF v_change_type != 'unchanged' THEN
        INSERT INTO rental_intel.rent_price_history (
            listing_id, property_id, observed_rent, change_type, observed_date
        ) VALUES (
            p_listing_id, v_property_id, p_new_rent, v_change_type, CURRENT_DATE
        );
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Function to calculate daily ZIP metrics
CREATE OR REPLACE FUNCTION rental_intel.calculate_zip_metrics(
    p_zip TEXT,
    p_date DATE DEFAULT CURRENT_DATE
) RETURNS VOID AS $$
DECLARE
    v_median_rent DECIMAL;
    v_avg_rent DECIMAL;
    v_active_count INTEGER;
    v_avg_rent_psf DECIMAL;
    v_avg_7 DECIMAL;
    v_avg_30 DECIMAL;
    v_avg_90 DECIMAL;
BEGIN
    SELECT 
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY lr.observed_rent),
        AVG(lr.observed_rent),
        COUNT(*),
        AVG(lr.rent_per_sqft)
    INTO v_median_rent, v_avg_rent, v_active_count, v_avg_rent_psf
    FROM rental_intel.v_active_listings al
    JOIN rental_intel.v_latest_rent lr ON al.listing_id = lr.listing_id
    WHERE al.zip = p_zip;
    
    SELECT AVG(observed_rent) INTO v_avg_7
    FROM rental_intel.rent_price_history rph
    JOIN rental_intel.listings l ON rph.listing_id = l.listing_id
    JOIN rental_intel.properties p ON rph.property_id = p.property_id
    WHERE p.zip = p_zip AND rph.observed_date >= p_date - 7;
    
    SELECT AVG(observed_rent) INTO v_avg_30
    FROM rental_intel.rent_price_history rph
    JOIN rental_intel.listings l ON rph.listing_id = l.listing_id
    JOIN rental_intel.properties p ON rph.property_id = p.property_id
    WHERE p.zip = p_zip AND rph.observed_date >= p_date - 30;
    
    SELECT AVG(observed_rent) INTO v_avg_90
    FROM rental_intel.rent_price_history rph
    JOIN rental_intel.listings l ON rph.listing_id = l.listing_id
    JOIN rental_intel.properties p ON rph.property_id = p.property_id
    WHERE p.zip = p_zip AND rph.observed_date >= p_date - 90;
    
    INSERT INTO rental_intel.daily_zip_metrics (
        zip, metric_date, median_rent, average_rent, rent_per_sqft,
        active_listing_count, avg_7_day, avg_30_day, avg_90_day
    ) VALUES (
        p_zip, p_date, v_median_rent, v_avg_rent, v_avg_rent_psf,
        v_active_count, v_avg_7, v_avg_30, v_avg_90
    )
    ON CONFLICT (zip, metric_date) DO UPDATE SET
        median_rent = EXCLUDED.median_rent,
        average_rent = EXCLUDED.average_rent,
        rent_per_sqft = EXCLUDED.rent_per_sqft,
        active_listing_count = EXCLUDED.active_listing_count,
        avg_7_day = EXCLUDED.avg_7_day,
        avg_30_day = EXCLUDED.avg_30_day,
        avg_90_day = EXCLUDED.avg_90_day,
        updated_at = CURRENT_TIMESTAMP;
END;
$$ LANGUAGE plpgsql;

-- ============================================
-- PARTITION MANAGEMENT
-- ============================================

-- Function to create monthly partition
CREATE OR REPLACE FUNCTION rental_intel.create_monthly_partition(
    p_year INTEGER,
    p_month INTEGER
) RETURNS TEXT AS $$
DECLARE
    v_partition_name TEXT;
    v_start_date DATE;
    v_end_date DATE;
    v_sql TEXT;
BEGIN
    v_partition_name := 'rent_price_history_' || p_year || '_' || LPAD(p_month::TEXT, 2, '0');
    v_start_date := MAKE_DATE(p_year, p_month, 1);
    v_end_date := v_start_date + INTERVAL '1 month';
    
    IF EXISTS (SELECT 1 FROM pg_tables WHERE tablename = v_partition_name) THEN
        RETURN 'Partition ' || v_partition_name || ' already exists';
    END IF;
    
    v_sql := 'CREATE TABLE rental_intel.' || v_partition_name || 
             ' PARTITION OF rental_intel.rent_price_history ' ||
             'FOR VALUES FROM (''' || v_start_date || ''') TO (''' || v_end_date || ''')';
    
    EXECUTE v_sql;
    
    RETURN 'Created partition: ' || v_partition_name;
END;
$$ LANGUAGE plpgsql;

-- Function to auto-create next 3 months of partitions
CREATE OR REPLACE FUNCTION rental_intel.auto_create_partitions()
RETURNS TABLE(month_created TEXT) AS $$
DECLARE
    v_current_date DATE := CURRENT_DATE;
    v_i INTEGER;
    v_year INTEGER;
    v_month INTEGER;
    v_result TEXT;
BEGIN
    FOR v_i IN 0..3 LOOP
        v_year := EXTRACT(YEAR FROM v_current_date + (v_i || ' months')::INTERVAL)::INTEGER;
        v_month := EXTRACT(MONTH FROM v_current_date + (v_i || ' months')::INTERVAL)::INTEGER;
        v_result := rental_intel.create_monthly_partition(v_year, v_month);
        month_created := v_result;
        RETURN NEXT;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- Create initial partitions
SELECT rental_intel.create_monthly_partition(EXTRACT(YEAR FROM CURRENT_DATE)::INTEGER, EXTRACT(MONTH FROM CURRENT_DATE)::INTEGER);
SELECT rental_intel.create_monthly_partition(EXTRACT(YEAR FROM CURRENT_DATE + INTERVAL '1 month')::INTEGER, EXTRACT(MONTH FROM CURRENT_DATE + INTERVAL '1 month')::INTEGER);

-- ============================================
-- COMMENTS
-- ============================================

COMMENT ON SCHEMA rental_intel IS 'Rental Intelligence System for market analysis and forecasting';
COMMENT ON TABLE rental_intel.properties IS 'Master property records with normalized addresses';
COMMENT ON TABLE rental_intel.listings IS 'Active and historical rental listings';
COMMENT ON TABLE rental_intel.rent_price_history IS 'Append-only price history, partitioned by month';
COMMENT ON TABLE rental_intel.daily_zip_metrics IS 'Daily ZIP code level market metrics';
COMMENT ON TABLE rental_intel.forecast_zip_rent IS 'Forecasted rent predictions by ZIP code';
COMMENT ON TABLE rental_intel.ingestion_log IS 'Audit log for all data ingestion runs';

-- Done
SELECT 'Schema created successfully' AS status;

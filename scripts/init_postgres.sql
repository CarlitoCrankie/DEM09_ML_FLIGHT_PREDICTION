-- PostgreSQL Analytics Initialization Script
-- Medallion Architecture: Bronze, Silver, Gold
-- Updated for Incremental Loading Support

-- ============================================
-- Create Schemas
-- ============================================

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS audit;

-- ============================================
-- Drop existing tables (for clean setup)
-- ============================================

DROP TABLE IF EXISTS bronze.data_load_metadata CASCADE;
DROP TABLE IF EXISTS bronze.validated_flights CASCADE;
DROP TABLE IF EXISTS audit.pipeline_runs CASCADE;

-- ============================================
-- Bronze Layer - Main Data Table
-- ============================================

CREATE TABLE bronze.validated_flights (
    -- Primary key
    id SERIAL PRIMARY KEY,
    
    -- Flight details
    airline VARCHAR(100),
    source_code VARCHAR(10),
    source_name VARCHAR(255),
    destination_code VARCHAR(10),
    destination_name VARCHAR(255),
    departure_datetime TIMESTAMP,
    arrival_datetime TIMESTAMP,
    duration_hrs NUMERIC(6, 2),
    stopovers VARCHAR(50),
    aircraft_type VARCHAR(100),
    travel_class VARCHAR(50),
    booking_source VARCHAR(100),
    
    -- Fare information
    base_fare_bdt NUMERIC(12, 2),
    tax_surcharge_bdt NUMERIC(12, 2),
    total_fare_bdt NUMERIC(12, 2),
    
    -- Additional fields
    seasonality VARCHAR(50),
    days_before_departure INT,
    
    -- Validation
    is_valid BOOLEAN,
    validation_errors TEXT,
    
    -- Lineage (from MySQL)
    mysql_raw_id INT,
    mysql_validated_id INT,
    mysql_loaded_at TIMESTAMP,
    metadata_id INT,
    
    -- Bronze layer timestamp
    bronze_loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- ==========================================
    -- INCREMENTAL LOADING COLUMNS (NEW)
    -- ==========================================
    
    -- Change detection hash (MD5)
    record_hash VARCHAR(64),
    
    -- Data source tracking
    data_source_id INTEGER,
    
    -- Ingestion timestamp (for this specific load)
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Active record flag (for soft deletes)
    is_active BOOLEAN DEFAULT TRUE
);

-- ============================================
-- Incremental Loading Metadata Table (NEW)
-- ============================================

CREATE TABLE bronze.data_load_metadata (
    -- Primary key
    load_id SERIAL PRIMARY KEY,
    
    -- Load timing
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Source information
    source_file VARCHAR(255),
    kaggle_dataset_version VARCHAR(50),
    schema_hash VARCHAR(64),
    
    -- Load statistics
    records_inserted INTEGER DEFAULT 0,
    records_updated INTEGER DEFAULT 0,
    records_deleted INTEGER DEFAULT 0,
    records_unchanged INTEGER DEFAULT 0,
    
    -- Load type
    load_type VARCHAR(20),  -- 'FULL' or 'INCREMENTAL'
    
    -- Change metrics
    change_percentage DECIMAL(5,2),
    
    -- Performance
    execution_time_seconds INTEGER
);

-- ============================================
-- Audit Table
-- ============================================

CREATE TABLE audit.pipeline_runs (
    id SERIAL PRIMARY KEY,
    dag_id VARCHAR(255),
    dag_run_id VARCHAR(255),
    task_id VARCHAR(255),
    status VARCHAR(50),
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    rows_processed INT,
    rows_failed INT,
    error_message TEXT,
    metadata JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================
-- Indexes for Bronze Layer
-- ============================================

-- Business query indexes
CREATE INDEX idx_bronze_airline ON bronze.validated_flights(airline);
CREATE INDEX idx_bronze_route ON bronze.validated_flights(source_code, destination_code);
CREATE INDEX idx_bronze_valid ON bronze.validated_flights(is_valid);
CREATE INDEX idx_bronze_metadata ON bronze.validated_flights(metadata_id);

-- Incremental loading indexes (NEW)
CREATE INDEX idx_record_hash ON bronze.validated_flights(record_hash);
CREATE INDEX idx_ingestion_timestamp ON bronze.validated_flights(ingestion_timestamp);
CREATE INDEX idx_is_active ON bronze.validated_flights(is_active);

-- Composite index for incremental queries (NEW)
CREATE INDEX idx_active_hash ON bronze.validated_flights(is_active, record_hash) 
    WHERE is_active = TRUE;

-- ============================================
-- Indexes for Metadata Table (NEW)
-- ============================================

CREATE INDEX idx_load_timestamp ON bronze.data_load_metadata(load_timestamp);
CREATE INDEX idx_load_type ON bronze.data_load_metadata(load_type);

-- ============================================
-- Indexes for Audit Table
-- ============================================

CREATE INDEX idx_audit_dag ON audit.pipeline_runs(dag_id);
CREATE INDEX idx_audit_task ON audit.pipeline_runs(task_id);
CREATE INDEX idx_audit_status ON audit.pipeline_runs(status);

-- ============================================
-- Comments for Documentation
-- ============================================

COMMENT ON TABLE bronze.validated_flights IS 'Bronze layer validated flight data with incremental loading support';
COMMENT ON COLUMN bronze.validated_flights.record_hash IS 'MD5 hash for change detection based on business key fields';
COMMENT ON COLUMN bronze.validated_flights.is_active IS 'TRUE for current records, FALSE for soft-deleted records';
COMMENT ON COLUMN bronze.validated_flights.ingestion_timestamp IS 'Timestamp when this record was loaded into bronze layer';

COMMENT ON TABLE bronze.data_load_metadata IS 'Tracks incremental load statistics and performance metrics';
COMMENT ON COLUMN bronze.data_load_metadata.load_type IS 'FULL = truncate+reload, INCREMENTAL = change-based';
COMMENT ON COLUMN bronze.data_load_metadata.change_percentage IS 'Percentage of records that changed (new + deleted)';

-- ============================================
-- Success Message
-- ============================================

DO $$
BEGIN
    RAISE NOTICE '======================================================';
    RAISE NOTICE 'PostgreSQL initialization completed successfully!';
    RAISE NOTICE '======================================================';
    RAISE NOTICE 'Schemas created: bronze, silver, gold, audit';
    RAISE NOTICE 'Tables created:';
    RAISE NOTICE '  - bronze.validated_flights (with incremental support)';
    RAISE NOTICE '  - bronze.data_load_metadata (NEW)';
    RAISE NOTICE '  - audit.pipeline_runs';
    RAISE NOTICE '======================================================';
    RAISE NOTICE 'Incremental loading features enabled:';
    RAISE NOTICE '  ✓ Record hashing for change detection';
    RAISE NOTICE '  ✓ Soft deletes (is_active flag)';
    RAISE NOTICE '  ✓ Load metadata tracking';
    RAISE NOTICE '  ✓ Performance indexes';
    RAISE NOTICE '======================================================';
END $$;
-- ============================================
-- ML TRACKING SCHEMA
-- ============================================

CREATE SCHEMA IF NOT EXISTS ml_tracking;

-- Model training history
CREATE TABLE IF NOT EXISTS ml_tracking.model_training_history (
    id SERIAL PRIMARY KEY,
    training_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    model_name VARCHAR(100),
    model_type VARCHAR(100),
    training_records INTEGER,
    test_records INTEGER,
    features_used INTEGER,
    test_r2 DECIMAL(10, 6),
    test_mae DECIMAL(10, 2),
    test_rmse DECIMAL(10, 2),
    cv_r2_mean DECIMAL(10, 6),
    cv_r2_std DECIMAL(10, 6),
    model_path TEXT,
    is_production BOOLEAN DEFAULT FALSE,
    metadata JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_training_history_timestamp 
ON ml_tracking.model_training_history(training_timestamp);

CREATE INDEX IF NOT EXISTS idx_training_history_production 
ON ml_tracking.model_training_history(is_production);

-- Permissions
GRANT ALL PRIVILEGES ON SCHEMA ml_tracking TO analytics_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA ml_tracking TO analytics_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA ml_tracking TO analytics_user;

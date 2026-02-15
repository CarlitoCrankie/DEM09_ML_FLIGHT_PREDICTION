-- -- MySQL Initialization Script
-- -- Flight Price Dataset of Bangladesh

-- USE flight_staging;

-- DROP TABLE IF EXISTS validated_flight_data;
-- DROP TABLE IF EXISTS raw_flight_data;

-- CREATE TABLE raw_flight_data (
--     id INT AUTO_INCREMENT PRIMARY KEY,
--     airline VARCHAR(100) NOT NULL,
--     source_code VARCHAR(10) NOT NULL,
--     source_name VARCHAR(255),
--     destination_code VARCHAR(10) NOT NULL,
--     destination_name VARCHAR(255),
--     departure_datetime DATETIME,
--     arrival_datetime DATETIME,
--     duration_hrs DECIMAL(6, 2),
--     stopovers VARCHAR(50),
--     aircraft_type VARCHAR(100),
--     travel_class VARCHAR(50),
--     booking_source VARCHAR(100),
--     base_fare_bdt DECIMAL(12, 2),
--     tax_surcharge_bdt DECIMAL(12, 2),
--     total_fare_bdt DECIMAL(12, 2),
--     seasonality VARCHAR(50),
--     days_before_departure INT,
--     loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
--     source_file VARCHAR(255)
-- );

-- CREATE TABLE validated_flight_data (
--     id INT AUTO_INCREMENT PRIMARY KEY,
--     airline VARCHAR(100) NOT NULL,
--     source_code VARCHAR(10) NOT NULL,
--     source_name VARCHAR(255),
--     destination_code VARCHAR(10) NOT NULL,
--     destination_name VARCHAR(255),
--     departure_datetime DATETIME,
--     arrival_datetime DATETIME,
--     duration_hrs DECIMAL(6, 2),
--     stopovers VARCHAR(50),
--     aircraft_type VARCHAR(100),
--     travel_class VARCHAR(50),
--     booking_source VARCHAR(100),
--     base_fare_bdt DECIMAL(12, 2),
--     tax_surcharge_bdt DECIMAL(12, 2),
--     total_fare_bdt DECIMAL(12, 2),
--     seasonality VARCHAR(50),
--     days_before_departure INT,
--     is_valid BOOLEAN DEFAULT TRUE,
--     validation_errors TEXT,
--     raw_id INT NOT NULL,
--     validated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
--     FOREIGN KEY (raw_id) REFERENCES raw_flight_data(id)
-- );

-- CREATE INDEX idx_raw_airline ON raw_flight_data(airline);
-- CREATE INDEX idx_raw_source ON raw_flight_data(source_code);
-- CREATE INDEX idx_raw_destination ON raw_flight_data(destination_code);
-- CREATE INDEX idx_validated_airline ON validated_flight_data(airline);
-- CREATE INDEX idx_validated_route ON validated_flight_data(source_code, destination_code);
-- CREATE INDEX idx_validated_valid ON validated_flight_data(is_valid);
-- 

-- MySQL Initialization Script
-- Flight Price Dataset of Bangladesh

USE flight_staging;

DROP TABLE IF EXISTS validated_flight_data;
DROP TABLE IF EXISTS raw_flight_data;
DROP TABLE IF EXISTS dataset_metadata;

-- Metadata tracking table
CREATE TABLE dataset_metadata (
    id INT AUTO_INCREMENT PRIMARY KEY,
    dataset_name VARCHAR(255) NOT NULL,
    kaggle_dataset_id VARCHAR(255) NOT NULL,
    version VARCHAR(50),
    download_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    file_name VARCHAR(255),
    file_size_bytes BIGINT,
    row_count INT,
    column_count INT,
    columns_json JSON,
    schema_hash VARCHAR(64),
    schema_changed BOOLEAN DEFAULT FALSE,
    new_columns_added JSON,
    previous_metadata_id INT,
    FOREIGN KEY (previous_metadata_id) REFERENCES dataset_metadata(id)
);

CREATE TABLE raw_flight_data (
    id INT AUTO_INCREMENT PRIMARY KEY,
    airline VARCHAR(100),
    source_code VARCHAR(10),
    source_name VARCHAR(255),
    destination_code VARCHAR(10),
    destination_name VARCHAR(255),
    departure_datetime DATETIME,
    arrival_datetime DATETIME,
    duration_hrs DECIMAL(6, 2),
    stopovers VARCHAR(50),
    aircraft_type VARCHAR(100),
    travel_class VARCHAR(50),
    booking_source VARCHAR(100),
    base_fare_bdt DECIMAL(12, 2),
    tax_surcharge_bdt DECIMAL(12, 2),
    total_fare_bdt DECIMAL(12, 2),
    seasonality VARCHAR(50),
    days_before_departure INT,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_file VARCHAR(255),
    metadata_id INT,
    FOREIGN KEY (metadata_id) REFERENCES dataset_metadata(id)
);

CREATE TABLE validated_flight_data (
    id INT AUTO_INCREMENT PRIMARY KEY,
    airline VARCHAR(100),
    source_code VARCHAR(10),
    source_name VARCHAR(255),
    destination_code VARCHAR(10),
    destination_name VARCHAR(255),
    departure_datetime DATETIME,
    arrival_datetime DATETIME,
    duration_hrs DECIMAL(6, 2),
    stopovers VARCHAR(50),
    aircraft_type VARCHAR(100),
    travel_class VARCHAR(50),
    booking_source VARCHAR(100),
    base_fare_bdt DECIMAL(12, 2),
    tax_surcharge_bdt DECIMAL(12, 2),
    total_fare_bdt DECIMAL(12, 2),
    seasonality VARCHAR(50),
    days_before_departure INT,
    is_valid BOOLEAN DEFAULT TRUE,
    validation_errors TEXT,
    raw_id INT NOT NULL,
    validated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (raw_id) REFERENCES raw_flight_data(id)
);

CREATE INDEX idx_raw_airline ON raw_flight_data(airline);
CREATE INDEX idx_raw_source ON raw_flight_data(source_code);
CREATE INDEX idx_raw_destination ON raw_flight_data(destination_code);
CREATE INDEX idx_validated_airline ON validated_flight_data(airline);
CREATE INDEX idx_validated_route ON validated_flight_data(source_code, destination_code);
CREATE INDEX idx_validated_valid ON validated_flight_data(is_valid);
CREATE INDEX idx_metadata_dataset ON dataset_metadata(kaggle_dataset_id);
CREATE INDEX idx_metadata_hash ON dataset_metadata(schema_hash);

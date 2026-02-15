{{
    config(
        materialized='view',
        schema='gold'
    )
}}

/*
    Gold Layer: Fare History Analysis
    
    Provides easy access to historical fare changes from SCD Type 2 snapshot.
    Use this to analyze how fares changed over time.
*/

SELECT
    fare_key,
    airline,
    route,
    source_code,
    destination_code,
    travel_class,
    seasonality,
    route_type,
    
    -- Metrics
    booking_count,
    avg_base_fare,
    avg_total_fare,
    min_fare,
    max_fare,
    
    -- SCD columns (renamed for clarity)
    dbt_valid_from AS valid_from,
    dbt_valid_to AS valid_to,
    
    -- Is this the current record?
    CASE 
        WHEN dbt_valid_to IS NULL THEN TRUE 
        ELSE FALSE 
    END AS is_current,
    
    -- Record status
    CASE 
        WHEN dbt_valid_to IS NULL THEN 'Current'
        ELSE 'Historical'
    END AS record_status,
    
    -- Days this version was active
    CASE 
        WHEN dbt_valid_to IS NULL THEN 
            EXTRACT(DAY FROM CURRENT_TIMESTAMP - dbt_valid_from)::INT
        ELSE 
            EXTRACT(DAY FROM dbt_valid_to - dbt_valid_from)::INT
    END AS days_active,
    
    -- Version number for each fare_key
    ROW_NUMBER() OVER (
        PARTITION BY fare_key 
        ORDER BY dbt_valid_from
    ) AS version_number,
    
    -- Total versions for this fare_key
    COUNT(*) OVER (
        PARTITION BY fare_key
    ) AS total_versions

FROM {{ ref('flight_fare_snapshot') }}

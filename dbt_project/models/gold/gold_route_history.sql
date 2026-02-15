{{
    config(
        materialized='view',
        schema='gold'
    )
}}

/*
    Gold Layer: Route History Analysis
    
    Provides easy access to historical route metrics from SCD Type 2 snapshot.
*/

SELECT
    route_key,
    route,
    source_code,
    source_name,
    destination_code,
    destination_name,
    route_type,
    
    -- Metrics
    total_bookings,
    avg_fare,
    min_fare,
    max_fare,
    avg_duration,
    direct_flight_pct,
    
    -- Class breakdown
    economy_bookings,
    business_bookings,
    first_bookings,
    
    -- SCD columns
    dbt_valid_from AS valid_from,
    dbt_valid_to AS valid_to,
    
    CASE 
        WHEN dbt_valid_to IS NULL THEN TRUE 
        ELSE FALSE 
    END AS is_current,
    
    CASE 
        WHEN dbt_valid_to IS NULL THEN 'Current'
        ELSE 'Historical'
    END AS record_status,
    
    ROW_NUMBER() OVER (
        PARTITION BY route_key 
        ORDER BY dbt_valid_from
    ) AS version_number

FROM {{ ref('route_fare_snapshot') }}

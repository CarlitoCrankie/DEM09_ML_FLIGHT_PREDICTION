{% snapshot flight_fare_snapshot %}

{{
    config(
        target_schema='silver',
        unique_key='fare_key',
        strategy='check',
        check_cols=['avg_base_fare', 'avg_total_fare', 'booking_count'],
        invalidate_hard_deletes=True
    )
}}

/*
    SCD Type 2 Snapshot: Flight Fare Tracking
    
    Tracks changes in average fares by:
    - Airline
    - Route
    - Travel Class
    - Seasonality
    
    When any of the check_cols change, a new record is created
    and the old record is closed (valid_to is set).
*/

WITH fare_summary AS (
    SELECT
        -- Create a unique key for each combination
        airline || '|' || route || '|' || travel_class || '|' || seasonality AS fare_key,
        
        -- Dimensions
        airline,
        route,
        source_code,
        destination_code,
        travel_class,
        seasonality,
        route_type,
        
        -- Metrics (these are tracked for changes)
        COUNT(*) AS booking_count,
        ROUND(AVG(base_fare_bdt)::numeric, 2) AS avg_base_fare,
        ROUND(AVG(total_fare_bdt)::numeric, 2) AS avg_total_fare,
        ROUND(MIN(total_fare_bdt)::numeric, 2) AS min_fare,
        ROUND(MAX(total_fare_bdt)::numeric, 2) AS max_fare,
        ROUND(AVG(duration_hrs)::numeric, 2) AS avg_duration_hrs,
        
        -- Metadata
        CURRENT_TIMESTAMP AS snapshot_timestamp
        
    FROM {{ ref('silver_cleaned_flights') }}
    GROUP BY
        airline,
        route,
        source_code,
        destination_code,
        travel_class,
        seasonality,
        route_type
)

SELECT * FROM fare_summary

{% endsnapshot %}

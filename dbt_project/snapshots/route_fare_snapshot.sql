{% snapshot route_fare_snapshot %}

{{
    config(
        target_schema='silver',
        unique_key='route_key',
        strategy='check',
        check_cols=['avg_fare', 'total_bookings', 'direct_flight_pct']
    )
}}

/*
    SCD Type 2 Snapshot: Route Fare Tracking
    
    Tracks changes in route-level metrics over time.
*/

WITH route_summary AS (
    SELECT
        -- Unique key
        route || '|' || route_type AS route_key,
        
        -- Dimensions
        route,
        source_code,
        source_name,
        destination_code,
        destination_name,
        route_type,
        
        -- Metrics
        COUNT(*) AS total_bookings,
        ROUND(AVG(total_fare_bdt)::numeric, 2) AS avg_fare,
        ROUND(MIN(total_fare_bdt)::numeric, 2) AS min_fare,
        ROUND(MAX(total_fare_bdt)::numeric, 2) AS max_fare,
        ROUND(AVG(duration_hrs)::numeric, 2) AS avg_duration,
        
        -- Calculated metrics
        ROUND(
            (COUNT(*) FILTER (WHERE num_stops = 0)::numeric / NULLIF(COUNT(*), 0)) * 100, 
            2
        ) AS direct_flight_pct,
        
        -- Class distribution
        COUNT(*) FILTER (WHERE travel_class = 'Economy') AS economy_bookings,
        COUNT(*) FILTER (WHERE travel_class = 'Business') AS business_bookings,
        COUNT(*) FILTER (WHERE travel_class = 'First') AS first_bookings,
        
        -- Metadata
        CURRENT_TIMESTAMP AS snapshot_timestamp
        
    FROM {{ ref('silver_cleaned_flights') }}
    GROUP BY
        route,
        source_code,
        source_name,
        destination_code,
        destination_name,
        route_type
)

SELECT * FROM route_summary

{% endsnapshot %}

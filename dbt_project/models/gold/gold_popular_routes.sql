{{
    config(
        materialized='table',
        schema='gold'
    )
}}

/*
    Gold Layer: Popular Routes
    
    Uses is_peak_season from silver for flexible analysis.
*/

SELECT
    route,
    source_code,
    MAX(source_name) AS source_name,
    destination_code,
    MAX(destination_name) AS destination_name,
    route_type,
    
    -- Booking counts
    COUNT(*) AS total_bookings,
    
    -- Fare metrics
    ROUND(AVG(total_fare_bdt)::numeric, 2) AS avg_fare,
    ROUND(MIN(total_fare_bdt)::numeric, 2) AS min_fare,
    ROUND(MAX(total_fare_bdt)::numeric, 2) AS max_fare,
    ROUND((PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY total_fare_bdt))::numeric, 2) AS median_fare,
    
    -- Duration
    ROUND(AVG(duration_hrs)::numeric, 2) AS avg_duration_hrs,
    
    -- Stops distribution
    COUNT(*) FILTER (WHERE num_stops = 0) AS direct_flights,
    COUNT(*) FILTER (WHERE num_stops = 1) AS one_stop_flights,
    COUNT(*) FILTER (WHERE num_stops = 2) AS two_stop_flights,
    
    -- Class distribution (captures all classes)
    COUNT(*) FILTER (WHERE travel_class = 'Economy') AS economy_bookings,
    COUNT(*) FILTER (WHERE travel_class = 'Business') AS business_bookings,
    COUNT(*) FILTER (WHERE travel_class = 'First') AS first_class_bookings,
    COUNT(*) FILTER (WHERE travel_class NOT IN ('Economy', 'Business', 'First')) AS other_class_bookings,
    
    -- Peak vs off-peak (flexible)
    COUNT(*) FILTER (WHERE is_peak_season = TRUE) AS peak_season_bookings,
    COUNT(*) FILTER (WHERE is_peak_season = FALSE) AS off_peak_bookings,
    ROUND(AVG(total_fare_bdt) FILTER (WHERE is_peak_season = TRUE)::numeric, 2) AS avg_peak_fare,
    ROUND(AVG(total_fare_bdt) FILTER (WHERE is_peak_season = FALSE)::numeric, 2) AS avg_off_peak_fare,
    
    -- Peak premium
    ROUND(
        ((AVG(total_fare_bdt) FILTER (WHERE is_peak_season = TRUE) - 
          AVG(total_fare_bdt) FILTER (WHERE is_peak_season = FALSE)) /
         NULLIF(AVG(total_fare_bdt) FILTER (WHERE is_peak_season = FALSE), 0)) * 100,
        2
    ) AS peak_premium_pct,
    
    -- Top airline on this route
    MODE() WITHIN GROUP (ORDER BY airline) AS most_common_airline,
    
    -- Route popularity rank
    RANK() OVER (ORDER BY COUNT(*) DESC) AS popularity_rank,
    
    CURRENT_TIMESTAMP AS generated_at
    
FROM {{ ref('silver_cleaned_flights') }}
GROUP BY 
    route, 
    source_code, 
    destination_code,
    route_type
ORDER BY total_bookings DESC

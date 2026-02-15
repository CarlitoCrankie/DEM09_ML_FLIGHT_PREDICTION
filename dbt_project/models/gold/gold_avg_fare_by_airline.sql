{{
    config(
        materialized='table',
        schema='gold'
    )
}}

/*
    Gold Layer: Average Fare by Airline
    
    Uses is_peak_season from silver for flexible peak/off-peak analysis.
*/

WITH airline_metrics AS (
    SELECT
        airline,
        COUNT(*) AS total_bookings,
        ROUND(AVG(base_fare_bdt)::numeric, 2) AS avg_base_fare,
        ROUND(AVG(tax_surcharge_bdt)::numeric, 2) AS avg_tax_surcharge,
        ROUND(AVG(total_fare_bdt)::numeric, 2) AS avg_total_fare,
        ROUND(MIN(total_fare_bdt)::numeric, 2) AS min_fare,
        ROUND(MAX(total_fare_bdt)::numeric, 2) AS max_fare,
        ROUND(STDDEV(total_fare_bdt)::numeric, 2) AS fare_std_dev,
        ROUND((PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY total_fare_bdt))::numeric, 2) AS median_fare,
        ROUND((PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY total_fare_bdt))::numeric, 2) AS fare_25th_percentile,
        ROUND((PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY total_fare_bdt))::numeric, 2) AS fare_75th_percentile,
        
        -- By class (handles any class)
        COUNT(*) FILTER (WHERE travel_class = 'Economy') AS economy_bookings,
        COUNT(*) FILTER (WHERE travel_class = 'Business') AS business_bookings,
        COUNT(*) FILTER (WHERE travel_class = 'First') AS first_class_bookings,
        COUNT(*) FILTER (WHERE travel_class = 'Premium Economy') AS premium_economy_bookings,
        COUNT(*) FILTER (WHERE travel_class NOT IN ('Economy', 'Business', 'First', 'Premium Economy')) AS other_class_bookings,
        
        -- By route type
        COUNT(*) FILTER (WHERE route_type = 'Domestic') AS domestic_bookings,
        COUNT(*) FILTER (WHERE route_type = 'International') AS international_bookings,
        
        -- By peak season (flexible)
        COUNT(*) FILTER (WHERE is_peak_season = TRUE) AS peak_season_bookings,
        COUNT(*) FILTER (WHERE is_peak_season = FALSE) AS off_peak_bookings,
        
        -- Fare by peak vs off-peak
        ROUND(AVG(total_fare_bdt) FILTER (WHERE is_peak_season = TRUE)::numeric, 2) AS avg_peak_fare,
        ROUND(AVG(total_fare_bdt) FILTER (WHERE is_peak_season = FALSE)::numeric, 2) AS avg_off_peak_fare,
        
        -- Peak premium calculation
        ROUND(
            ((AVG(total_fare_bdt) FILTER (WHERE is_peak_season = TRUE) - 
              AVG(total_fare_bdt) FILTER (WHERE is_peak_season = FALSE)) /
             NULLIF(AVG(total_fare_bdt) FILTER (WHERE is_peak_season = FALSE), 0)) * 100,
            2
        ) AS peak_premium_pct,
        
        ROUND(AVG(duration_hrs)::numeric, 2) AS avg_duration_hrs
        
    FROM {{ ref('silver_cleaned_flights') }}
    GROUP BY airline
)

SELECT
    airline,
    total_bookings,
    avg_base_fare,
    avg_tax_surcharge,
    avg_total_fare,
    min_fare,
    max_fare,
    fare_std_dev,
    median_fare,
    fare_25th_percentile,
    fare_75th_percentile,
    economy_bookings,
    business_bookings,
    first_class_bookings,
    premium_economy_bookings,
    other_class_bookings,
    domestic_bookings,
    international_bookings,
    peak_season_bookings,
    off_peak_bookings,
    avg_peak_fare,
    avg_off_peak_fare,
    peak_premium_pct,
    avg_duration_hrs,
    ROUND((total_bookings::numeric / SUM(total_bookings) OVER ()) * 100, 2) AS market_share_pct,
    CURRENT_TIMESTAMP AS generated_at
FROM airline_metrics
ORDER BY total_bookings DESC

{{
    config(
        materialized='table',
        schema='gold'
    )
}}

/*
    Gold Layer: Fare Analysis by Travel Class
    
    Captures ALL travel classes (known and unknown).
*/

WITH class_metrics AS (
    SELECT
        travel_class,
        COUNT(*) AS total_bookings,
        ROUND(AVG(base_fare_bdt)::numeric, 2) AS avg_base_fare,
        ROUND(AVG(tax_surcharge_bdt)::numeric, 2) AS avg_tax,
        ROUND(AVG(total_fare_bdt)::numeric, 2) AS avg_total_fare,
        ROUND(MIN(total_fare_bdt)::numeric, 2) AS min_fare,
        ROUND(MAX(total_fare_bdt)::numeric, 2) AS max_fare,
        ROUND((PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY total_fare_bdt))::numeric, 2) AS median_fare,
        
        -- By route type
        ROUND(AVG(total_fare_bdt) FILTER (WHERE route_type = 'Domestic')::numeric, 2) AS avg_domestic_fare,
        ROUND(AVG(total_fare_bdt) FILTER (WHERE route_type = 'International')::numeric, 2) AS avg_international_fare,
        COUNT(*) FILTER (WHERE route_type = 'Domestic') AS domestic_bookings,
        COUNT(*) FILTER (WHERE route_type = 'International') AS international_bookings,
        
        -- By peak season (flexible)
        ROUND(AVG(total_fare_bdt) FILTER (WHERE is_peak_season = TRUE)::numeric, 2) AS avg_peak_season_fare,
        ROUND(AVG(total_fare_bdt) FILTER (WHERE is_peak_season = FALSE)::numeric, 2) AS avg_regular_fare,
        COUNT(*) FILTER (WHERE is_peak_season = TRUE) AS peak_season_bookings,
        COUNT(*) FILTER (WHERE is_peak_season = FALSE) AS off_peak_bookings,
        
        -- Peak premium
        ROUND(
            ((AVG(total_fare_bdt) FILTER (WHERE is_peak_season = TRUE) - 
              AVG(total_fare_bdt) FILTER (WHERE is_peak_season = FALSE)) /
             NULLIF(AVG(total_fare_bdt) FILTER (WHERE is_peak_season = FALSE), 0)) * 100,
            2
        ) AS peak_premium_pct,
        
        ROUND(AVG(duration_hrs)::numeric, 2) AS avg_duration_hrs
        
    FROM {{ ref('silver_cleaned_flights') }}
    GROUP BY travel_class
)

SELECT
    travel_class,
    total_bookings,
    avg_base_fare,
    avg_tax,
    avg_total_fare,
    min_fare,
    max_fare,
    median_fare,
    avg_domestic_fare,
    avg_international_fare,
    domestic_bookings,
    international_bookings,
    avg_peak_season_fare,
    avg_regular_fare,
    peak_season_bookings,
    off_peak_bookings,
    peak_premium_pct,
    avg_duration_hrs,
    ROUND((total_bookings::numeric / SUM(total_bookings) OVER ()) * 100, 2) AS class_share_pct,
    CURRENT_TIMESTAMP AS generated_at
FROM class_metrics
ORDER BY total_bookings DESC

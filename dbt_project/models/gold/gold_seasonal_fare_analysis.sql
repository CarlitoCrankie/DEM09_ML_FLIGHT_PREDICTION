{{
    config(
        materialized='table',
        schema='gold'
    )
}}

/*
    Gold Layer: Seasonal Fare Analysis
    
    Analyzes fares by:
    1. Original seasonality values (preserves all unique values)
    2. Season category (grouped for high-level analysis)
    3. Peak vs Off-peak (binary for simple comparison)
*/

-- Analysis by original seasonality (captures ALL unique values)
WITH by_seasonality AS (
    SELECT
        seasonality,
        season_category,
        is_peak_season,
        COUNT(*) AS total_bookings,
        ROUND(AVG(total_fare_bdt)::numeric, 2) AS avg_fare,
        ROUND(MIN(total_fare_bdt)::numeric, 2) AS min_fare,
        ROUND(MAX(total_fare_bdt)::numeric, 2) AS max_fare,
        ROUND((PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY total_fare_bdt))::numeric, 2) AS median_fare,
        ROUND(AVG(total_fare_bdt) FILTER (WHERE route_type = 'Domestic')::numeric, 2) AS avg_domestic_fare,
        ROUND(AVG(total_fare_bdt) FILTER (WHERE route_type = 'International')::numeric, 2) AS avg_international_fare,
        ROUND(AVG(total_fare_bdt) FILTER (WHERE travel_class = 'Economy')::numeric, 2) AS avg_economy_fare,
        ROUND(AVG(total_fare_bdt) FILTER (WHERE travel_class = 'Business')::numeric, 2) AS avg_business_fare,
        ROUND(AVG(total_fare_bdt) FILTER (WHERE travel_class = 'First')::numeric, 2) AS avg_first_class_fare
    FROM {{ ref('silver_cleaned_flights') }}
    GROUP BY seasonality, season_category, is_peak_season
),

regular_avg AS (
    SELECT avg_fare 
    FROM by_seasonality 
    WHERE UPPER(seasonality) = 'REGULAR'
    LIMIT 1
)

SELECT
    s.seasonality,
    s.season_category,
    s.is_peak_season,
    s.total_bookings,
    s.avg_fare,
    s.min_fare,
    s.max_fare,
    s.median_fare,
    s.avg_domestic_fare,
    s.avg_international_fare,
    s.avg_economy_fare,
    s.avg_business_fare,
    s.avg_first_class_fare,
    ROUND(
        ((s.avg_fare - r.avg_fare) / NULLIF(r.avg_fare, 0)) * 100,
        2
    ) AS pct_diff_from_regular,
    ROUND((s.total_bookings::numeric / SUM(s.total_bookings) OVER ()) * 100, 2) AS booking_share_pct,
    CURRENT_TIMESTAMP AS generated_at
FROM by_seasonality s
CROSS JOIN regular_avg r
ORDER BY s.total_bookings DESC

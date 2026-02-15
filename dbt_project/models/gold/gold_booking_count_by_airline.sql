{{
    config(
        materialized='table',
        schema='gold'
    )
}}

/*
    Gold Layer: Booking Count by Airline
    
    Uses is_peak_season and season_category from silver for flexible analysis.
    No hardcoded season values - handles any seasonality automatically.
*/

SELECT
    airline,
    
    -- Total bookings
    COUNT(*) AS total_bookings,
    
    -- By booking source
    COUNT(*) FILTER (WHERE booking_source = 'Direct Booking') AS direct_bookings,
    COUNT(*) FILTER (WHERE booking_source = 'Travel Agency') AS agency_bookings,
    COUNT(*) FILTER (WHERE booking_source = 'Online Website') AS online_bookings,
    
    -- By booking window
    COUNT(*) FILTER (WHERE booking_window = 'Last Minute') AS last_minute_bookings,
    COUNT(*) FILTER (WHERE booking_window = 'Short Notice') AS short_notice_bookings,
    COUNT(*) FILTER (WHERE booking_window = 'Advance') AS advance_bookings,
    COUNT(*) FILTER (WHERE booking_window = 'Early Bird') AS early_bird_bookings,
    
    -- By peak vs off-peak (flexible - works with any seasonality)
    COUNT(*) FILTER (WHERE is_peak_season = TRUE) AS peak_season_bookings,
    COUNT(*) FILTER (WHERE is_peak_season = FALSE) AS off_peak_bookings,
    
    -- By season category (flexible grouping)
    COUNT(*) FILTER (WHERE season_category = 'Regular') AS regular_bookings,
    COUNT(*) FILTER (WHERE season_category = 'Religious') AS religious_holiday_bookings,
    COUNT(*) FILTER (WHERE season_category = 'Seasonal') AS seasonal_holiday_bookings,
    COUNT(*) FILTER (WHERE season_category = 'Holiday') AS general_holiday_bookings,
    COUNT(*) FILTER (WHERE season_category = 'Other') AS other_season_bookings,
    
    -- By day type
    COUNT(*) FILTER (WHERE departure_day_type = 'Weekday') AS weekday_bookings,
    COUNT(*) FILTER (WHERE departure_day_type = 'Weekend') AS weekend_bookings,
    
    -- Average days before departure
    ROUND(AVG(days_before_departure)::numeric, 1) AS avg_days_before_departure,
    
    -- Peak season percentage
    ROUND(
        (COUNT(*) FILTER (WHERE is_peak_season = TRUE)::numeric / NULLIF(COUNT(*), 0)) * 100, 
        2
    ) AS peak_season_pct,
    
    -- Market share
    ROUND((COUNT(*)::numeric / SUM(COUNT(*)) OVER ()) * 100, 2) AS market_share_pct,
    
    CURRENT_TIMESTAMP AS generated_at
    
FROM {{ ref('silver_cleaned_flights') }}
GROUP BY airline
ORDER BY total_bookings DESC

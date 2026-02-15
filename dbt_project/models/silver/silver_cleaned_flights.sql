{{
    config(
        materialized='table',
        schema='silver'
    )
}}

/*
    Silver Layer: Cleaned Flights
    
    UPDATED FOR INCREMENTAL LOADING:
    - Now filters for is_active = TRUE to exclude soft-deleted records
    - Only processes currently active records from bronze layer
    
    This model:
    1. Filters to only valid AND active records
    2. Standardizes text fields
    3. Adds derived columns
    4. Handles ANY categorical values flexibly
    
    Peak Season Detection:
    - Uses pattern matching for known peak indicators
    - Any seasonality containing holiday-related words = Peak
    - Unknown values default to Regular (safe assumption)
*/

WITH source_data AS (
    SELECT *
    FROM {{ source('bronze', 'validated_flights') }}
    WHERE is_valid = TRUE
      AND is_active = TRUE  -- NEW: Only process active records (incremental loading support)
),

cleaned AS (
    SELECT
        -- Primary key
        id,
        
        -- Airline (standardized)
        INITCAP(TRIM(airline)) AS airline,
        
        -- Source airport
        UPPER(TRIM(source_code)) AS source_code,
        INITCAP(TRIM(source_name)) AS source_name,
        
        -- Destination airport
        UPPER(TRIM(destination_code)) AS destination_code,
        INITCAP(TRIM(destination_name)) AS destination_name,
        
        -- Derived: Route
        UPPER(TRIM(source_code)) || '-' || UPPER(TRIM(destination_code)) AS route,
        
        -- Time information
        departure_datetime,
        arrival_datetime,
        duration_hrs,
        
        -- Derived: Time dimensions
        EXTRACT(YEAR FROM departure_datetime)::INT AS departure_year,
        EXTRACT(MONTH FROM departure_datetime)::INT AS departure_month,
        EXTRACT(DOW FROM departure_datetime)::INT AS departure_day_of_week,
        CASE 
            WHEN EXTRACT(DOW FROM departure_datetime) IN (0, 6) THEN 'Weekend'
            ELSE 'Weekday'
        END AS departure_day_type,
        
        -- Flight details
        stopovers,
        CASE 
            WHEN stopovers = 'Direct' THEN 0
            WHEN stopovers LIKE '%1%' THEN 1
            WHEN stopovers LIKE '%2%' THEN 2
            ELSE NULL
        END AS num_stops,
        aircraft_type,
        
        -- Travel class (standardize known variations, keep unknown as-is)
        CASE 
            WHEN UPPER(TRIM(travel_class)) IN ('FIRST CLASS', 'FIRST', '1ST CLASS', '1ST') THEN 'First'
            WHEN UPPER(TRIM(travel_class)) IN ('BUSINESS CLASS', 'BUSINESS', 'BIZ') THEN 'Business'
            WHEN UPPER(TRIM(travel_class)) IN ('ECONOMY CLASS', 'ECONOMY', 'COACH') THEN 'Economy'
            WHEN UPPER(TRIM(travel_class)) IN ('PREMIUM ECONOMY', 'PREMIUM', 'PREMIUM ECO') THEN 'Premium Economy'
            ELSE INITCAP(TRIM(travel_class))
        END AS travel_class,
        
        booking_source,
        
        -- Fare information
        base_fare_bdt,
        tax_surcharge_bdt,
        total_fare_bdt,
        
        -- Derived: Fare calculations
        ROUND((tax_surcharge_bdt / NULLIF(base_fare_bdt, 0) * 100)::numeric, 2) AS tax_percentage,
        CASE
            WHEN total_fare_bdt < 10000 THEN 'Budget'
            WHEN total_fare_bdt < 50000 THEN 'Mid-Range'
            WHEN total_fare_bdt < 100000 THEN 'Premium'
            ELSE 'Luxury'
        END AS fare_category,
        
        -- Seasonality (keep original value)
        seasonality,
        
        /*
            Peak Season Detection - Flexible Pattern Matching
            
            Logic: If seasonality contains ANY of these indicators, it's peak season:
            - Religious holidays: Eid, Hajj, Ramadan, Diwali, Puja, Christmas, Easter
            - Seasonal: Winter, Summer, Spring (holidays)
            - Generic: Holiday, Festival, Peak, High Season, Vacation
            
            This catches:
            - "Winter Holidays" ✓
            - "Eid ul-Fitr" ✓
            - "Christmas Break" ✓
            - "Durga Puja" ✓
            - "Summer Vacation" ✓
            - "Peak Season" ✓
            - Any future holiday we haven't thought of that contains these words ✓
        */
        CASE
            -- Religious holidays
            WHEN UPPER(seasonality) LIKE '%EID%' THEN TRUE
            WHEN UPPER(seasonality) LIKE '%HAJJ%' THEN TRUE
            WHEN UPPER(seasonality) LIKE '%RAMADAN%' THEN TRUE
            WHEN UPPER(seasonality) LIKE '%DIWALI%' THEN TRUE
            WHEN UPPER(seasonality) LIKE '%PUJA%' THEN TRUE
            WHEN UPPER(seasonality) LIKE '%CHRISTMAS%' THEN TRUE
            WHEN UPPER(seasonality) LIKE '%EASTER%' THEN TRUE
            WHEN UPPER(seasonality) LIKE '%HOLI%' THEN TRUE
            WHEN UPPER(seasonality) LIKE '%DURGA%' THEN TRUE
            WHEN UPPER(seasonality) LIKE '%NAVRATRI%' THEN TRUE
            WHEN UPPER(seasonality) LIKE '%DUSSEHRA%' THEN TRUE
            WHEN UPPER(seasonality) LIKE '%THANKSGIVING%' THEN TRUE
            WHEN UPPER(seasonality) LIKE '%NEW YEAR%' THEN TRUE
            
            -- Generic holiday/peak indicators
            WHEN UPPER(seasonality) LIKE '%HOLIDAY%' THEN TRUE
            WHEN UPPER(seasonality) LIKE '%FESTIVAL%' THEN TRUE
            WHEN UPPER(seasonality) LIKE '%PEAK%' THEN TRUE
            WHEN UPPER(seasonality) LIKE '%HIGH SEASON%' THEN TRUE
            WHEN UPPER(seasonality) LIKE '%VACATION%' THEN TRUE
            WHEN UPPER(seasonality) LIKE '%BREAK%' THEN TRUE
            
            -- Seasonal (when combined with holiday context)
            WHEN UPPER(seasonality) LIKE '%WINTER%' THEN TRUE
            WHEN UPPER(seasonality) LIKE '%SUMMER%' AND UPPER(seasonality) LIKE '%HOLIDAY%' THEN TRUE
            WHEN UPPER(seasonality) LIKE '%SPRING%' AND UPPER(seasonality) LIKE '%BREAK%' THEN TRUE
            
            -- Default: Not peak season
            ELSE FALSE
        END AS is_peak_season,
        
        /*
            Season Category - Groups similar seasons for reporting
            
            Groups:
            - Regular: Normal/off-peak periods
            - Religious: Eid, Hajj, Christmas, etc.
            - Seasonal: Winter, Summer holidays
            - Other: Any unrecognized season (still captured, not lost)
        */
        CASE
            WHEN UPPER(seasonality) = 'REGULAR' THEN 'Regular'
            WHEN UPPER(seasonality) LIKE '%EID%' THEN 'Religious'
            WHEN UPPER(seasonality) LIKE '%HAJJ%' THEN 'Religious'
            WHEN UPPER(seasonality) LIKE '%CHRISTMAS%' THEN 'Religious'
            WHEN UPPER(seasonality) LIKE '%EASTER%' THEN 'Religious'
            WHEN UPPER(seasonality) LIKE '%PUJA%' THEN 'Religious'
            WHEN UPPER(seasonality) LIKE '%DIWALI%' THEN 'Religious'
            WHEN UPPER(seasonality) LIKE '%WINTER%' THEN 'Seasonal'
            WHEN UPPER(seasonality) LIKE '%SUMMER%' THEN 'Seasonal'
            WHEN UPPER(seasonality) LIKE '%HOLIDAY%' THEN 'Holiday'
            WHEN UPPER(seasonality) LIKE '%FESTIVAL%' THEN 'Festival'
            WHEN UPPER(seasonality) LIKE '%VACATION%' THEN 'Vacation'
            ELSE 'Other'
        END AS season_category,
        
        days_before_departure,
        CASE
            WHEN days_before_departure <= 7 THEN 'Last Minute'
            WHEN days_before_departure <= 14 THEN 'Short Notice'
            WHEN days_before_departure <= 30 THEN 'Advance'
            ELSE 'Early Bird'
        END AS booking_window,
        
        -- Derived: Route type
        CASE
            WHEN source_code IN ('DAC', 'CGP', 'ZYL', 'CXB', 'JSR', 'RJH', 'SPD', 'BZL')
                AND destination_code IN ('DAC', 'CGP', 'ZYL', 'CXB', 'JSR', 'RJH', 'SPD', 'BZL')
            THEN 'Domestic'
            ELSE 'International'
        END AS route_type,
        
        -- Lineage
        mysql_raw_id,
        mysql_validated_id,
        mysql_loaded_at,
        bronze_loaded_at,
        
        -- NEW: Incremental loading lineage
        record_hash,
        ingestion_timestamp,
        
        -- Current timestamp
        CURRENT_TIMESTAMP AS silver_loaded_at
        
    FROM source_data
)

SELECT * FROM cleaned
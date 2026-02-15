{{
    config(
        materialized='table',
        schema='gold'
    )
}}

/*
    Gold Layer: Data Quality Report

    Tracks all values in categorical fields.
    Useful for monitoring data quality and identifying new categories.
*/

WITH seasonality_summary AS (
    SELECT
        'seasonality'::text AS field_name,
        seasonality::text AS field_value,
        season_category::text AS category,
        CASE WHEN is_peak_season THEN 'Yes' ELSE 'No' END AS is_peak,
        COUNT(*)::bigint AS record_count
    FROM {{ ref('silver_cleaned_flights') }}
    GROUP BY seasonality, season_category, is_peak_season
),

travel_class_summary AS (
    SELECT
        'travel_class'::text AS field_name,
        travel_class::text AS field_value,
        'N/A'::text AS category,
        'N/A'::text AS is_peak,
        COUNT(*)::bigint AS record_count
    FROM {{ ref('silver_cleaned_flights') }}
    GROUP BY travel_class
),

booking_source_summary AS (
    SELECT
        'booking_source'::text AS field_name,
        booking_source::text AS field_value,
        'N/A'::text AS category,
        'N/A'::text AS is_peak,
        COUNT(*)::bigint AS record_count
    FROM {{ ref('silver_cleaned_flights') }}
    GROUP BY booking_source
),

stopovers_summary AS (
    SELECT
        'stopovers'::text AS field_name,
        stopovers::text AS field_value,
        'N/A'::text AS category,
        'N/A'::text AS is_peak,
        COUNT(*)::bigint AS record_count
    FROM {{ ref('silver_cleaned_flights') }}
    GROUP BY stopovers
),

combined AS (
    SELECT field_name, field_value, category, is_peak, record_count FROM seasonality_summary
    UNION ALL
    SELECT field_name, field_value, category, is_peak, record_count FROM travel_class_summary
    UNION ALL
    SELECT field_name, field_value, category, is_peak, record_count FROM booking_source_summary
    UNION ALL
    SELECT field_name, field_value, category, is_peak, record_count FROM stopovers_summary
)

SELECT
    field_name,
    field_value,
    category,
    is_peak,
    record_count,
    CURRENT_TIMESTAMP AS generated_at
FROM combined
ORDER BY field_name, record_count DESC

{{ config(materialized='view') }}

WITH monthly_data AS (
    SELECT
        property_type,
        room_type,
        accommodates,
        DATE_TRUNC('month', date)::date AS month_year,
        COUNT(*) FILTER (WHERE has_availability = 't' AND date >= property_valid_from AND date <= property_valid_to) AS active_listings,
        COUNT(*) AS total_listings,
        COUNT(*) FILTER (WHERE has_availability = 'f' AND date >= property_valid_from AND date <= property_valid_to) AS inactive_listings,
        MIN(price) FILTER (WHERE has_availability = 't' AND date >= property_valid_from AND date <= property_valid_to) AS min_price,
        MAX(price) FILTER (WHERE has_availability = 't' AND date >= property_valid_from AND date <= property_valid_to) AS max_price,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price) FILTER (WHERE has_availability = 't' AND date >= property_valid_from AND date <= property_valid_to) AS median_price,
        ROUND(AVG(price) FILTER (WHERE has_availability = 't' AND date >= property_valid_from AND date <= property_valid_to)::numeric, 2) AS avg_price,
        COUNT(DISTINCT host_id) AS distinct_hosts,
        COUNT(DISTINCT host_id) FILTER (WHERE host_is_superhost = 't' AND date >= host_valid_from AND date <= host_valid_to) AS superhost_count,
        ROUND(AVG(review_scores_rating) FILTER (WHERE has_availability = 't' AND date >= property_valid_from AND date <= property_valid_to)::numeric, 2) AS avg_review_score,
        SUM(30 - availability_30) FILTER (WHERE has_availability = 't' AND date >= property_valid_from AND date <= property_valid_to) AS total_stays,
        SUM((30 - availability_30) * price) FILTER (WHERE has_availability = 't' AND date >= property_valid_from AND date <= property_valid_to) AS total_estimated_revenue
    FROM {{ ref('facts_listings') }}
    GROUP BY property_type, room_type, accommodates, month_year
),
percentage_changes AS (
    SELECT
        property_type,
        room_type,
        accommodates,
        month_year,
        active_listings,
        total_listings - active_listings AS inactive_listings,
        LAG(active_listings, 1) OVER (PARTITION BY property_type, room_type, accommodates ORDER BY month_year) AS previous_month_active_listings,
        LAG(total_listings - active_listings, 1) OVER (PARTITION BY property_type, room_type, accommodates ORDER BY month_year) AS previous_month_inactive_listings
    FROM monthly_data
)
SELECT
    md.property_type,
    md.room_type,
    md.accommodates,
    md.month_year,
    ROUND((md.active_listings::numeric / NULLIF(md.total_listings, 0)) * 100, 2) AS active_listing_rate,
    md.min_price,
    md.max_price,
    md.median_price,
    md.avg_price,
    md.distinct_hosts,
    ROUND((md.superhost_count::numeric / NULLIF(md.distinct_hosts, 0)) * 100, 2) AS superhost_rate,
    md.avg_review_score,
    ROUND(((pc.active_listings - pc.previous_month_active_listings)::numeric / NULLIF(pc.previous_month_active_listings, 0)) * 100, 2) AS percentage_change_active_listings,
    ROUND(((pc.inactive_listings - pc.previous_month_inactive_listings)::numeric / NULLIF(pc.previous_month_inactive_listings, 0)) * 100, 2) AS percentage_change_inactive_listings,
    md.total_stays,
    ROUND(md.total_estimated_revenue::numeric / NULLIF(md.active_listings, 0), 2) AS avg_estimated_revenue_per_active_listing
FROM monthly_data md
LEFT JOIN percentage_changes pc ON md.property_type = pc.property_type
                               AND md.room_type = pc.room_type
                               AND md.accommodates = pc.accommodates
                               AND md.month_year = pc.month_year
ORDER BY md.property_type, md.room_type, md.accommodates, md.month_year

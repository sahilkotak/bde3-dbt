{{ config(materialized='view') }}

WITH monthly_data AS (
    SELECT
        listing_neighbourhood,
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
    WHERE date >= property_valid_from AND date <= property_valid_to
    GROUP BY listing_neighbourhood, month_year
),
percentage_changes AS (
    SELECT
        listing_neighbourhood,
        month_year,
        active_listings,
        total_listings - active_listings AS inactive_listings,
        LAG(active_listings, 1) OVER (PARTITION BY listing_neighbourhood ORDER BY month_year) AS previous_month_active_listings,
        LAG(total_listings - active_listings, 1) OVER (PARTITION BY listing_neighbourhood ORDER BY month_year) AS previous_month_inactive_listings
    FROM monthly_data
)
SELECT
    m.listing_neighbourhood,
    m.month_year,
    ROUND((m.active_listings::numeric / NULLIF(m.total_listings, 0)) * 100, 2) AS active_listing_rate,
    m.min_price,
    m.max_price,
    m.median_price,
    m.avg_price,
    m.distinct_hosts,
    ROUND((m.superhost_count::numeric / NULLIF(m.distinct_hosts, 0)) * 100, 2) AS superhost_rate,
    m.avg_review_score,
    ROUND(((p.active_listings - p.previous_month_active_listings)::numeric / NULLIF(p.previous_month_active_listings, 0)) * 100, 2) AS percentage_change_active_listings,
    ROUND(((p.inactive_listings - p.previous_month_inactive_listings)::numeric / NULLIF(p.previous_month_inactive_listings, 0)) * 100, 2) AS percentage_change_inactive_listings,
    m.total_stays,
    ROUND(m.total_estimated_revenue::numeric / NULLIF(m.active_listings, 0), 2) AS avg_estimated_revenue_per_active_listing
FROM monthly_data m
LEFT JOIN percentage_changes p ON m.listing_neighbourhood = p.listing_neighbourhood AND m.month_year = p.month_year
ORDER BY m.listing_neighbourhood, m.month_year
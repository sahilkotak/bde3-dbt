-- dm_listing_neighbourhood View
{{ config(materialized='view') }}

WITH base AS (
    SELECT
        listing_neighbourhood,
        DATE_TRUNC('month', date) as month_year,
        host_id,
        host_is_superhost,
        price,
        has_availability,
        availability_30,
        review_scores_rating
    FROM {{ ref('facts_listings') }}
),
active_listings AS (
    SELECT
        listing_neighbourhood,
        month_year,
        COUNT(*) FILTER (WHERE has_availability = 't') AS active_count,
        COUNT(*) AS total_count
    FROM base
    GROUP BY listing_neighbourhood, month_year
),
price_stats AS (
    SELECT
        listing_neighbourhood,
        month_year,
        MIN(price) FILTER (WHERE has_availability = 't') AS min_price,
        MAX(price) FILTER (WHERE has_availability = 't') AS max_price,
        AVG(price) FILTER (WHERE has_availability = 't') AS avg_price,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price) FILTER (WHERE has_availability = 't') AS median_price
    FROM base
    GROUP BY listing_neighbourhood, month_year
),
host_stats AS (
    SELECT
        listing_neighbourhood,
        month_year,
        COUNT(DISTINCT host_id) AS distinct_hosts,
        COUNT(DISTINCT host_id) FILTER (WHERE host_is_superhost = 't') AS superhost_count
    FROM base
    GROUP BY listing_neighbourhood, month_year
),
review_scores AS (
    SELECT
        listing_neighbourhood,
        month_year,
        AVG(review_scores_rating) FILTER (WHERE has_availability = 't') AS avg_review_score
    FROM base
    GROUP BY listing_neighbourhood, month_year
),
stays_and_revenue AS (
    SELECT
        listing_neighbourhood,
        month_year,
        SUM(30 - availability_30) FILTER (WHERE has_availability = 't') AS total_stays,
        SUM((30 - availability_30) * price) FILTER (WHERE has_availability = 't') AS total_revenue
    FROM base
    GROUP BY listing_neighbourhood, month_year
),
-- Calculate percentage changes using window functions
percentage_changes AS (
    SELECT
        listing_neighbourhood,
        month_year,
        active_count,
        total_count,
        LAG(active_count, 1) OVER (PARTITION BY listing_neighbourhood ORDER BY month_year) AS prev_month_active_count,
        LAG(total_count, 1) OVER (PARTITION BY listing_neighbourhood ORDER BY month_year) AS prev_month_total_count
    FROM active_listings
),
final_percentage_changes AS (
    SELECT
        listing_neighbourhood,
        month_year,
        (active_count - prev_month_active_count) / NULLIF(prev_month_active_count, 0) AS active_listings_percentage_change,
        (total_count - prev_month_total_count) / NULLIF(prev_month_total_count, 0) AS inactive_listings_percentage_change
    FROM percentage_changes
)
SELECT
    a.listing_neighbourhood,
    a.month_year,
    (active_count::float / total_count) * 100 AS active_listing_rate,
    p.min_price,
    p.max_price,
    p.median_price,
    p.avg_price,
    h.distinct_hosts,
    (superhost_count::float / distinct_hosts) * 100 AS superhost_rate,
    r.avg_review_score,
    fpc.active_listings_percentage_change,
    fpc.inactive_listings_percentage_change,
    s.total_stays,
    (total_revenue / NULLIF(active_count, 0)) AS avg_estimated_revenue_per_active_listing
FROM active_listings a
JOIN price_stats p ON a.listing_neighbourhood = p.listing_neighbourhood AND a.month_year = p.month_year
JOIN host_stats h ON a.listing_neighbourhood = h.listing_neighbourhood AND a.month_year = h.month_year
JOIN review_scores r ON a.listing_neighbourhood = r.listing_neighbourhood AND a.month_year = r.month_year
JOIN stays_and_revenue s ON a.listing_neighbourhood = s.listing_neighbourhood AND a.month_year = s.month_year
JOIN final_percentage_changes fpc ON a.listing_neighbourhood = fpc.listing_neighbourhood AND a.month_year = fpc.month_year
ORDER BY a.listing_neighbourhood, a.month_year
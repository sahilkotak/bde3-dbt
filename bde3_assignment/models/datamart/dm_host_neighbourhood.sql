{{ config(materialized='view') }}

WITH monthly_host_data AS (
    SELECT
        host_neighbourhood_lga_name AS host_neighbourhood_lga,
        DATE_TRUNC('month', date)::date AS month_year,
        COUNT(DISTINCT host_id) AS distinct_hosts,
        SUM((30 - availability_30) * price) FILTER (WHERE has_availability = 't' AND date >= host_valid_from AND date <= host_valid_to) AS estimated_revenue
    FROM {{ ref('facts_listings') }}
    WHERE has_availability = 't' AND date >= host_valid_from AND date <= host_valid_to
    GROUP BY host_neighbourhood_lga, month_year
)
SELECT
    mh.host_neighbourhood_lga,
    mh.month_year,
    mh.distinct_hosts,
    mh.estimated_revenue,
    ROUND(mh.estimated_revenue::numeric / NULLIF(mh.distinct_hosts, 0), 2) AS estimated_revenue_per_host
FROM monthly_host_data mh
ORDER BY mh.host_neighbourhood_lga, mh.month_year
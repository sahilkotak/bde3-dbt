{{ config(materialized='table') }}

WITH host_data AS (
    SELECT
        host_id,
        host_name,
        host_since,
        host_is_superhost,
        host_neighbourhood,
        scraped_date,
        dbt_scd_id,
        dbt_updated_at,
        dbt_valid_from,
        dbt_valid_to
    FROM {{ ref('host_stg') }}
),
suburb_to_lga AS (
    SELECT
        suburb_name,
        lga_name
    FROM {{ ref('nsw_lga_suburb_stg') }}
),
lga_code AS (
    SELECT
        lga_name AS lga_name_code,
        lga_code
    FROM {{ ref('nsw_lga_code_stg') }}
)
SELECT
    h.host_id,
    h.host_name,
    h.host_since,
    h.host_is_superhost,
    h.host_neighbourhood,
    s.lga_name AS host_neighbourhood_lga,
    l.lga_code AS host_neighbourhood_lga_code,
    h.scraped_date,
    h.dbt_scd_id,
    h.dbt_updated_at,
    h.dbt_valid_from,
    h.dbt_valid_to
FROM host_data h
LEFT JOIN suburb_to_lga s ON h.host_neighbourhood = s.suburb_name
LEFT JOIN lga_code l ON s.lga_name = l.lga_name_code
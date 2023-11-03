{{ config(materialized='table') }}

WITH listing AS (
    SELECT
        p.listing_id,
        p.scraped_date AS date,
        p.listing_neighbourhood,
        p.host_id,
        p.property_type,
        rt.room_type,
        rt.price,
        rt.accommodates,
        rt.has_availability,
        rt.availability_30,
        rv.number_of_reviews,
        rv.review_scores_rating,
        -- Include the SCD columns for property and room
        p.dbt_valid_from AS property_valid_from,
        p.dbt_valid_to AS property_valid_to,
        rt.dbt_valid_from AS room_valid_from,
        rt.dbt_valid_to AS room_valid_to
    FROM {{ ref('property_stg') }} AS p
    INNER JOIN {{ ref('room_stg') }} AS rt 
        ON p.listing_id = rt.listing_id 
        AND p.scraped_date >= rt.dbt_valid_from 
        AND (p.scraped_date < rt.dbt_valid_to OR rt.dbt_valid_to IS NULL)
    INNER JOIN {{ ref('reviews_stg') }} AS rv ON p.listing_id = rv.listing_id
),
host_dimension AS (
    SELECT
        h.host_id,
        h.host_name,
        h.host_since,
        h.host_is_superhost,
        h.host_neighbourhood,
        -- Include the SCD columns for host
        h.dbt_valid_from AS host_valid_from,
        h.dbt_valid_to AS host_valid_to,
        s.lga_name AS host_neighbourhood_lga_name,
        l.lga_code AS host_neighbourhood_lga_code
    FROM {{ ref('host_stg') }} AS h
    LEFT JOIN {{ ref('nsw_lga_suburb_stg') }} AS s ON h.host_neighbourhood = s.suburb_name
    LEFT JOIN {{ ref('nsw_lga_code_stg') }} AS l ON s.lga_name = l.lga_name
),
neighbourhood_to_lga AS (
    SELECT
        lga_name,
        lga_code
    FROM {{ ref('nsw_lga_code_stg') }}
)
-- The final select should include the SCD columns for the fact table records
SELECT
    li.listing_id,
    li.date,
    li.listing_neighbourhood,
    n.lga_code AS listing_neighbourhood_lga_code,
    hd.host_id,
    hd.host_name,
    hd.host_since,
    hd.host_is_superhost,
    hd.host_neighbourhood,
    hd.host_neighbourhood_lga_code,
    hd.host_neighbourhood_lga_name,
    li.property_type,
    li.room_type,
    li.price,
    li.accommodates,
    li.has_availability,
    li.availability_30,
    li.number_of_reviews,
    li.review_scores_rating,
    -- Include the SCD columns in the select
    li.property_valid_from,
    li.property_valid_to,
    li.room_valid_from,
    li.room_valid_to,
    -- Include the SCD columns for the host
    hd.host_valid_from,
    hd.host_valid_to
FROM listing li
LEFT JOIN host_dimension hd 
    ON li.host_id = hd.host_id 
    AND li.date >= hd.host_valid_from 
    AND (li.date < hd.host_valid_to OR hd.host_valid_to IS NULL)
LEFT JOIN neighbourhood_to_lga n ON li.listing_neighbourhood = n.lga_name
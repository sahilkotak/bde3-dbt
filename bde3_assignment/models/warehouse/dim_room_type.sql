{{ config(materialized='table') }}

WITH room_changes AS (
    SELECT
        r.listing_id,
        r.room_type,
        r.accommodates,
        r.dbt_valid_from,
        COALESCE(r.dbt_valid_to, '9999-12-31'::date) AS dbt_valid_to, -- Future date for active records
        r.scraped_date,
        r.dbt_scd_id,
        r.dbt_updated_at
    FROM {{ ref('room_stg') }} r
)

SELECT
    -- The room_type_id reflects a unique ID for each unique combination of room_type and accommodates
    MD5(room_type || accommodates) AS room_type_id, -- Using MD5 hash to make it a more consistent identifier
    room_type,
    accommodates,
    dbt_valid_from,
    dbt_valid_to,
    dbt_scd_id,
    dbt_updated_at
FROM room_changes
ORDER BY room_type, accommodates, dbt_valid_from
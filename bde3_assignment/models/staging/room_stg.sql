WITH room_changes AS (
    SELECT
        CAST("LISTING_ID" AS BIGINT) AS listing_id,
        COALESCE(NULLIF(TRIM("ROOM_TYPE"), ''), 'Unknown') AS room_type,
        COALESCE(CAST("ACCOMMODATES" AS INTEGER), 1) AS accommodates,
        COALESCE(CAST("PRICE" AS DECIMAL(10,2)), 0.00) AS price,
        -- Convert has_availability to boolean
        "HAS_AVAILABILITY"::BOOLEAN AS has_availability,
        COALESCE(CAST("AVAILABILITY_30" AS INTEGER), 30) AS availability_30,
        CASE 
            WHEN TO_DATE("SCRAPED_DATE", 'YYYY/MM/DD') <= CURRENT_DATE THEN TO_DATE("SCRAPED_DATE", 'YYYY/MM/DD')
            ELSE NULL 
        END AS scraped_date,
        -- Include your SCD tracking columns here
        "dbt_scd_id",
        TO_DATE("dbt_updated_at", 'YYYY/MM/DD') AS dbt_updated_at,
        TO_DATE("dbt_valid_from", 'YYYY/MM/DD') AS dbt_valid_from,
        TO_DATE(dbt_valid_to, 'YYYY/MM/DD') AS dbt_valid_to
    FROM raw.room_snapshot
)

SELECT
    r.listing_id,
    r.room_type,
    r.accommodates,
    r.price,
    r.has_availability,
    r.availability_30,
    r.scraped_date,
    r.dbt_scd_id,
    r.dbt_updated_at,
    r.dbt_valid_from,
    r.dbt_valid_to
FROM room_changes AS r
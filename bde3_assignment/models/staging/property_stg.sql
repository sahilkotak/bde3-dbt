WITH ranked_data AS (
    SELECT
        "LISTING_ID",
        "PROPERTY_TYPE",
        "LISTING_NEIGHBOURHOOD",
        "HOST_ID",
        "SCRAPED_DATE",
        dbt_scd_id,
        TO_DATE(dbt_updated_at, 'YYYY/MM/DD') AS dbt_updated_at,
        TO_DATE(dbt_valid_from, 'YYYY/MM/DD') AS dbt_valid_from,
        TO_DATE(dbt_valid_to, 'YYYY/MM/DD') AS dbt_valid_to
    FROM raw.property_snapshot
)

SELECT
    CAST("LISTING_ID" AS BIGINT) AS listing_id,
    COALESCE(NULLIF(TRIM("PROPERTY_TYPE"), ''), 'Unknown') AS property_type,
    COALESCE(NULLIF(TRIM("LISTING_NEIGHBOURHOOD"), ''), 'Not Provided') AS listing_neighbourhood,
    CAST("HOST_ID" AS BIGINT) AS host_id,
    CASE 
        WHEN TO_DATE("SCRAPED_DATE", 'YYYY/MM/DD') <= CURRENT_DATE THEN TO_DATE("SCRAPED_DATE", 'YYYY/MM/DD')
        ELSE NULL 
    END AS scraped_date,
    dbt_scd_id AS dbt_scd_id,
    dbt_updated_at AS dbt_updated_at,
    dbt_valid_from AS dbt_valid_from,
    dbt_valid_to AS dbt_valid_to
FROM ranked_data
WHERE 
    "LISTING_ID" IS NOT NULL
    AND "HOST_ID" IS NOT NULL
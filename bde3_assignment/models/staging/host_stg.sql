WITH host_changes AS (
    SELECT
        CAST("HOST_ID" AS BIGINT) AS host_id,
        CAST("LISTING_ID" AS BIGINT) AS listing_id,
        "host_listing_key",
        COALESCE(NULLIF(TRIM("HOST_NAME"), ''), 'Unknown') AS host_name, 
        CASE 
            WHEN "HOST_SINCE" IS NOT NULL AND TO_DATE("HOST_SINCE", 'DD/MM/YYYY') <= CURRENT_DATE 
            THEN TO_DATE("HOST_SINCE", 'DD/MM/YYYY')
            ELSE NULL 
        END AS host_since,
        CASE 
            WHEN "HOST_IS_SUPERHOST" = 't' THEN TRUE
            WHEN "HOST_IS_SUPERHOST" = 'f' THEN FALSE
            ELSE NULL 
        END AS host_is_superhost,
        COALESCE(NULLIF(TRIM("HOST_NEIGHBOURHOOD"), ''), 'Not Provided') AS host_neighbourhood,
        CAST("SCRAPED_DATE" AS DATE) AS scraped_date,
        dbt_scd_id,
        TO_DATE(dbt_updated_at, 'YYYY/MM/DD') AS dbt_updated_at,
        TO_DATE(dbt_valid_from, 'YYYY/MM/DD') AS dbt_valid_from,
        TO_DATE(dbt_valid_to, 'YYYY/MM/DD') AS dbt_valid_to
    FROM raw.host_snapshot
)

SELECT
    host_id,
    listing_id,
    host_listing_key,
    host_name,
    host_since,
    host_is_superhost,
    host_neighbourhood,
    scraped_date,
    dbt_scd_id,
    dbt_updated_at,
    dbt_valid_from,
    dbt_valid_to
FROM host_changes
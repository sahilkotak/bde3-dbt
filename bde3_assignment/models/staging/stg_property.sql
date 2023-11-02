WITH ranked_data AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY "LISTING_ID" 
               ORDER BY "SCRAPED_DATE" DESC
           ) as row_num
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
    END AS scraped_date
FROM ranked_data
WHERE 
    row_num = 1
    AND "LISTING_ID" IS NOT NULL
    AND "HOST_ID" IS NOT NULL
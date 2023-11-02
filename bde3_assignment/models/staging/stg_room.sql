WITH ranked_data AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY "LISTING_ID" 
               ORDER BY "SCRAPED_DATE" DESC
           ) as row_num
    FROM raw.room_snapshot
)

SELECT
    CAST("LISTING_ID" AS BIGINT) AS listing_id,
    COALESCE(NULLIF(TRIM("ROOM_TYPE"), ''), 'Unknown') AS room_type,
    COALESCE(CAST("ACCOMMODATES" AS INTEGER), 1) AS accommodates,
    COALESCE(CAST("PRICE" AS DECIMAL(10,2)), 0.00) AS price,
    CASE 
        WHEN "HAS_AVAILABILITY" = 't' THEN 'Yes'
        WHEN "HAS_AVAILABILITY" = 'f' THEN 'No'
        ELSE NULL 
    END AS has_availability,
    COALESCE(CAST("AVAILABILITY_30" AS INTEGER), 30) AS availability_30,
    CASE 
        WHEN TO_DATE("SCRAPED_DATE", 'YYYY/MM/DD') <= CURRENT_DATE THEN TO_DATE("SCRAPED_DATE", 'YYYY/MM/DD')
        ELSE NULL 
    END AS scraped_date
FROM ranked_data
WHERE 
    row_num = 1
    AND "LISTING_ID" IS NOT NULL

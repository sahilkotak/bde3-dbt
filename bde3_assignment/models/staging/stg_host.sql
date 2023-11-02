-- Start with a Common Table Expression (CTE) to rank the data.
WITH ranked_data AS (
    -- Select all columns from the raw host snapshot.
    SELECT *,
           -- Generate a row number for each host ID based on our ordering logic.
           ROW_NUMBER() OVER (
               PARTITION BY "HOST_ID" 
               ORDER BY 
                   -- Prioritize records with non-null host names.
                   -- Assign a 1 to records with non-null host names and a 2 to those with null host names.
                   CASE WHEN "HOST_NAME" IS NOT NULL THEN 1 ELSE 2 END,
                   -- Within those groups, order by the scraped date to get the most recent record.
                   "SCRAPED_DATE" DESC
           ) as row_num
    FROM raw.host_snapshot
)

-- Select relevant columns and apply transformations.
SELECT
    -- Convert HOST_ID to BIGINT for consistency.
    CAST("HOST_ID" AS BIGINT) AS host_id,
    -- If HOST_NAME is null or blank, assign 'Unknown'. Otherwise, use the given name.
    COALESCE(NULLIF(TRIM("HOST_NAME"), ''), 'Unknown') AS host_name, 
    -- Convert HOST_SINCE to date format if it's less than or equal to the current date. If not, assign NULL.
    CASE 
        WHEN TO_DATE("HOST_SINCE", 'DD/MM/YYYY') <= CURRENT_DATE THEN TO_DATE("HOST_SINCE", 'DD/MM/YYYY')
        ELSE NULL 
    END AS host_since,
    -- Interpret the HOST_IS_SUPERHOST flag.
    CASE 
        WHEN "HOST_IS_SUPERHOST" = 't' THEN 'Yes'
        WHEN "HOST_IS_SUPERHOST" = 'f' THEN 'No'
        ELSE NULL 
    END AS host_is_superhost,
    -- If HOST_NEIGHBOURHOOD is null or blank, assign 'Not Provided'. Otherwise, use the given neighbourhood.
    COALESCE(NULLIF(TRIM("HOST_NEIGHBOURHOOD"), ''), 'Not Provided') AS host_neighbourhood,
    -- Convert SCRAPED_DATE to date format if it's less than or equal to the current date. If not, assign NULL.
    CASE 
        WHEN TO_DATE("SCRAPED_DATE", 'YYYY/MM/DD') <= CURRENT_DATE THEN TO_DATE("SCRAPED_DATE", 'YYYY/MM/DD')
        ELSE NULL 
    END AS scraped_date
-- Filter to only take the top-ranked (most recent and prioritized) record for each host ID.
FROM ranked_data
WHERE 
    row_num = 1
    AND "HOST_ID" IS NOT NULL
    AND "HOST_SINCE" IS NOT NULL
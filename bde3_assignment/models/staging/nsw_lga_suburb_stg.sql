-- Create a Common Table Expression (CTE) to rank the data based on the combination of lga_name and suburb_name.
WITH ranked_data AS (
    SELECT *,
           -- Generate a row number for each combination of lga_name and suburb_name to identify duplicates.
           ROW_NUMBER() OVER (PARTITION BY lga_name, suburb_name) AS row_num
    FROM {{ source('raw', 'nsw_lga_suburb') }}
)

-- Select all columns from the ranked_data where the row_num is 1 (this filters out duplicates).
SELECT 
    INITCAP(lga_name) AS lga_name,
    INITCAP(suburb_name) AS suburb_name
FROM ranked_data
WHERE row_num = 1
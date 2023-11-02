-- Create a Common Table Expression (CTE) to rank the data based on lga_code.
WITH ranked_data AS (
    SELECT *,
           -- Generate a row number for each lga_code to identify duplicates.
           ROW_NUMBER() OVER (PARTITION BY lga_code) AS row_num
    FROM {{ source('raw', 'nsw_lga_code') }}
)

-- Select only the necessary columns from the ranked_data where the row_num is 1 (this filters out duplicates).
SELECT 
    lga_code,
    lga_name 
FROM ranked_data
WHERE row_num = 1
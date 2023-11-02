-- Create a Common Table Expression (CTE) to rank the data based on lga_code_2016.
WITH ranked_data AS (
    SELECT *,
           -- Generate a row number for each lga_code_2016 to identify duplicates.
           ROW_NUMBER() OVER (PARTITION BY lga_code_2016) AS row_num
    FROM {{ source('raw', 'census_lga_g02') }}
)

-- Select all columns from the ranked_data where the row_num is 1 (this filters out duplicates).
SELECT 
    lga_code_2016,
    median_age_persons,
    median_mortgage_repay_monthly,
    median_tot_prsnl_inc_weekly,
    median_rent_weekly,
    median_tot_fam_inc_weekly,
    average_num_psns_per_bedroom,
    median_tot_hhd_inc_weekly,
    average_household_size
FROM ranked_data
WHERE row_num = 1
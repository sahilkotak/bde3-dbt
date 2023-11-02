SELECT
    lga_code,
    lga_name
FROM {{ ref('stg_nsw_lga_code') }}
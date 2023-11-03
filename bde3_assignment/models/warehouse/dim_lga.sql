SELECT
    lga_code,
    lga_name
FROM {{ ref('nsw_lga_code_stg') }}
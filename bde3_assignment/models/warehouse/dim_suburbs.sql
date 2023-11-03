SELECT DISTINCT
    suburb_name
FROM {{ ref('nsw_lga_suburb_stg') }}
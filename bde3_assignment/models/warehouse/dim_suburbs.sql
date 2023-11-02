SELECT DISTINCT
    suburb_name
FROM {{ ref('stg_nsw_lga_suburb') }}
{{ config(materialized='table') }}

WITH source_data AS (
    SELECT
        listing_id,
        property_type,
        MAX(scraped_date) as latest_scraped_date
    FROM {{ ref('stg_property') }}
    GROUP BY listing_id, property_type
)

SELECT
    ROW_NUMBER() OVER () as property_type_id, -- Creates a unique ID for each property type
    property_type
FROM source_data
GROUP BY property_type

{{ config(materialized='table') }}

WITH source_data AS (
    SELECT
        listing_id,
        room_type,
        accommodates,
        MAX(scraped_date) as latest_scraped_date
    FROM {{ ref('stg_room') }}
    GROUP BY listing_id, room_type, accommodates
)

SELECT
    ROW_NUMBER() OVER () as room_type_id, -- Creates a unique ID for each room type
    room_type,
    accommodates
FROM source_data
GROUP BY room_type, accommodates
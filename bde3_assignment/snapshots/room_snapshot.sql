-- This is a snapshot of the room data
{% snapshot room_snapshot %}

{{
      config(
      target_schema='raw',
      strategy='timestamp',
      updated_at='"SCRAPED_DATE"',
      unique_key='"LISTING_ID"'
    )
}}

-- The SELECT statement below fetches the necessary columns from the 'listings' source
SELECT
    "LISTING_ID",
    "ROOM_TYPE",
    "ACCOMMODATES",
    "PRICE",
    "HAS_AVAILABILITY",
    "AVAILABILITY_30",
    "SCRAPED_DATE"
FROM {{ source('raw', 'listings') }} -- The data is fetched from the 'listings' source in the 'raw' schema

{% endsnapshot %}
-- This is a snapshot of the property data
{% snapshot property_snapshot %}

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
    "LISTING_ID", -- The unique identifier for each listing
    "PROPERTY_TYPE", -- The type of property (e.g., house, apartment, etc.)
    "LISTING_NEIGHBOURHOOD", -- The neighbourhood where the property is located
    "HOST_ID", -- The unique identifier for the host of the listing
    "SCRAPED_DATE" -- The date when the data was scraped
FROM {{ source('raw', 'listings') }} -- The data is fetched from the 'listings' source in the 'raw' schema

{% endsnapshot %}
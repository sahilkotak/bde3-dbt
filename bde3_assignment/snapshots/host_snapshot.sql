-- This is a snapshot of the host data
{% snapshot host_snapshot %}

{{
    config(
      target_schema='raw',
      strategy='timestamp',
      updated_at='"SCRAPED_DATE"',
      unique_key='host_listing_key'
    )
}}

-- The SELECT statement below fetches the necessary columns from the 'listings' source
SELECT
    "HOST_ID", -- The unique identifier for each host
    "LISTING_ID", -- The unique identifier for each listing
    "HOST_ID" || '_' || "LISTING_ID" AS host_listing_key, -- Creating a new column that concatenates HOST_ID and LISTING_ID
    "HOST_NAME", -- The name of the host
    "HOST_SINCE", -- The date when the host joined the platform
    "HOST_IS_SUPERHOST", -- Whether the host is a superhost or not
    "HOST_NEIGHBOURHOOD", -- The neighbourhood where the host is located
    "SCRAPED_DATE" -- The date when the data was scraped
FROM {{ source('raw', 'listings') }} -- The data is fetched from the 'listings' source in the 'raw' schema

{% endsnapshot %}
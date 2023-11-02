-- This is a snapshot of the reviews data
{% snapshot reviews_snapshot %}

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
    "NUMBER_OF_REVIEWS",
    "REVIEW_SCORES_RATING",
    "REVIEW_SCORES_ACCURACY",
    "REVIEW_SCORES_CLEANLINESS",
    "REVIEW_SCORES_CHECKIN",
    "REVIEW_SCORES_COMMUNICATION",
    "REVIEW_SCORES_VALUE",
    "SCRAPED_DATE"
FROM {{ source('raw', 'listings') }} -- The data is fetched from the 'listings' source in the 'raw' schema

{% endsnapshot %}
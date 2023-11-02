WITH ranked_data AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY "LISTING_ID" 
               ORDER BY "SCRAPED_DATE" DESC
           ) as row_num
    FROM raw.reviews_snapshot
)

SELECT
    CAST("LISTING_ID" AS BIGINT) AS listing_id,
    COALESCE(CAST("NUMBER_OF_REVIEWS" AS DECIMAL(10,2)), 0) AS number_of_reviews,
    COALESCE(CAST("REVIEW_SCORES_RATING" AS DECIMAL(10,2)), 0) AS review_scores_rating,
    COALESCE(CAST("REVIEW_SCORES_ACCURACY" AS DECIMAL(10,2)), 0) AS review_scores_accuracy,
    COALESCE(CAST("REVIEW_SCORES_CLEANLINESS" AS DECIMAL(10,2)), 0) AS review_scores_cleanliness,
    COALESCE(CAST("REVIEW_SCORES_CHECKIN" AS DECIMAL(10,2)), 0) AS review_scores_checkin,
    COALESCE(CAST("REVIEW_SCORES_COMMUNICATION" AS DECIMAL(10,2)), 0) AS review_scores_communication,
    COALESCE(CAST("REVIEW_SCORES_VALUE" AS DECIMAL(10,2)), 0) AS review_scores_value,
    CASE 
        WHEN TO_DATE("SCRAPED_DATE", 'YYYY/MM/DD') <= CURRENT_DATE THEN TO_DATE("SCRAPED_DATE", 'YYYY/MM/DD')
        ELSE NULL 
    END AS scraped_date
FROM ranked_data
WHERE 
    row_num = 1
    AND "LISTING_ID" IS NOT NULL

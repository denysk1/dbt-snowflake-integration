SELECT *
FROM {{ ref('mart_category_analysis') }}
WHERE gross_revenue < 0
LIMIT 10

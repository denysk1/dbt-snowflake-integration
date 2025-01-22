SELECT *
FROM {{ ref('mart_category_analysis') }}
WHERE avg_selling_price > avg_base_price
   OR avg_selling_price <= 0
   OR avg_base_price <= 0

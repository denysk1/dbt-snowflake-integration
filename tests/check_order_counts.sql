SELECT *
FROM {{ ref('mart_category_analysis') }}
WHERE total_orders < completed_orders 
   OR total_orders < paid_orders
   OR completed_orders < paid_orders

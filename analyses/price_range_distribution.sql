USE DATABASE DEV_ECOMMERCE_DB;

WITH price_ranges AS (
    SELECT 
        category_name,
        CASE 
            WHEN avg_base_price < 50 THEN 'Under $50'
            WHEN avg_base_price < 100 THEN '$50 - $99'
            WHEN avg_base_price < 200 THEN '$100 - $199'
            ELSE '$200 and above'
        END as price_range,
        COUNT(*) as category_count,
        SUM(total_orders) as total_orders,
        SUM(gross_revenue) as total_revenue
    FROM MART.MART_CATEGORY_ANALYSIS
    GROUP BY 1, 2
)
SELECT 
    price_range,
    COUNT(DISTINCT category_name) as number_of_categories,
    SUM(total_orders) as total_orders,
    SUM(total_revenue) as total_revenue,
    ROUND(AVG(total_revenue/NULLIF(total_orders, 0)), 2) as avg_order_value
FROM price_ranges
GROUP BY price_range
ORDER BY 
    CASE price_range
        WHEN 'Under $50' THEN 1
        WHEN '$50 - $99' THEN 2
        WHEN '$100 - $199' THEN 3
        ELSE 4
    END

USE DATABASE DEV_ECOMMERCE_DB;

SELECT 
    category_name,
    SUM(gross_revenue) as total_revenue,
    SUM(total_orders) as total_orders,
    AVG(avg_selling_price) as avg_price,
    ROUND(AVG(margin_percentage), 2) as avg_margin_percentage
FROM MART.MART_CATEGORY_ANALYSIS
GROUP BY category_name
ORDER BY total_revenue DESC
LIMIT 10

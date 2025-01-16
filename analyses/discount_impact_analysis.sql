USE DATABASE DEV_ECOMMERCE_DB;

SELECT 
    category_name,
    ROUND(AVG(avg_discount_percentage), 2) as avg_discount,
    SUM(net_revenue) as total_net_revenue,
    SUM(total_discounts) as total_discounts,
    ROUND(SUM(total_discounts) / NULLIF(SUM(net_revenue), 0) * 100, 2) as discount_to_revenue_ratio
FROM MART.MART_CATEGORY_ANALYSIS
GROUP BY category_name
HAVING total_net_revenue > 0
ORDER BY discount_to_revenue_ratio DESC

with products as (
    select * from {{ ref('dim_products') }}
),

order_items as (
    select * from {{ ref('fct_order_items') }}
),

orders as (
    select * from {{ ref('fct_orders') }}
),

inventory_metrics as (
    select
        -- Product dimensions
        p.product_sk,
        p.product_id,
        p.product_name,
        p.product_code,
        p.brand_name,
        p.category_name,
        p.product_category_path,
        p.base_price_amount,
        
        -- Stock levels
        p.current_stock_quantity as current_stock,
        p.reorder_point,
        p.needs_reorder,
        p.stock_status,
        
        -- Sales velocity
        count(distinct o.order_sk) as number_of_orders,
        sum(oi.quantity) as units_sold,
        sum(oi.total_price_amount) as total_revenue,
        sum(oi.net_price_amount) as net_revenue,
        
        -- Stock coverage
        case 
            when sum(oi.quantity) = 0 then null
            else p.current_stock_quantity / (sum(oi.quantity) / nullif(datediff('day', min(o.order_date), max(o.order_date)), 0))
        end as days_of_stock_remaining,
        
        -- Inventory health indicators
        case
            when p.current_stock_quantity = 0 then 'Out of Stock'
            when p.current_stock_quantity <= p.reorder_point then 'Low Stock'
            when p.current_stock_quantity > p.reorder_point then 'Healthy Stock'
            else 'Unknown'
        end as stock_status_calculated,
        
        -- Stock turn metrics
        case 
            when p.current_stock_quantity = 0 then null
            else (sum(oi.quantity) / nullif(p.current_stock_quantity, 0)) * 
                (365 / nullif(datediff('day', min(o.order_date), max(o.order_date)), 0))
        end as inventory_turnover_rate,
        
        -- Value metrics
        p.current_stock_quantity * p.base_price_amount as current_inventory_value,
        sum(oi.quantity * p.base_price_amount) as cost_of_goods_sold,
        
        -- Performance indicators
        sum(oi.quantity) / nullif(datediff('day', min(o.order_date), max(o.order_date)), 0) as daily_sales_velocity,
        count(distinct o.customer_sk) as unique_customers,
        
        -- Metadata
        current_timestamp() as dbt_updated_at
        
    from products p
    left join order_items oi 
        on p.product_sk = oi.product_sk
    left join orders o 
        on oi.order_sk = o.order_sk
        and o.is_cancelled = false
    group by 
        p.product_sk,
        p.product_id,
        p.product_name,
        p.product_code,
        p.brand_name,
        p.category_name,
        p.product_category_path,
        p.base_price_amount,
        p.current_stock_quantity,
        p.reorder_point,
        p.needs_reorder,
        p.stock_status
),

inventory_analysis as (
    select 
        *,
        -- Additional calculated metrics
        case
            when days_of_stock_remaining <= 7 then 'Critical'
            when days_of_stock_remaining <= 14 then 'Warning'
            when days_of_stock_remaining <= 30 then 'Watch'
            when days_of_stock_remaining > 30 then 'Healthy'
            else 'Unknown'
        end as stock_coverage_status,
        
        case
            when inventory_turnover_rate >= 12 then 'High Turn'
            when inventory_turnover_rate >= 6 then 'Medium Turn'
            when inventory_turnover_rate >= 2 then 'Low Turn'
            else 'Slow Turn'
        end as turnover_category,
        
        case
            when daily_sales_velocity = 0 and current_stock > 0 then 'Non-Moving'
            when daily_sales_velocity > 0 and current_stock = 0 then 'Stock Out'
            when daily_sales_velocity > 0 and current_stock <= reorder_point then 'Reorder'
            else 'OK'
        end as inventory_action_needed
        
    from inventory_metrics
)

select * from inventory_analysis

with products as (
    select * from {{ ref('dim_products') }}
),

order_items as (
    select * from {{ ref('fct_order_items') }}
),

orders as (
    select * from {{ ref('fct_orders') }}
),

product_metrics as (
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
        p.current_stock_quantity,
        
        -- Sales metrics
        count(distinct o.order_sk) as total_orders,
        count(distinct o.customer_sk) as unique_customers,
        sum(oi.quantity) as units_sold,
        sum(oi.total_price_amount) as total_revenue,
        sum(oi.net_price_amount) as net_revenue,
        sum(oi.discount_amount) as total_discounts,
        
        -- Price metrics
        avg(oi.unit_price_amount) as avg_selling_price,
        min(oi.unit_price_amount) as min_selling_price,
        max(oi.unit_price_amount) as max_selling_price,
        
        -- Time range
        min(o.order_date) as first_order_date,
        max(o.order_date) as last_order_date,
        datediff('day', min(o.order_date), max(o.order_date)) as selling_days,
        
        -- Calculated metrics
        sum(oi.quantity) / nullif(datediff('day', min(o.order_date), max(o.order_date)), 0) as daily_sales_velocity,
        sum(oi.total_price_amount) / nullif(count(distinct o.order_sk), 0) as revenue_per_order,
        sum(oi.quantity) / nullif(count(distinct o.order_sk), 0) as units_per_order,
        
        -- Margin metrics
        sum(oi.net_price_amount - (p.base_price_amount * oi.quantity)) as total_margin,
        (sum(oi.net_price_amount - (p.base_price_amount * oi.quantity)) / 
            nullif(sum(oi.net_price_amount), 0)) * 100 as margin_percentage,
        
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
        p.current_stock_quantity
),

product_performance as (
    select
        *,
        -- Performance categories
        case
            when units_sold = 0 then 'No Sales'
            when daily_sales_velocity >= 5 then 'High Velocity'
            when daily_sales_velocity >= 1 then 'Medium Velocity'
            else 'Low Velocity'
        end as sales_velocity_category,
        
        case
            when margin_percentage >= 40 then 'High Margin'
            when margin_percentage >= 20 then 'Medium Margin'
            when margin_percentage >= 0 then 'Low Margin'
            else 'Negative Margin'
        end as margin_category,
        
        case
            when current_stock_quantity = 0 and daily_sales_velocity > 0 then 'Out of Stock'
            when current_stock_quantity <= daily_sales_velocity * 7 then 'Critical Stock'
            when current_stock_quantity <= daily_sales_velocity * 14 then 'Low Stock'
            when daily_sales_velocity = 0 then 'Non-Moving'
            else 'Healthy Stock'
        end as stock_status,
        
        -- ABC Analysis (based on revenue)
        ntile(3) over (order by total_revenue desc) as abc_rank,
        
        -- Popularity score (based on order count)
        ntile(5) over (order by total_orders desc) as popularity_score
        
    from product_metrics
)

select 
    *,
    case 
        when abc_rank = 1 then 'A'
        when abc_rank = 2 then 'B'
        else 'C'
    end as abc_category
from product_performance

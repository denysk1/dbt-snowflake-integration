with orders as (
    select * from {{ ref('fct_orders') }}
),

order_items as (
    select * from {{ ref('fct_order_items') }}
),

products as (
    select * from {{ ref('dim_products') }}
),

daily_sales as (
    select
        -- Dimensions
        date_trunc('day', o.order_date) as date_day,
        p.category_name,
        p.brand_name,
        
        -- Sales metrics
        count(distinct o.order_id) as total_orders,
        count(distinct o.customer_sk) as unique_customers,
        sum(oi.quantity) as total_units_sold,
        
        -- Revenue metrics
        sum(oi.total_price_amount) as gross_revenue,
        sum(oi.discount_amount) as total_discounts,
        sum(oi.net_price_amount) as net_revenue,
        
        -- Average order metrics
        avg(oi.total_price_amount) as avg_order_value,
        avg(oi.quantity) as avg_units_per_order,
        
        -- Calculated metrics
        sum(oi.discount_amount) / nullif(sum(oi.total_price_amount), 0) * 100 as discount_percentage,
        
        -- Metadata
        current_timestamp() as dbt_updated_at
        
    from orders o
    inner join order_items oi 
        on o.order_sk = oi.order_sk
    inner join products p 
        on oi.product_sk = p.product_sk
    where o.is_cancelled = false
    group by 1, 2, 3
)

select * from daily_sales

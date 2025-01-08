with orders as (
    select * from {{ ref('stg_orders') }}
),

order_items as (
    select * from {{ ref('stg_order_items') }}
),

daily_metrics as (
    select
        date_trunc('day', o.created_at) as order_date,
        count(distinct o.order_id) as total_orders,
        count(distinct o.customer_id) as unique_customers,
        sum(o.total_amount) as total_revenue,
        sum(o.shipping_amount) as total_shipping_revenue,
        sum(o.tax_amount) as total_tax_amount,
        sum(o.discount_amount) as total_discount_amount,
        sum(oi.quantity) as total_items_sold,
        avg(o.total_amount) as avg_order_value,
        sum(o.total_amount) / nullif(count(distinct o.customer_id), 0) as revenue_per_customer
    from orders o
    left join order_items oi 
        on o.order_id = oi.order_id
    group by 1
)

select * from daily_metrics

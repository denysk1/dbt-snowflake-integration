with order_items as (
    select * from {{ ref('stg_order_items') }}
),

orders as (
    select * from {{ ref('stg_orders') }}
),

products as (
    select * from {{ ref('stg_products') }}
),

product_metrics as (
    select
        p.product_id,
        p.product_name,
        p.category_id,
        p.brand_name,
        p.is_digital,
        count(distinct oi.order_id) as number_of_orders,
        count(distinct o.customer_id) as number_of_unique_customers,
        sum(oi.quantity) as total_units_sold,
        sum(oi.total_price_amount) as gross_revenue,
        sum(oi.discount_amount) as total_discounts,
        sum(oi.total_price_amount - oi.discount_amount) as net_revenue,
        avg(oi.unit_price_amount) as avg_unit_price,
        p.stock_quantity as current_stock_level,
        p.reorder_point
    from products p
    left join order_items oi on p.product_id = oi.product_id
    left join orders o on oi.order_id = o.order_id
    group by 1, 2, 3, 4, 5, 13, 14
)

select * from product_metrics

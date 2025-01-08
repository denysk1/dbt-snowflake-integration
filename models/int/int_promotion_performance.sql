with order_promotions as (
    select * from {{ ref('stg_order_promotions') }}
),

orders as (
    select * from {{ ref('stg_orders') }}
),

promotions as (
    select * from {{ ref('stg_promotions') }}
),

promotion_metrics as (
    select
        p.promotion_id,
        p.promotion_code,
        p.promotion_name,
        p.discount_type,
        p.discount_value,
        count(distinct op.order_id) as total_orders_using_promotion,
        count(distinct o.customer_id) as total_customers_using_promotion,
        sum(op.discount_amount) as total_discount_amount,
        sum(o.total_amount) as total_order_value,
        avg(op.discount_amount) as avg_discount_per_order,
        sum(op.discount_amount) / nullif(sum(o.total_amount), 0) * 100 as discount_percentage_of_sales,
        min(o.created_at) as first_used_at,
        max(o.created_at) as last_used_at
    from promotions p
    left join order_promotions op on p.promotion_id = op.promotion_id
    left join orders o on op.order_id = o.order_id
    group by 1, 2, 3, 4, 5
)

select * from promotion_metrics

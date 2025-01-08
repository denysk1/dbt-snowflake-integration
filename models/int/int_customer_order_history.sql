with orders as (
    select * from {{ ref('stg_orders') }}
),

customer_orders as (
    select
        customer_id,
        count(distinct order_id) as total_orders,
        sum(total_amount) as total_lifetime_value,
        min(created_at) as first_order_date,
        max(created_at) as last_order_date,
        avg(total_amount) as avg_order_value,
        sum(case when order_status = 'cancelled' then 1 else 0 end) as total_cancelled_orders,
        sum(case when order_status = 'delivered' then 1 else 0 end) as total_delivered_orders
    from orders
    group by 1
)

select * from customer_orders

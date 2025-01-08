with customer_orders as (
    select * from {{ ref('int_customer_order_history') }}
),

customer_details as (
    select * from {{ ref('stg_customers') }}
),

customer_segments as (
    select
        cd.customer_id,
        cd.first_name,
        cd.last_name,
        cd.email_address,
        cd.region_id,
        co.total_orders,
        co.total_lifetime_value,
        co.avg_order_value,
        datediff('day', co.first_order_date, current_date()) as days_since_first_order,
        datediff('day', co.last_order_date, current_date()) as days_since_last_order,
        case
            when co.total_lifetime_value >= 1000 and co.total_orders >= 5 then 'VIP'
            when co.total_lifetime_value >= 500 or co.total_orders >= 3 then 'Regular'
            when co.total_orders = 1 then 'New'
            else 'At Risk'
        end as customer_segment,
        case
            when datediff('day', co.last_order_date, current_date()) <= 30 then 'Active'
            when datediff('day', co.last_order_date, current_date()) <= 90 then 'Recent'
            when datediff('day', co.last_order_date, current_date()) <= 180 then 'Lapsed'
            else 'Inactive'
        end as activity_status
    from customer_details cd
    left join customer_orders co on cd.customer_id = co.customer_id
)

select * from customer_segments

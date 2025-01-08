with orders as (
    select * from {{ ref('stg_orders') }}
),

customers as (
    select * from {{ ref('stg_customers') }}
),

regions as (
    select * from {{ ref('stg_regions') }}
),

regional_metrics as (
    select
        r.region_id,
        r.region_name,
        r.region_code,
        r.country_code,
        count(distinct c.customer_id) as total_customers,
        count(distinct o.order_id) as total_orders,
        count(distinct case when o.created_at >= current_date - interval '30 days' 
            then o.order_id else null end) as orders_last_30_days,
        sum(o.total_amount) as total_revenue,
        sum(case when o.created_at >= current_date - interval '30 days' 
            then o.total_amount else 0 end) as revenue_last_30_days,
        avg(o.total_amount) as avg_order_value,
        sum(o.total_amount) / nullif(count(distinct c.customer_id), 0) as revenue_per_customer
    from regions r
    left join customers c on r.region_id = c.region_id
    left join orders o on c.customer_id = o.customer_id
    where r.is_active = true
    group by 1, 2, 3, 4
)

select * from regional_metrics

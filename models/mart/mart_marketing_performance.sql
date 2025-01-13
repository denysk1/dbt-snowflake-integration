with orders as (
    select * from {{ ref('fct_orders') }}
),

order_items as (
    select * from {{ ref('fct_order_items') }}
),

products as (
    select * from {{ ref('dim_products') }}
),

daily_performance as (
    select
        -- Time dimensions
        date_trunc('day', o.order_date) as date,
        date_trunc('week', o.order_date) as week,
        date_trunc('month', o.order_date) as month,
        
        -- Order metrics
        count(distinct o.order_sk) as total_orders,
        count(distinct o.customer_sk) as unique_customers,
        sum(o.order_total_amount) as total_revenue,
        sum(o.subtotal_amount) as subtotal_revenue,
        sum(o.shipping_amount) as shipping_revenue,
        sum(o.tax_amount) as tax_revenue,
        sum(o.discount_amount) as total_discounts,
        sum(o.total_items) as total_items,
        sum(o.distinct_products) as distinct_products,
        
        -- Product performance
        count(distinct oi.product_sk) as unique_products_sold,
        sum(oi.quantity) as units_sold,
        
        -- Conversion metrics
        sum(case when o.is_completed then 1 else 0 end) as completed_orders,
        sum(case when o.is_paid then 1 else 0 end) as paid_orders,
        
        -- Value metrics
        sum(o.order_total_amount) / nullif(count(distinct o.order_sk), 0) as average_order_value,
        sum(o.order_total_amount) / nullif(count(distinct o.customer_sk), 0) as revenue_per_customer,
        sum(o.total_items) / nullif(count(distinct o.order_sk), 0) as items_per_order,
        
        -- Product category metrics
        p.category_name,
        sum(oi.quantity) as category_units_sold,
        sum(oi.total_price_amount) as category_revenue,
        
        -- Metadata
        current_timestamp() as dbt_updated_at
        
    from orders o
    inner join order_items oi 
        on o.order_sk = oi.order_sk
    inner join products p 
        on oi.product_sk = p.product_sk
    where o.is_cancelled = false
    group by 1, 2, 3, p.category_name
),

performance_analysis as (
    select
        *,
        -- Additional calculated metrics
        completed_orders::float / nullif(total_orders, 0) as completion_rate,
        paid_orders::float / nullif(total_orders, 0) as payment_rate,
        
        -- Performance categories
        case
            when total_orders >= 100 then 'High Volume'
            when total_orders >= 50 then 'Medium Volume'
            when total_orders >= 10 then 'Low Volume'
            else 'Very Low Volume'
        end as volume_category,
        
        case
            when average_order_value >= 100 then 'High Value'
            when average_order_value >= 50 then 'Medium Value'
            else 'Low Value'
        end as value_category
        
    from daily_performance
)

select * from performance_analysis

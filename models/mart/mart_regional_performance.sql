with orders as (
    select * from {{ ref('fct_orders') }}
),

order_items as (
    select * from {{ ref('fct_order_items') }}
),

products as (
    select * from {{ ref('dim_products') }}
),

shipping_rates as (
    select * from {{ ref('seed_shipping_rates') }}
),

regional_metrics as (
    select
        -- Geographical dimensions
        sr.region_code,
        sr.country_code,
        date_trunc('month', o.order_date) as sales_month,
        
        -- Order metrics
        count(distinct o.order_sk) as total_orders,
        count(distinct o.customer_sk) as unique_customers,
        sum(o.order_total_amount) as total_revenue,
        sum(o.subtotal_amount) as subtotal_revenue,
        sum(o.shipping_amount) as shipping_revenue,
        sum(o.tax_amount) as tax_amount,
        sum(o.discount_amount) as total_discounts,
        
        -- Product metrics
        count(distinct oi.product_sk) as unique_products,
        sum(o.total_items) as total_items,
        sum(o.distinct_products) as distinct_products,
        
        -- Shipping metrics
        avg(sr.base_rate) as avg_base_shipping_rate,
        avg(sr.per_kg_rate) as avg_per_kg_rate,
        avg(sr.min_delivery_days) as min_delivery_days,
        avg(sr.max_delivery_days) as max_delivery_days,
        sum(case when sr.is_express then 1 else 0 end) as express_shipments,
        sum(case when o.shipping_amount = 0 then 1 else 0 end) as free_shipping_orders,
        
        -- Order status metrics
        sum(case when o.is_completed then 1 else 0 end) as completed_orders,
        sum(case when o.is_paid then 1 else 0 end) as paid_orders,
        
        -- Value metrics
        sum(o.order_total_amount) / nullif(count(distinct o.order_sk), 0) as avg_order_value,
        sum(o.shipping_amount) / nullif(count(distinct o.order_sk), 0) as avg_shipping_cost,
        sum(o.discount_amount) / nullif(sum(o.order_total_amount), 0) * 100 as discount_rate,
        
        -- Metadata
        current_timestamp() as dbt_updated_at
        
    from orders o
    inner join order_items oi 
        on o.order_sk = oi.order_sk
    inner join products p 
        on oi.product_sk = p.product_sk
    left join shipping_rates sr 
        on o.shipping_method_sk = sr.shipping_rate_id
        and sr.is_active = true
        and o.order_date between sr.valid_from and sr.valid_to
    where o.is_cancelled = false
    group by 1, 2, 3
),

regional_performance as (
    select
        *,
        -- Performance metrics
        completed_orders::float / nullif(total_orders, 0) * 100 as completion_rate,
        paid_orders::float / nullif(total_orders, 0) * 100 as payment_rate,
        free_shipping_orders::float / nullif(total_orders, 0) * 100 as free_shipping_rate,
        express_shipments::float / nullif(total_orders, 0) * 100 as express_shipping_rate,
        
        -- Performance categories
        case
            when total_orders >= 1000 then 'High Volume'
            when total_orders >= 500 then 'Medium Volume'
            when total_orders >= 100 then 'Low Volume'
            else 'Very Low Volume'
        end as volume_category,
        
        case
            when avg_order_value >= 100 then 'High Value'
            when avg_order_value >= 50 then 'Medium Value'
            else 'Low Value'
        end as value_category,
        
        -- Regional ranking
        row_number() over (partition by sales_month order by total_revenue desc) as monthly_rank,
        percent_rank() over (partition by sales_month order by total_revenue desc) as revenue_percentile
        
    from regional_metrics
)

select * from regional_performance
order by sales_month, total_revenue desc

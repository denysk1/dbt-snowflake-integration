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

shipping_metrics as (
    select
        -- Shipping dimensions
        sr.shipping_rate_id,
        sr.region_code,
        sr.country_code,
        sr.shipping_method,
        sr.is_express,
        date_trunc('month', o.order_date) as shipping_month,
        
        -- Product dimensions
        p.category_name,
        p.product_category_path,
        
        -- Shipping metrics
        count(distinct o.order_sk) as total_shipments,
        sum(o.shipping_amount) as total_shipping_revenue,
        sum(o.order_total_amount) as total_order_value,
        sum(o.total_items) as total_items_shipped,
        
        -- Base rates
        avg(sr.base_rate) as avg_base_rate,
        avg(sr.per_kg_rate) as avg_per_kg_rate,
        avg(sr.min_delivery_days) as avg_min_delivery_days,
        avg(sr.max_delivery_days) as avg_max_delivery_days,
        
        -- Free shipping metrics
        sum(case when o.shipping_amount = 0 then 1 else 0 end) as free_shipping_orders,
        sum(case when o.shipping_amount = 0 then o.order_total_amount else 0 end) as free_shipping_order_value,
        
        -- Express shipping metrics
        sum(case when sr.is_express then 1 else 0 end) as express_shipments,
        sum(case when sr.is_express then o.shipping_amount else 0 end) as express_shipping_revenue,
        
        -- Order status metrics
        sum(case when o.is_completed then 1 else 0 end) as completed_orders,
        sum(case when o.is_paid then 1 else 0 end) as paid_orders,
        
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
    group by 1, 2, 3, 4, 5, 6, 7, 8
),

shipping_analysis as (
    select
        *,
        -- Calculated metrics
        total_shipping_revenue / nullif(total_shipments, 0) as avg_shipping_cost,
        total_order_value / nullif(total_shipments, 0) as avg_order_value,
        total_items_shipped / nullif(total_shipments, 0) as avg_items_per_shipment,
        completed_orders::float / nullif(total_shipments, 0) * 100 as completion_rate,
        paid_orders::float / nullif(total_shipments, 0) * 100 as payment_rate,
        free_shipping_orders::float / nullif(total_shipments, 0) * 100 as free_shipping_rate,
        express_shipments::float / nullif(total_shipments, 0) * 100 as express_shipping_rate,
        
        -- Performance categories
        case
            when total_shipments >= 1000 then 'High Volume'
            when total_shipments >= 500 then 'Medium Volume'
            when total_shipments >= 100 then 'Low Volume'
            else 'Very Low Volume'
        end as volume_category,
        
        -- Shipping cost analysis
        case
            when total_shipping_revenue / nullif(total_order_value, 0) * 100 >= 15 then 'High Cost'
            when total_shipping_revenue / nullif(total_order_value, 0) * 100 >= 10 then 'Medium Cost'
            else 'Low Cost'
        end as shipping_cost_category,
        
        -- Express shipping analysis
        case
            when express_shipping_rate >= 50 then 'High Express Usage'
            when express_shipping_rate >= 25 then 'Medium Express Usage'
            else 'Low Express Usage'
        end as express_shipping_category,
        
        -- Rankings
        row_number() over (partition by shipping_month order by total_shipping_revenue desc) as revenue_rank,
        percent_rank() over (partition by shipping_month order by total_shipments desc) as volume_percentile
        
    from shipping_metrics
)

select * from shipping_analysis
order by shipping_month, total_shipping_revenue desc

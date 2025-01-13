with order_items as (
    select * from {{ ref('fct_order_items') }}
),

orders as (
    select * from {{ ref('fct_orders') }}
),

products as (
    select * from {{ ref('dim_products') }}
),

shipping_rates as (
    select * from {{ ref('seed_shipping_rates') }}
),

category_metrics as (
    select
        -- Category dimensions
        p.category_sk,
        p.category_name,
        p.product_category_path,
        date_trunc('month', o.order_date) as sales_month,
        sr.region_code,
        sr.country_code,
        
        -- Sales metrics
        count(distinct o.order_id) as total_orders,
        count(distinct o.customer_sk) as unique_customers,
        sum(oi.quantity) as units_sold,
        
        -- Revenue metrics
        sum(o.order_total_amount) as gross_revenue,
        sum(o.discount_amount) as total_discounts,
        sum(o.subtotal_amount) as net_revenue,
        sum(o.shipping_amount) as shipping_revenue,
        sum(o.tax_amount) as total_tax,
        
        -- Margin metrics
        sum(oi.net_price_amount - (p.base_price_amount * oi.quantity)) as gross_margin,
        (sum(oi.net_price_amount - (p.base_price_amount * oi.quantity)) / 
            nullif(sum(oi.net_price_amount), 0)) * 100 as margin_percentage,
        
        -- Category performance
        avg(p.base_price_amount) as avg_base_price,
        sum(oi.net_price_amount) / nullif(sum(oi.quantity), 0) as avg_selling_price,
        avg(p.discount_percentage) as avg_discount_percentage,
        
        -- Order metrics
        avg(o.distinct_products) as avg_products_per_order,
        avg(o.total_items) as avg_items_per_order,
        sum(case when o.is_completed then 1 else 0 end) as completed_orders,
        sum(case when o.is_paid then 1 else 0 end) as paid_orders,
        
        -- Inventory metrics
        sum(p.current_stock_quantity) as total_stock_quantity,
        sum(case when p.needs_reorder then 1 else 0 end) as products_needing_reorder,
        
        -- Shipping metrics
        sum(case when o.shipping_amount = 0 then 1 else 0 end) as free_shipping_orders,
        avg(o.shipping_amount) as avg_shipping_amount,
        avg(sr.base_rate) as base_shipping_rate,
        avg(sr.per_kg_rate) as per_kg_rate,
        avg(sr.min_delivery_days) as min_delivery_days,
        avg(sr.max_delivery_days) as max_delivery_days,
        sum(case when sr.is_express then 1 else 0 end) as express_shipments,
        
        -- Target metrics
        30 as target_margin_percentage,
        case
            when (sum(oi.net_price_amount - (p.base_price_amount * oi.quantity)) / 
                nullif(sum(oi.net_price_amount), 0)) * 100 >= 30
            then true
            else false
        end as is_meeting_margin_target,
        
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
    group by 1, 2, 3, 4, 5, 6
)

select * from category_metrics

with orders as (
    select * from {{ ref('fct_orders') }}
),

customer_metrics as (
    select
        customer_sk,
        count(distinct order_sk) as total_orders,
        sum(order_total_amount) as total_revenue,
        sum(subtotal_amount) as subtotal_revenue,
        sum(shipping_amount) as shipping_revenue,
        sum(tax_amount) as tax_amount,
        sum(discount_amount) as discount_amount,
        sum(total_items) as total_items,
        sum(distinct_products) as total_distinct_products,
        min(order_date) as first_purchase_date,
        max(order_date) as last_purchase_date,
        datediff('day', min(order_date), max(order_date)) as customer_lifetime_days,
        sum(case when is_paid then 1 else 0 end) as paid_orders,
        sum(case when shipping_amount = 0 then 1 else 0 end) as free_shipping_orders,
        avg(order_total_amount) as avg_order_value,
        sum(order_total_amount) / nullif(count(distinct order_sk), 0) as revenue_per_order,
        sum(total_items) / nullif(count(distinct order_sk), 0) as items_per_order
    from orders
    where is_cancelled = false
    group by 1
),

customer_segments as (
    select
        customer_sk,
        total_orders,
        total_revenue,
        subtotal_revenue,
        shipping_revenue,
        tax_amount,
        discount_amount,
        total_items,
        total_distinct_products,
        first_purchase_date,
        last_purchase_date,
        customer_lifetime_days,
        paid_orders,
        free_shipping_orders,
        avg_order_value,
        revenue_per_order,
        items_per_order,
        
        -- RFM Scores (1-5 scale)
        ntile(5) over (order by last_purchase_date) as recency_score,
        ntile(5) over (order by total_orders) as frequency_score,
        ntile(5) over (order by total_revenue) as monetary_score,
        
        -- Customer Value Segments
        case
            when total_revenue > percentile_cont(0.9) within group (order by total_revenue) over () 
                then 'High Value'
            when total_revenue > percentile_cont(0.7) within group (order by total_revenue) over () 
                then 'Mid-High Value'
            when total_revenue > percentile_cont(0.5) within group (order by total_revenue) over () 
                then 'Mid Value'
            when total_revenue > percentile_cont(0.3) within group (order by total_revenue) over () 
                then 'Mid-Low Value'
            else 'Low Value'
        end as value_segment,
        
        -- Purchase Frequency Segments
        case
            when total_orders >= 5 then 'Very Frequent'
            when total_orders >= 3 then 'Frequent'
            when total_orders = 2 then 'Repeat Customer'
            else 'One-Time Customer'
        end as frequency_segment,
        
        -- Recency Segments
        case
            when datediff('day', last_purchase_date, current_date()) <= 30 then 'Active'
            when datediff('day', last_purchase_date, current_date()) <= 90 then 'Recent'
            when datediff('day', last_purchase_date, current_date()) <= 180 then 'Lapsed'
            else 'Inactive'
        end as recency_segment,
        
        -- Combined RFM Segment
        concat(
            ntile(5) over (order by last_purchase_date),
            ntile(5) over (order by total_orders),
            ntile(5) over (order by total_revenue)
        ) as rfm_score,
        
        -- Metadata
        current_timestamp() as dbt_updated_at
        
    from customer_metrics
)

select * from customer_segments

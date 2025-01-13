with orders as (
    select * from {{ ref('fct_orders') }}
),

first_purchases as (
    select
        customer_sk,
        min(order_date) as first_purchase_date,
        date_trunc('month', min(order_date)) as cohort_month
    from orders
    where is_cancelled = false
    group by 1
),

customer_orders as (
    select
        o.customer_sk,
        fp.cohort_month,
        date_trunc('month', o.order_date) as order_month,
        datediff('month', fp.cohort_month, date_trunc('month', o.order_date)) as months_since_first_purchase,
        count(distinct o.order_sk) as number_of_orders,
        sum(o.order_total_amount) as total_revenue,
        sum(o.subtotal_amount) as subtotal_revenue,
        sum(o.shipping_amount) as shipping_revenue,
        sum(o.tax_amount) as tax_amount,
        sum(o.discount_amount) as discount_amount,
        sum(o.total_items) as total_items,
        sum(o.distinct_products) as distinct_products,
        sum(case when o.is_paid then 1 else 0 end) as paid_orders,
        sum(case when o.shipping_amount = 0 then 1 else 0 end) as free_shipping_orders
    from orders o
    inner join first_purchases fp 
        on o.customer_sk = fp.customer_sk
    where o.is_cancelled = false
    group by 1, 2, 3, 4
),

cohort_size as (
    select
        cohort_month,
        count(distinct customer_sk) as customers_in_cohort
    from first_purchases
    group by 1
),

cohort_analysis as (
    select
        co.cohort_month,
        co.order_month,
        co.months_since_first_purchase,
        cs.customers_in_cohort,
        count(distinct co.customer_sk) as active_customers,
        sum(co.number_of_orders) as total_orders,
        sum(co.total_revenue) as total_revenue,
        sum(co.subtotal_revenue) as subtotal_revenue,
        sum(co.shipping_revenue) as shipping_revenue,
        sum(co.tax_amount) as tax_amount,
        sum(co.discount_amount) as discount_amount,
        sum(co.total_items) as total_items,
        sum(co.distinct_products) as distinct_products,
        sum(co.paid_orders) as paid_orders,
        sum(co.free_shipping_orders) as free_shipping_orders,
        
        -- Metrics per customer
        sum(co.total_revenue) / nullif(count(distinct co.customer_sk), 0) as revenue_per_customer,
        sum(co.number_of_orders) / nullif(count(distinct co.customer_sk), 0) as orders_per_customer,
        sum(co.total_items) / nullif(sum(co.number_of_orders), 0) as items_per_order,
        
        -- Retention metrics
        count(distinct co.customer_sk) / nullif(cs.customers_in_cohort, 0) * 100 as retention_rate,
        
        -- Average values
        sum(co.total_revenue) / nullif(sum(co.number_of_orders), 0) as average_order_value,
        sum(co.discount_amount) / nullif(sum(co.total_revenue), 0) * 100 as discount_rate,
        
        -- Metadata
        current_timestamp() as dbt_updated_at
        
    from customer_orders co
    inner join cohort_size cs 
        on co.cohort_month = cs.cohort_month
    group by 1, 2, 3, 4
)

select * from cohort_analysis
order by cohort_month, months_since_first_purchase

with orders as (
    select * from {{ ref('stg_orders') }}
),

order_items as (
    select * from {{ ref('stg_order_items') }}
),

customers as (
    select * from {{ ref('dim_customers') }}
),

products as (
    select * from {{ ref('dim_products') }}
),

promotions as (
    select * from {{ ref('dim_promotions') }}
),

order_status as (
    select * from {{ ref('dim_order_status') }}
),

shipping_methods as (
    select * from {{ ref('dim_shipping_methods') }}
),

final as (
    select
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['o.order_id']) }} as order_sk,
        
        -- Natural key
        o.order_id,
        
        -- Foreign keys (surrogate keys from dimensions)
        c.customer_sk,
        os.order_status_sk,
        sm.shipping_method_sk,
        
        -- Dates and timestamps
        o.created_at as order_date,
        o.updated_at as last_updated_date,
        
        -- Order amounts
        o.total_amount as order_total_amount,
        o.subtotal_amount,
        o.tax_amount,
        o.shipping_amount,
        o.discount_amount,
        
        -- Order metrics
        count(distinct oi.product_id) as distinct_products,
        sum(oi.quantity) as total_items,
        
        -- Status flags
        os.is_completed,
        os.is_cancelled,
        os.is_paid,
        
        -- Tracking
        current_timestamp() as dbt_updated_at
        
    from orders o
    inner join customers c 
        on o.customer_id = c.customer_id
    left join order_items oi 
        on o.order_id = oi.order_id
    left join order_status os 
        on o.order_status = os.order_status 
        and o.payment_status = os.payment_status
    left join shipping_methods sm 
        on o.shipping_method = sm.shipping_method
    group by 1,2,3,4,5,6,7,8,9,10,11,12,15,16,17,18
)

select * from final

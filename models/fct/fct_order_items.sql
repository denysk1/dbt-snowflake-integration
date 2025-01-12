with order_items as (
    select * from {{ ref('stg_order_items') }}
),

orders as (
    select * from {{ ref('fct_orders') }}
),

products as (
    select * from {{ ref('dim_products') }}
),

final as (
    select
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['oi.order_item_id']) }} as order_item_sk,
        
        -- Natural keys
        oi.order_item_id,
        oi.order_id,
        
        -- Foreign keys (surrogate keys from dimensions)
        o.order_sk,
        p.product_sk,
        
        -- Item details
        oi.quantity,
        oi.unit_price_amount,
        oi.discount_amount,
        oi.total_price_amount,
        
        -- Derived metrics
        (oi.total_price_amount - oi.discount_amount) as net_price_amount,
        (oi.total_price_amount - oi.discount_amount) / nullif(oi.quantity, 0) as net_unit_price,
        
        -- Tracking
        current_timestamp() as dbt_updated_at
        
    from order_items oi
    inner join orders o 
        on oi.order_id = o.order_id
    inner join products p 
        on oi.product_id = p.product_id
)

select * from final

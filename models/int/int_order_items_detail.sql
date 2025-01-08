with order_items as (
    select * from {{ ref('stg_order_items') }}
),

products as (
    select * from {{ ref('stg_products') }}
),

categories as (
    select * from {{ ref('stg_product_categories') }}
),

enriched_order_items as (
    select
        oi.order_item_id,
        oi.order_id,
        oi.product_id,
        p.product_name,
        p.category_id,
        c.category_name,
        oi.quantity,
        oi.unit_price_amount,
        oi.discount_amount,
        oi.total_price_amount,
        (oi.total_price_amount - oi.discount_amount) as net_price_amount,
        p.is_digital
    from order_items oi
    left join products p 
        on oi.product_id = p.product_id
    left join categories c 
        on p.category_id = c.category_id
)

select * from enriched_order_items

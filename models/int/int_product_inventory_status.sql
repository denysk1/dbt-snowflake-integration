with products as (
    select * from {{ ref('stg_products') }}
),

order_items as (
    select 
        product_id,
        sum(quantity) as units_ordered
    from {{ ref('stg_order_items') }}
    group by 1
),

inventory_status as (
    select
        p.product_id,
        p.product_name,
        p.category_id,
        p.stock_quantity as current_stock,
        p.reorder_point,
        coalesce(oi.units_ordered, 0) as total_units_ordered,
        case
            when p.stock_quantity <= p.reorder_point then true
            else false
        end as needs_reorder,
        case
            when p.stock_quantity = 0 then 'out_of_stock'
            when p.stock_quantity <= p.reorder_point then 'low_stock'
            when p.stock_quantity <= (p.reorder_point * 2) then 'medium_stock'
            else 'healthy_stock'
        end as stock_status,
        p.is_active
    from products p
    left join order_items oi on p.product_id = oi.product_id
)

select * from inventory_status

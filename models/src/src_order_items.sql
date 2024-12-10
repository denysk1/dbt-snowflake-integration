with source as (
    select * from {{ source('raw', 'order_items') }}
),
renamed as (
    select
        order_item_id,
        order_id,
        product_id,
        quantity,
        unit_price_cents,
        discount_cents,
        total_price_cents,
        is_gift,
        created_at,
        updated_at
    from source
)
select * from renamed

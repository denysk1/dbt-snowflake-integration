with source as (
    select * from {{ source('raw', 'products') }}
),
renamed as (
    select
        product_id,
        product_name,
        product_code,
        category_id,
        description,
        base_price_cents,
        discount_price_cents,
        brand,
        weight_grams,
        is_digital,
        stock_quantity,
        reorder_point,
        is_active,
        created_at,
        updated_at
    from source
)
select * from renamed

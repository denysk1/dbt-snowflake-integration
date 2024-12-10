with source as (
    select * from {{ source('raw', 'order_promotions') }}
),
renamed as (
    select
        order_id,
        promotion_id,
        discount_amount_cents,
        created_at
    from source
)
select * from renamed

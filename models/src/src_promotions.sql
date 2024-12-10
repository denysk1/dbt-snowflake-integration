with source as (
    select * from {{ source('raw', 'promotions') }}
),
renamed as (
    select
        promotion_id,
        promotion_code,
        promotion_name,
        description,
        discount_type,
        discount_value,
        minimum_order_cents,
        start_date,
        end_date,
        is_active,
        created_at,
        updated_at
    from source
)
select * from renamed

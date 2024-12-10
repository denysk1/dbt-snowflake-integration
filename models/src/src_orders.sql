with source as (
    select * from {{ source('raw', 'orders') }}
),
renamed as (
    select
        order_id,
        customer_id,
        order_date,
        order_status,
        payment_status,
        payment_method,
        shipping_method,
        currency_code,
        subtotal_cents,
        shipping_amount_cents,
        tax_amount_cents,
        discount_amount_cents,
        total_amount_cents,
        estimated_delivery_date,
        actual_delivery_date,
        created_at,
        updated_at
    from source
)
select * from renamed

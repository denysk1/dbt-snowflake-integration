with source as (
    select * from {{ ref('src_orders') }}
),

staged as (
    select
        -- Primary keys
        order_id,
        
        -- Foreign keys
        customer_id,
        
        -- Order status attributes
        trim(order_status) as order_status,
        trim(payment_status) as payment_status,
        trim(payment_method) as payment_method,
        trim(shipping_method) as shipping_method,
        upper(trim(currency_code)) as currency_code,
        
        -- Monetary values standardization (cents to dollars)
        subtotal_cents / 100.0 as subtotal_amount,
        shipping_amount_cents / 100.0 as shipping_amount,
        tax_amount_cents / 100.0 as tax_amount,
        discount_amount_cents / 100.0 as discount_amount,
        total_amount_cents / 100.0 as total_amount,
        
        -- Timestamps standardization
        convert_timezone('UTC', order_date) as order_date,
        convert_timezone('UTC', estimated_delivery_date) as estimated_delivery_date,
        convert_timezone('UTC', actual_delivery_date) as actual_delivery_date,
        convert_timezone('UTC', created_at) as created_at,
        convert_timezone('UTC', updated_at) as updated_at,
        
        -- Audit columns
        current_timestamp() as stg_loaded_at,
        '{{ invocation_id }}' as _dbt_job_id
    from source
)

select * from staged

with source as (
    select * from {{ ref('src_order_items') }}
),

staged as (
    select
        -- Primary keys
        order_item_id,
        
        -- Foreign keys
        order_id,
        product_id,
        
        -- Quantities
        quantity,
        
        -- Monetary values standardization (cents to dollars)
        unit_price_cents / 100.0 as unit_price_amount,
        discount_cents / 100.0 as discount_amount,
        total_price_cents / 100.0 as total_price_amount,
        
        -- Boolean standardization
        case 
            when is_gift = 1 then true
            when is_gift = 0 then false
            else null 
        end as is_gift,
        
        -- Timestamps standardization
        convert_timezone('UTC', created_at) as created_at,
        convert_timezone('UTC', updated_at) as updated_at,
        
        -- Audit columns
        current_timestamp() as stg_loaded_at,
        '{{ invocation_id }}' as _dbt_job_id
    from source
)

select * from staged

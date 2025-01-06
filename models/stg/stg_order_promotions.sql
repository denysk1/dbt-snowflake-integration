with source as (
    select * from {{ ref('src_order_promotions') }}
),

staged as (
    select
        -- Foreign keys (composite key)
        order_id,
        promotion_id,
        
        -- Monetary values standardization
        discount_amount_cents / 100.0 as discount_amount,
        
        -- Timestamps standardization
        convert_timezone('UTC', created_at) as created_at,
        
        -- Audit columns
        current_timestamp() as stg_loaded_at,
        '{{ invocation_id }}' as _dbt_job_id
    from source
)

select * from staged

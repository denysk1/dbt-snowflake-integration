with source as (
    select * from {{ ref('src_promotions') }}
),

staged as (
    select
        -- Primary key
        promotion_id,
        
        -- Promotion attributes
        trim(promotion_code) as promotion_code,
        trim(promotion_name) as promotion_name,
        trim(description) as promotion_description,
        trim(discount_type) as discount_type,
        discount_value,
        
        -- Monetary values standardization
        minimum_order_cents / 100.0 as minimum_order_amount,
        
        -- Timestamps standardization
        convert_timezone('UTC', start_date) as start_date,
        convert_timezone('UTC', end_date) as end_date,
        convert_timezone('UTC', created_at) as created_at,
        convert_timezone('UTC', updated_at) as updated_at,
        
        -- Boolean standardization
        case 
            when is_active = 1 then true
            when is_active = 0 then false
            else null 
        end as is_active,
        
        -- Audit columns
        current_timestamp() as stg_loaded_at,
        '{{ invocation_id }}' as _dbt_job_id
    from source
)

select * from staged

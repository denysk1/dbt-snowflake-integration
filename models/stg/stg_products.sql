with source as (
    select * from {{ ref('src_products') }}
),

staged as (
    select
        -- Primary key
        product_id,
        
        -- Foreign keys
        category_id,
        
        -- Product attributes
        trim(product_name) as product_name,
        trim(product_code) as product_code,
        trim(description) as product_description,
        trim(brand) as brand_name,
        
        -- Monetary values standardization (cents to dollars)
        base_price_cents / 100.0 as base_price_amount,
        discount_price_cents / 100.0 as discount_price_amount,
        
        -- Physical attributes
        weight_grams,
        
        -- Boolean standardization
        case 
            when is_digital = 1 then true
            when is_digital = 0 then false
            else null 
        end as is_digital,
        
        case 
            when is_active = 1 then true
            when is_active = 0 then false
            else null 
        end as is_active,
        
        -- Inventory attributes
        stock_quantity,
        reorder_point,
        
        -- Timestamps standardization
        convert_timezone('UTC', created_at) as created_at,
        convert_timezone('UTC', updated_at) as updated_at,
        
        -- Audit columns
        current_timestamp() as stg_loaded_at,
        '{{ invocation_id }}' as _dbt_job_id
    from source
)

select * from staged

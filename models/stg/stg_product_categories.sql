with source as (
    select * from {{ ref('src_product_categories') }}
),

staged as (
    select
        -- Primary key
        category_id,
        
        -- Foreign keys (self-referential)
        parent_category_id,
        
        -- Category attributes
        trim(category_name) as category_name,
        trim(category_code) as category_code,
        trim(category_description) as category_description,
        
        -- Boolean standardization
        case 
            when is_active = 1 then true
            when is_active = 0 then false
            else null 
        end as is_active,
        
        -- Timestamps standardization
        convert_timezone('UTC', created_at) as created_at,
        convert_timezone('UTC', updated_at) as updated_at,
        
        -- Audit columns
        current_timestamp() as stg_loaded_at,
        '{{ invocation_id }}' as _dbt_job_id
    from source
)

select * from staged

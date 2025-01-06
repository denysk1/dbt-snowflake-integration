with source as (
    select * from {{ ref('src_regions') }}
),

staged as (
    select
        -- Primary key
        region_id,
        
        -- Region attributes
        trim(region_name) as region_name,
        upper(trim(region_code)) as region_code,
        upper(trim(country_code)) as country_code,
        
        -- Boolean standardization
        case 
            when is_active = 1 then true
            when is_active = 0 then false
            else null 
        end as is_active,
        
        -- Validity period timestamps
        convert_timezone('UTC', valid_from) as valid_from,
        convert_timezone('UTC', valid_to) as valid_to,
        
        -- Audit columns
        current_timestamp() as stg_loaded_at,
        '{{ invocation_id }}' as _dbt_job_id
    from source
)

select * from staged

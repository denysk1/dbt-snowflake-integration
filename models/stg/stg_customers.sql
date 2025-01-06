with source as (
    select * from {{ ref('src_customers') }}
),

staged as (
    select
        -- Primary keys
        customer_id,
        
        -- Customer attributes
        trim(first_name) as first_name,
        trim(last_name) as last_name,
        lower(trim(email)) as email_address,
        regexp_replace(phone, '[^0-9]', '') as phone_number,
        
        -- Foreign keys
        region_id,
        
        -- Address attributes
        trim(address_line1) as address_line1,
        trim(address_line2) as address_line2,
        trim(city) as city,
        trim(state) as state,
        trim(postal_code) as postal_code,
        upper(trim(country)) as country_code,
        
        -- Customer categorization
        trim(customer_segment) as customer_segment,
        
        -- Boolean standardization
        case 
            when is_active = 1 then true
            when is_active = 0 then false
            else null 
        end as is_active,
        
        -- Timestamps standardization
        cast(date_of_birth as date) as birth_date,
        convert_timezone('UTC', created_at) as created_at,
        convert_timezone('UTC', updated_at) as updated_at,
        convert_timezone('UTC', last_login_at) as last_login_at,
        
        -- Audit columns
        current_timestamp() as stg_loaded_at,
        '{{ invocation_id }}' as _dbt_job_id
    from source
)

select * from staged

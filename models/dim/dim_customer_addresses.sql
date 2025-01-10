with customer_addresses as (
    select * from {{ ref('stg_customers') }}
),

final as (
    select
        -- Surrogate and natural keys
        {{ dbt_utils.generate_surrogate_key(['customer_id', 'address_line1', 'address_line2', 'city', 'state', 'postal_code']) }} as address_sk,
        customer_id,
        
        -- Address details
        address_line1,
        address_line2,
        city,
        state,
        postal_code,
        country_code,
        
        -- Metadata
        created_at as valid_from,
        coalesce(
            lead(created_at) over (
                partition by customer_id 
                order by created_at
            ),
            '9999-12-31'
        ) as valid_to,
        current_timestamp() as dbt_updated_at
    from customer_addresses
)

select * from final

with customers as (
    select * from {{ ref('stg_customers') }}
),

customer_segments as (
    select * from {{ ref('int_customer_segmentation') }}
),

regions as (
    select * from {{ ref('dim_regions') }}
),

final as (
    select
        -- Surrogate and natural keys
        {{ dbt_utils.generate_surrogate_key(['c.customer_id']) }} as customer_sk,
        c.customer_id,
        
        -- Customer attributes
        c.first_name,
        c.last_name,
        c.email_address,
        c.phone_number,
        
        -- Address information
        c.address_line1,
        c.address_line2,
        c.city,
        c.state,
        c.postal_code,
        c.country_code,
        
        -- Region references
        r.region_sk,
        r.region_name,
        r.region_code,
        
        -- Customer segmentation
        cs.customer_segment,
        cs.activity_status,
        cs.total_lifetime_value,
        cs.total_orders,
        cs.avg_order_value,
        cs.days_since_first_order,
        cs.days_since_last_order,
        
        -- Additional attributes
        c.customer_segment as original_segment,
        c.birth_date,
        c.is_active,
        
        -- Tracking fields
        c.created_at,
        c.updated_at,
        c.last_login_at,
        current_timestamp() as dbt_updated_at
    from customers c
    left join customer_segments cs 
        on c.customer_id = cs.customer_id
    left join regions r 
        on c.region_id = r.region_id
)

select * from final

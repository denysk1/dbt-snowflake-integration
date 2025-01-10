with statuses as (
    select distinct 
        coalesce(order_status, 'Unknown') as order_status, 
        coalesce(payment_status, 'Unknown') as payment_status 
    from {{ ref('stg_orders') }}
),

final as (
    select
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['order_status', 'payment_status']) }} as order_status_sk,
        
        -- Status attributes
        order_status,
        payment_status,
        
        -- Derived status flags
        case 
            when order_status = 'delivered' and payment_status = 'paid' then true 
            else false 
        end as is_completed,
        
        case 
            when order_status = 'cancelled' then true 
            else false 
        end as is_cancelled,
        
        case 
            when payment_status = 'paid' then true 
            else false 
        end as is_paid,
        
        -- Status grouping
        case
            when order_status in ('pending', 'processing') then 'In Progress'
            when order_status = 'shipped' then 'In Transit'
            when order_status = 'delivered' then 'Completed'
            when order_status = 'cancelled' then 'Cancelled'
            else 'Other'
        end as status_category,
        
        -- Tracking
        current_timestamp() as dbt_updated_at
    from statuses
)

select * from final

with shipping_methods as (
    select distinct 
        shipping_method,
        count(*) as usage_count,
        avg(shipping_amount) as avg_shipping_cost,
        min(shipping_amount) as min_shipping_cost,
        max(shipping_amount) as max_shipping_cost
    from {{ ref('stg_orders') }}
    where shipping_method is not null
    group by 1
),

final as (
    select
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['shipping_method']) }} as shipping_method_sk,
        
        -- Method attributes
        shipping_method,
        
        -- Usage metrics
        usage_count as total_orders,
        avg_shipping_cost,
        min_shipping_cost,
        max_shipping_cost,
        
        -- Tracking
        current_timestamp() as dbt_updated_at
    from shipping_methods
)

select * from final

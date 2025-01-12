with order_promotions as (
    select * from {{ ref('stg_order_promotions') }}
),

orders as (
    select * from {{ ref('fct_orders') }}
),

promotions as (
    select * from {{ ref('dim_promotions') }}
),

final as (
    select
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['op.order_id', 'op.promotion_id']) }} as order_promotion_sk,
        
        -- Foreign keys (surrogate keys from dimensions)
        o.order_sk,
        p.promotion_sk,
        
        -- Promotion application details
        op.discount_amount as promotion_discount_amount,
        
        -- Tracking
        current_timestamp() as dbt_updated_at
        
    from order_promotions op
    inner join orders o 
        on op.order_id = o.order_id
    inner join promotions p 
        on op.promotion_id = p.promotion_id
)

select * from final

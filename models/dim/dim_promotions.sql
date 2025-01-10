with promotions as (
    select * from {{ ref('stg_promotions') }}
),

promotion_performance as (
    select * from {{ ref('int_promotion_performance') }}
),

final as (
    select
        -- Surrogate and natural keys
        {{ dbt_utils.generate_surrogate_key(['p.promotion_id']) }} as promotion_sk,
        p.promotion_id,
        
        -- Promotion attributes
        p.promotion_code,
        p.promotion_name,
        p.discount_type,
        p.discount_value,
        p.minimum_order_amount,
        
        -- Validity period
        p.start_date,
        p.end_date,
        current_date between p.start_date and coalesce(p.end_date, current_date) as is_currently_valid,
        
        -- Performance metrics
        pp.total_orders_using_promotion,
        pp.total_customers_using_promotion,
        pp.total_discount_amount,
        pp.total_order_value,
        pp.avg_discount_per_order,
        pp.discount_percentage_of_sales,
        
        -- Status and tracking
        p.is_active,
        current_timestamp() as dbt_updated_at
    from promotions p
    left join promotion_performance pp 
        on p.promotion_id = pp.promotion_id
)

select * from final

with products as (
    select * from {{ ref('stg_products') }}
),

product_inventory as (
    select * from {{ ref('int_product_inventory_status') }}
),

product_performance as (
    select * from {{ ref('int_product_performance') }}
),

categories as (
    select * from {{ ref('dim_product_categories') }}
),

final as (
    select
        -- Surrogate and natural keys
        {{ dbt_utils.generate_surrogate_key(['p.product_id']) }} as product_sk,
        p.product_id,
        
        -- Product attributes
        coalesce(p.product_name, 'Unknown Product') as product_name,
        p.product_code,
        p.brand_name,
        p.is_digital,
        
        -- Category references
        c.category_sk,
        c.category_name,
        c.category_path as product_category_path,
        
        -- Pricing information
        p.base_price_amount,
        p.discount_price_amount,
        case 
            when p.discount_price_amount is not null 
            then (p.base_price_amount - p.discount_price_amount) / p.base_price_amount * 100 
            else 0 
        end as discount_percentage,
        
        -- Product specifications
        p.weight_grams,
        
        -- Inventory status
        pi.current_stock as current_stock_quantity,
        pi.reorder_point,
        pi.needs_reorder,
        pi.stock_status,
        
        -- Performance metrics
        pp.total_units_sold,
        pp.gross_revenue,
        pp.net_revenue,
        pp.number_of_unique_customers,
        
        -- Status and tracking
        p.is_active,
        current_timestamp() as dbt_updated_at
    from products p
    left join categories c 
        on p.category_id = c.category_id
    left join product_inventory pi 
        on p.product_id = pi.product_id
    left join product_performance pp 
        on p.product_id = pp.product_id
)

select * from final

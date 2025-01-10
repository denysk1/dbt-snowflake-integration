with categories as (
    select * from {{ ref('stg_product_categories') }}
),

category_hierarchy as (
    select * from {{ ref('int_category_hierarchy') }}
),

final as (
    select
        -- Surrogate and natural keys
        {{ dbt_utils.generate_surrogate_key(['c.category_id']) }} as category_sk,
        c.category_id,
        c.parent_category_id,
        
        -- Category attributes
        c.category_name,
        c.category_code,
        
        -- Hierarchy information
        ch.level as hierarchy_level,
        ch.category_path,
        ch.category_id_path,
        
        -- Root category information
        split_part(ch.category_path, ' > ', 1) as root_category_name,
        split_part(ch.category_id_path, '/', 1) as root_category_id,
        
        -- Metadata
        current_timestamp() as dbt_updated_at
    from categories c
    left join category_hierarchy ch 
        on c.category_id = ch.category_id
)

select * from final

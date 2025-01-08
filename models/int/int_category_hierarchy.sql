with recursive category_tree as (
    -- Base case: top-level categories
    select
        category_id,
        parent_category_id,
        category_name,
        category_code,
        1 as level,
        cast(category_name as varchar) as category_path,
        cast(category_id as varchar) as category_id_path
    from {{ ref('stg_product_categories') }}
    where parent_category_id is null

    union all

    -- Recursive case: child categories
    select
        c.category_id,
        c.parent_category_id,
        c.category_name,
        c.category_code,
        t.level + 1 as level,
        t.category_path || ' > ' || c.category_name as category_path,
        t.category_id_path || '/' || c.category_id as category_id_path
    from {{ ref('stg_product_categories') }} c
    inner join category_tree t on c.parent_category_id = t.category_id
)

select * from category_tree

with source as (
    select * from {{ source('raw', 'product_categories') }}
),
renamed as (
    select
        category_id,
        parent_category_id,
        category_name,
        category_code,
        category_description,
        is_active,
        created_at,
        updated_at
    from source
)
select * from renamed

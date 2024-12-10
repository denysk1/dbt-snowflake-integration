with source as (
    select * from {{ source('raw', 'regions') }}
),
renamed as (
    select
        region_id,
        region_name,
        region_code,
        country_code,
        is_active,
        valid_from,
        valid_to
    from source
)
select * from renamed

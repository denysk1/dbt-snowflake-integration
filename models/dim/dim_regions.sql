with regions as (
    select * from {{ ref('stg_regions') }}
),

final as (
    select
        region_id,
        region_name,
        region_code,
        country_code,
        is_active,
        valid_from,
        valid_to,
        -- Add surrogate key
        {{ dbt_utils.generate_surrogate_key(['region_id']) }} as region_sk,
        current_timestamp() as dbt_updated_at
    from regions
)

select * from final

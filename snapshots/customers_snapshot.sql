{% snapshot customers_snapshot %}

{{
    config(
      target_schema='snapshots',
      strategy='check',
      unique_key='customer_id',
      check_cols=[
        'first_name',
        'last_name',
        'email',
        'phone',
        'date_of_birth',
        'region_id',
        'address_line1',
        'address_line2',
        'city',
        'state',
        'postal_code',
        'country',
        'customer_segment',
        'is_active',
        'last_login_at'
      ]
    )
}}

select 
    customer_id,
    first_name,
    last_name,
    email,
    phone,
    date_of_birth,
    region_id,
    address_line1,
    address_line2,
    city,
    state,
    postal_code,
    country,
    customer_segment,
    is_active,
    created_at,
    updated_at,
    last_login_at
from {{ source('raw', 'customers') }}

{% endsnapshot %}

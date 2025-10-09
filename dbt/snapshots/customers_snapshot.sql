{% snapshot customers_snapshot %}
{{
  config(
    target_schema='snapshot',
    unique_key='customer_id',
    strategy='check',
    check_cols=[
      'address_line1',
      'address_line2',
      'city',
      'state_region',
      'postcode',
      'country_code',
      'latitude',
      'longitude'
    ]
  )
}}
select * from {{ source('bronze', 'customers') }}
{% endsnapshot %}

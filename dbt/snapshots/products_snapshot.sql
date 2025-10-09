{% snapshot products_snapshot %}
{{
  config(
    target_schema='snapshot',
    unique_key='product_id',
    strategy='check',
    check_cols=['name','category','subcategory','current_price','currency','is_discontinued']
  )
}}
select * from {{ source('bronze', 'products') }}
{% endsnapshot %}

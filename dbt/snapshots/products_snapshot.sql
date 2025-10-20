{% snapshot products_snapshot %}
{{
  config(
    target_schema='snapshot',
    unique_key='product_id',
    strategy='check',
    check_cols=['product_name','category','subcategory','current_price','currency','is_discontinued']
  )
}}
select * from {{ ref('stg_products') }}
{% endsnapshot %}

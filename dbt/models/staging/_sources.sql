-- This model creates Bronze views and returns a status
{{ config(
    materialized='table',
    pre_hook="{{ create_bronze_views() }}"
) }}

-- Return a simple status table indicating views were created
select 
    'bronze_views_created' as status,
    current_timestamp as created_at

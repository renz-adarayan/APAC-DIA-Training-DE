{% macro create_bronze_views() %}
  {# Use the get_lake_path macro for intelligent path resolution #}
  {% set lake_root = get_lake_path() %}
  
  -- =============================================================================
  -- DIMENSION TABLES (Master Data)
  -- =============================================================================
  
  {% set sql %}
    create or replace view bronze_customers as
    select * from delta_scan('{{ lake_root }}/delta/customers');
  {% endset %}
  {% do run_query(sql) %}

  {% set sql %}
    create or replace view bronze_products as
    select * from delta_scan('{{ lake_root }}/delta/products');
  {% endset %}
  {% do run_query(sql) %}

  {% set sql %}
    create or replace view bronze_stores as
    select * from delta_scan('{{ lake_root }}/delta/stores');
  {% endset %}
  {% do run_query(sql) %}

  {% set sql %}
    create or replace view bronze_suppliers as
    select * from delta_scan('{{ lake_root }}/delta/suppliers');
  {% endset %}
  {% do run_query(sql) %}

  -- =============================================================================
  -- FACT TABLES (Transactional Data)
  -- =============================================================================

  {% set sql %}
    create or replace view bronze_orders_header as
    select * from delta_scan('{{ lake_root }}/delta/orders_header');
  {% endset %}
  {% do run_query(sql) %}

  {% set sql %}
    create or replace view bronze_orders_lines as
    select * from delta_scan('{{ lake_root }}/delta/orders_lines');
  {% endset %}
  {% do run_query(sql) %}

  -- =============================================================================
  -- EVENT AND IOT DATA
  -- =============================================================================

  {% set sql %}
    create or replace view bronze_events as
    select * from delta_scan('{{ lake_root }}/delta/events');
  {% endset %}
  {% do run_query(sql) %}

  {% set sql %}
    create or replace view bronze_sensors as
    select * from delta_scan('{{ lake_root }}/delta/sensors');
  {% endset %}
  {% do run_query(sql) %}

  -- =============================================================================
  -- FINANCIAL AND OPERATIONAL DATA
  -- =============================================================================

  {% set sql %}
    create or replace view bronze_exchange_rates as
    select * from delta_scan('{{ lake_root }}/delta/exchange_rates');
  {% endset %}
  {% do run_query(sql) %}

  {% set sql %}
    create or replace view bronze_shipments as
    select * from delta_scan('{{ lake_root }}/delta/shipments');
  {% endset %}
  {% do run_query(sql) %}

  {% set sql %}
    create or replace view bronze_returns as
    select * from delta_scan('{{ lake_root }}/delta/returns');
  {% endset %}
  {% do run_query(sql) %}

{% endmacro %}
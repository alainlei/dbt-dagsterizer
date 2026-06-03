{{
  config(
    enabled=var('enable_ods_test', false),
    materialized='incremental',
    schema=env_var('STARROCKS_ODS_DB', 'ods'),
    alias='customers',
    tags=['ods_test'],
    table_type='PRIMARY',
    keys=['customer_id'],
  )
}}

{% set n_bootstrap = var('ods_test_customers_count', 500) %}
{% set n_append = var('ods_test_customers_append_count', 0) %}
{% set days = var('ods_test_days', 30) %}
{% set base_ts = var('ods_test_base_ts', '') %}

{% if base_ts %}
  {% set base_expr = "cast('" ~ base_ts ~ "' as datetime)" %}
{% else %}
  {% set base_expr = "current_timestamp()" %}
{% endif %}

{% set total_hours = days * 24 %}

{% if is_incremental() %}
  {% set start_id = "coalesce((select max(customer_id) from " ~ this ~ "), 0)" %}
  {% set n = n_append %}
{% else %}
  {% set start_id = "0" %}
  {% set n = n_bootstrap %}
{% endif %}

{% if n | int <= 0 %}

select
  cast(null as bigint) as customer_id,
  cast(null as varchar) as first_name,
  cast(null as varchar) as last_name,
  cast(null as datetime) as updated_at,
  cast(null as datetime) as ods_created_at,
  cast(null as datetime) as ods_updated_at
where 1 = 0

{% else %}

with ids as (
  select
    cast({{ start_id }} as bigint) + cast(generate_series as bigint) as customer_id
  from TABLE(generate_series(1, {{ n | int }}))
),
named as (
  select
    customer_id,
    case (customer_id * 37 + 11) % 50
      when 0 then 'James'
      when 1 then 'Mary'
      when 2 then 'John'
      when 3 then 'Patricia'
      when 4 then 'Robert'
      when 5 then 'Jennifer'
      when 6 then 'Michael'
      when 7 then 'Linda'
      when 8 then 'William'
      when 9 then 'Elizabeth'
      when 10 then 'David'
      when 11 then 'Barbara'
      when 12 then 'Richard'
      when 13 then 'Susan'
      when 14 then 'Joseph'
      when 15 then 'Jessica'
      when 16 then 'Thomas'
      when 17 then 'Sarah'
      when 18 then 'Charles'
      when 19 then 'Karen'
      when 20 then 'Christopher'
      when 21 then 'Nancy'
      when 22 then 'Daniel'
      when 23 then 'Lisa'
      when 24 then 'Matthew'
      when 25 then 'Betty'
      when 26 then 'Anthony'
      when 27 then 'Margaret'
      when 28 then 'Mark'
      when 29 then 'Sandra'
      when 30 then 'Donald'
      when 31 then 'Ashley'
      when 32 then 'Steven'
      when 33 then 'Kimberly'
      when 34 then 'Paul'
      when 35 then 'Emily'
      when 36 then 'Andrew'
      when 37 then 'Donna'
      when 38 then 'Joshua'
      when 39 then 'Michelle'
      when 40 then 'Kenneth'
      when 41 then 'Carol'
      when 42 then 'Kevin'
      when 43 then 'Amanda'
      when 44 then 'Brian'
      when 45 then 'Melissa'
      when 46 then 'George'
      when 47 then 'Deborah'
      when 48 then 'Edward'
      else 'Stephanie'
    end as first_name,
    case (customer_id * 53 + 7) % 50
      when 0 then 'Smith'
      when 1 then 'Johnson'
      when 2 then 'Williams'
      when 3 then 'Brown'
      when 4 then 'Jones'
      when 5 then 'Garcia'
      when 6 then 'Miller'
      when 7 then 'Davis'
      when 8 then 'Rodriguez'
      when 9 then 'Martinez'
      when 10 then 'Hernandez'
      when 11 then 'Lopez'
      when 12 then 'Gonzalez'
      when 13 then 'Wilson'
      when 14 then 'Anderson'
      when 15 then 'Thomas'
      when 16 then 'Taylor'
      when 17 then 'Moore'
      when 18 then 'Jackson'
      when 19 then 'Martin'
      when 20 then 'Lee'
      when 21 then 'Perez'
      when 22 then 'Thompson'
      when 23 then 'White'
      when 24 then 'Harris'
      when 25 then 'Sanchez'
      when 26 then 'Clark'
      when 27 then 'Ramirez'
      when 28 then 'Lewis'
      when 29 then 'Robinson'
      when 30 then 'Walker'
      when 31 then 'Young'
      when 32 then 'Allen'
      when 33 then 'King'
      when 34 then 'Wright'
      when 35 then 'Scott'
      when 36 then 'Torres'
      when 37 then 'Nguyen'
      when 38 then 'Hill'
      when 39 then 'Flores'
      when 40 then 'Green'
      when 41 then 'Adams'
      when 42 then 'Nelson'
      when 43 then 'Baker'
      when 44 then 'Hall'
      when 45 then 'Rivera'
      when 46 then 'Campbell'
      when 47 then 'Mitchell'
      when 48 then 'Carter'
      else 'Roberts'
    end as last_name
  from ids
)

select
  customer_id,
  first_name,
  last_name,
  {% if is_incremental() %}
  {{ base_expr }} as updated_at,
  {% else %}
  hours_sub(date_trunc('hour', {{ base_expr }}), cast((customer_id * 13) % {{ total_hours }} as int)) as updated_at,
  {% endif %}
  {{ base_expr }} as ods_created_at,
  {% if is_incremental() %}
  {{ base_expr }} as ods_updated_at
  {% else %}
  hours_sub(date_trunc('hour', {{ base_expr }}), cast((customer_id * 13) % {{ total_hours }} as int)) as ods_updated_at
  {% endif %}
from named

{% endif %}

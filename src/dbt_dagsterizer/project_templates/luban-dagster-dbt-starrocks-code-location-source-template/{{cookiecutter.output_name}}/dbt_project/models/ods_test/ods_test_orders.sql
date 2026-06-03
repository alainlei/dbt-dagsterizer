{{
  config(
    enabled=var('enable_ods_test', false),
    materialized='incremental',
    schema=env_var('STARROCKS_ODS_DB', 'ods'),
    alias='orders',
    tags=['ods_test'],
    table_type='PRIMARY',
    keys=['order_id'],
  )
}}

{% set customers_bootstrap = var('ods_test_customers_count', 500) %}
{% set per_customer = var('ods_test_orders_per_customer', 20) %}
{% set orders_append = var('ods_test_orders_append_count', 1000) %}
{% set days = var('ods_test_days', 30) %}
{% set base_ts = var('ods_test_base_ts', '') %}

{% if base_ts %}
  {% set base_expr = "cast('" ~ base_ts ~ "' as datetime)" %}
{% else %}
  {% set base_expr = "current_timestamp()" %}
{% endif %}

{% set total_hours = days * 24 %}
{% set hour_weights = [
  9, 10, 11, 12, 12, 13, 13, 14,
  18, 18, 19, 19, 20, 20, 21, 21,
  22, 22, 11, 12, 13, 18, 19, 20,
  21, 22, 10, 9, 14, 15, 16, 17,
  8, 23, 7, 6, 5, 4, 3, 2,
  1, 0, 12, 18, 20, 13, 19, 21
] %}

{% if is_incremental() %}
  {% set start_id = "coalesce((select max(order_id) from " ~ this ~ "), 0)" %}
  {% set n = orders_append %}
{% else %}
  {% set start_id = "0" %}
  {% set n = (customers_bootstrap * per_customer) %}
{% endif %}

{% if n | int <= 0 %}

select
  cast(null as bigint) as order_id,
  cast(null as bigint) as customer_id,
  cast(null as bigint) as order_amount,
  cast(null as datetime) as order_datetime,
  cast(null as datetime) as updated_at,
  cast(null as datetime) as ods_created_at,
  cast(null as datetime) as ods_updated_at
where 1 = 0

{% else %}

with params as (
  select
    cast(coalesce((select max(customer_id) from {{ ref('ods_test_customers') }}), {{ customers_bootstrap | int }}) as bigint) as max_customer_id
),
order_rows as (
  select
    cast({{ start_id }} as bigint) + cast(generate_series as bigint) as order_id,
    cast(generate_series as bigint) as seq
  from TABLE(generate_series(1, {{ n | int }}))
),
enriched as (
  select
    r.order_id,
    cast(((r.seq * 73 + 19) % p.max_customer_id) + 1 as bigint) as customer_id,
    cast((r.seq * 31) % {{ days | int }} as int) as day_offset,
    cast((r.seq * 17) % {{ hour_weights | length }} as int) as hour_bucket,
    cast((r.seq * 29) % 100 as int) as amt_bucket
  from order_rows r
  cross join params p
)

select
  order_id,
  customer_id,
  case
    when amt_bucket < 60 then 10 + ((amt_bucket * 3) % 40)
    when amt_bucket < 85 then 50 + ((amt_bucket * 7) % 150)
    when amt_bucket < 96 then 200 + ((amt_bucket * 19) % 600)
    else 800 + ((amt_bucket * 23) % 1200)
  end as order_amount,
  hours_sub(
    date_trunc('hour', {{ base_expr }}),
    day_offset * 24 + (
      case hour_bucket
        {% for i in range(0, hour_weights | length) %}
        when {{ i }} then {{ hour_weights[i] }}
        {% endfor %}
      end
    )
  ) as order_datetime,
  {% if is_incremental() %}
  {{ base_expr }} as updated_at,
  {% else %}
  hours_sub(
    date_trunc('hour', {{ base_expr }}),
    day_offset * 24 + (
      case hour_bucket
        {% for i in range(0, hour_weights | length) %}
        when {{ i }} then {{ hour_weights[i] }}
        {% endfor %}
      end
    )
  ) as updated_at,
  {% endif %}
  {{ base_expr }} as ods_created_at,
  {% if is_incremental() %}
  {{ base_expr }} as ods_updated_at
  {% else %}
  hours_sub(
    date_trunc('hour', {{ base_expr }}),
    day_offset * 24 + (
      case hour_bucket
        {% for i in range(0, hour_weights | length) %}
        when {{ i }} then {{ hour_weights[i] }}
        {% endfor %}
      end
    )
  ) as ods_updated_at
  {% endif %}
from enriched

{% endif %}

# dbt Model SQL Files Guide

This document explains the structure, conventions, and patterns used in dbt model SQL files within a dbt-dagsterizer project targeting StarRocks.

## Overview

dbt models are SQL files that define transformations in your data pipeline. In this project, models follow a layered architecture and integrate with Dagster for partitioned execution.

## File Structure

```
dbt_project/
├── models/
│   ├── ods_test/          # Test data bootstrap models
│   │   ├── ods_test_orders.sql
│   │   └── ods_test_customers.sql
│   ├── dwd/               # Data Warehouse Detail layer
│   │   ├── orders.sql
│   │   └── customers.sql
│   └── dws/               # Data Warehouse Summary layer
│       ├── fact_orders_daily.sql
│       ├── fact_customer_orders_daily.sql
│       └── dim_customer.sql
├── macros/
│   └── dbt_dagsterizer/   # Custom macros
│       ├── partition_vars.sql
│       ├── starrocks_layer_schema.sql
│       ├── starrocks_overrides.sql
│       └── generate_schema_name.sql
└── sources.yml            # Source table definitions
```

## Layered Architecture

### DWD (Data Warehouse Detail) Layer

**Purpose**: Clean, standardized detail-level data from ODS (Operational Data Store)

**Characteristics**:
- One-to-one or slight transformation from source tables
- Partitioned by time (datetime or date)
- Incremental materialization
- Primary key tables in StarRocks

**Example** ([dwd/orders.sql]):

```sql
{{
    config(
        materialized='incremental',
        incremental_strategy='default',
        table_type='PRIMARY',
        keys=['order_id'],
    )
}}

select
  order_id,
  customer_id,
  order_amount,
  order_datetime,
  cast(order_datetime as date) as order_date,
  updated_at
from {{ source('ods', 'orders') }}

{% set w = luban_partition_window_datetime() %}
where order_datetime >= '{{ w["min_datetime"] }}'
  and order_datetime < '{{ w["max_datetime"] }}'
```

**Key Elements**:

1. **Materialization**: `incremental` - only processes new/changed data
2. **StarRocks Table Type**: `PRIMARY` - uses StarRocks Primary Key table
3. **Keys**: `['order_id']` - primary key definition for upserts
4. **Source Reference**: `{{ source('ods', 'orders') }}` - references dbt source
5. **Partition Window**: `luban_partition_window_datetime()` - Dagster-provided time window

### DWS (Data Warehouse Summary) Layer

**Purpose**: Aggregated, business-level data models for analytics

**Two Types**:

#### 1. Fact Models (Partitioned)

Time-based aggregations with daily/hourly partitions:

**Example** ([dws/fact_orders_daily.sql]):

```sql
{{
  config(
    materialized="incremental",
    unique_key="order_date"
  )
}}

select
  order_date,
  count(*) as order_count,
  sum(order_amount) as total_amount
from {{ ref('orders') }}

{% set w = luban_partition_window_date() %}
where order_date >= '{{ w["min_date"] }}'
  and order_date < '{{ w["max_date"] }}'

group by 1
```

**Key Elements**:
- **Unique Key**: `order_date` - used for incremental upserts
- **Reference**: `{{ ref('orders') }}` - references upstream DWD model
- **Date Partition**: `luban_partition_window_date()` - date-level partition window
- **Aggregation**: Groups by partition key with business metrics

#### 2. Dimension Models (Event-Driven)

Slowly changing dimensions that refresh when upstream changes:

**Example** ([dws/dim_customer.sql]):

```sql
{{
  config(
    tags=["dim"],
    materialized="incremental",
    incremental_strategy="default",
    table_type='PRIMARY',
    keys=['customer_id']
  )
}}

select
  customer_id,
  first_name,
  last_name,
  updated_at
from {{ ref('customers') }}

{% if is_incremental() %}
  where updated_at > (select max(updated_at) from {{ this }})
{% endif %}
```

**Key Elements**:
- **Tag**: `tags=["dim"]` - marks as dimension for automation conditions
- **Incremental Logic**: Uses `is_incremental()` to filter new records
- **Watermark**: `updated_at > (select max(updated_at) from {{ this }})` - processes only newer data

#### 3. Dynamic Partition Models (Non-Time-Based)

Models partitioned by business keys (e.g., country code, tenant ID, region):

**Example** (dynamic partition by country code):

```sql
{{
    config(
        materialized='incremental',
        incremental_strategy='default',
        table_type='PRIMARY',
        keys=['order_id', 'country_code'],
    )
}}

select
  order_id,
  customer_id,
  country_code,
  order_amount,
  order_datetime,
  cast(order_datetime as date) as order_date,
  updated_at
from {{ source('ods', 'orders') }}

where country_code = '{{ var("partition_key") }}'

{% set w = luban_partition_window_datetime() %}
  and order_datetime >= '{{ w["min_datetime"] }}'
  and order_datetime < '{{ w["max_datetime"] }}'
```

**Key Elements**:
- **Partition Key Filter**: `where country_code = '{{ var("partition_key") }}'` - filters by the dynamic partition key
- **Dagster Variable**: `var("partition_key")` - receives the partition key from Dagster at runtime (e.g., 'US', 'GB', 'DE')
- **Combined with Time Window**: Can also use partition window macros for time-based filtering within each partition key
- **Composite Keys**: `keys=['order_id', 'country_code']` - includes partition key in primary key definition

**How Dynamic Partitions Work**:

1. **Configuration** in `dagsterization.yml`:
   ```yaml
   partitions:
     dynamic:
       - name: country_code
         initial_partition_keys: ['US', 'GB', 'DE', 'JP', 'AU']
         models:
           - orders_by_country
   ```

2. **Dagster Execution**:
   - Bootstrap sensor syncs partition keys to Dagster instance
   - Schedule emits separate `RunRequest` for each partition key
   - Each run passes `partition_key` as a dbt variable

3. **SQL Execution**:
   - Run 1: `var("partition_key")` = 'US' → processes only US orders
   - Run 2: `var("partition_key")` = 'GB' → processes only GB orders
   - Run 3: `var("partition_key")` = 'DE' → processes only DE orders

**Another Example** (Tenant-based partitioning):

```sql
{{
    config(
        materialized='incremental',
        unique_key=['tenant_id', 'report_date'],
    )
}}

select
  tenant_id,
  report_date,
  count(*) as transaction_count,
  sum(amount) as total_amount
from {{ ref('transactions') }}

where tenant_id = '{{ var("partition_key") }}'

{% set w = luban_partition_window_date() %}
  and report_date >= '{{ w["min_date"] }}'
  and report_date < '{{ w["max_date"] }}'

group by 1, 2
```

**Use Cases for Dynamic Partitions**:
- Multi-tenant architectures (partition by `tenant_id`)
- Regional data processing (partition by `country_code`, `region`)
- Source-based partitioning (partition by `source_system`)
- Business unit segmentation (partition by `business_unit`)

---

## Configuration Options

### Materialization Strategies

| Strategy | Use Case | Description |
|----------|----------|-------------|
| `incremental` | Most models | Only processes new/changed data |
| `table` | Small dimension tables | Full rebuild on every run |
| `view` | Virtual transformations | Creates SQL view (not materialized) |

### StarRocks-Specific Configs

#### Primary Key Tables

```sql
{{
    config(
        materialized='incremental',
        incremental_strategy='default',
        table_type='PRIMARY',
        keys=['order_id', 'customer_id'],
    )
}}
```

- `table_type='PRIMARY'`: StarRocks Primary Key table (supports upserts)
- `keys`: Primary key columns for uniqueness and upsert behavior

#### Unique Key for Incremental

```sql
{{
  config(
    materialized="incremental",
    unique_key="order_date"
  )
}}
```

- `unique_key`: Column(s) that identify unique rows for incremental updates

### Tags

```sql
{{
  config(
    tags=["dim", "daily_refresh"]
  )
}}
```

Tags are used for:
- Model selection: `dbt run --select tag:dim`
- Automation conditions: DWS dimensions tagged `dim` get `AutomationCondition.eager()`
- Grouping and filtering in Dagster UI

---

## Partition Execution

### How Dagster Passes Partition Windows

When Dagster runs a partitioned job, it injects dbt variables:

**For Date Partitions**:
- `min_date`: Start date (inclusive) e.g., `'2024-01-15'`
- `max_date`: End date (exclusive) e.g., `'2024-01-16'`

**For Datetime Partitions**:
- `min_datetime`: Start timestamp (inclusive) e.g., `'2024-01-15 00:00:00'`
- `max_datetime`: End timestamp (exclusive) e.g., `'2024-01-15 01:00:00'`

**For Dynamic Partitions**:
- `partition_key`: The partition key value (e.g., `'US'`, `'GB'`, `'DE'`, `'tenant_001'`)
- Combined with time partition vars if the dynamic partition also uses time windows

### Partition Window Macros

#### `luban_partition_window_date()`

Returns date-based partition window:

```sql
{% set w = luban_partition_window_date() %}
where order_date >= '{{ w["min_date"] }}'
  and order_date < '{{ w["max_date"] }}'
```

**Source** ([macros/dbt_dagsterizer/partition_vars.sql]):

```sql
{% macro luban_partition_window_date() %}
  {% set min_date = var('min_date', none) %}
  {% set max_date = var('max_date', none) %}
  {% if execute and (min_date is none or max_date is none) %}
    {{ exceptions.raise_compiler_error("Missing required dbt vars 'min_date'/'max_date'. Run this model via Dagster partitioned execution (or pass --vars with a partition window).") }}
  {% endif %}
  {{ return({"min_date": min_date or "1970-01-01", "max_date": max_date or "1970-01-02"}) }}
{% endmacro %}
```

**Behavior**:
- Fails fast if vars are missing during Dagster execution
- Returns dictionary with `min_date` and `max_date`
- Safe defaults for non-partitioned runs (though should not occur in production)

#### `luban_partition_window_datetime()`

Returns datetime-based partition window:

```sql
{% set w = luban_partition_window_datetime() %}
where order_datetime >= '{{ w["min_datetime"] }}'
  and order_datetime < '{{ w["max_datetime"] }}'
```

**Source**:

```sql
{% macro luban_partition_window_datetime() %}
  {% set min_datetime = var('min_datetime', none) %}
  {% set max_datetime = var('max_datetime', none) %}
  {% if execute and (min_datetime is none or max_datetime is none) %}
    {{ exceptions.raise_compiler_error("Missing required dbt vars 'min_datetime'/'max_datetime'. Run this model via Dagster partitioned execution (or pass --vars with a partition window).") }}
  {% endif %}
  {{ return({"min_datetime": min_datetime or "1970-01-01 00:00:00", "max_datetime": max_datetime or "1970-01-02 00:00:00"}) }}
{% endmacro %}
```

---

## Incremental Logic Patterns

### Pattern 1: Partition Window Filtering (DWD/DWS Facts)

**Best for**: Time-partitioned models where Dagster controls the window

```sql
{% set w = luban_partition_window_datetime() %}
where order_datetime >= '{{ w["min_datetime"] }}'
  and order_datetime < '{{ w["max_datetime"] }}'
```

**How it works**:
1. Dagster passes partition time window as dbt vars
2. Macro retrieves and validates the window
3. SQL filters source data to only the partition's time range
4. Each partition run processes only its slice of data

### Pattern 2: Watermark-Based Incremental (Dimensions)

**Best for**: Slowly changing dimensions or event-driven refresh

```sql
{% if is_incremental() %}
  where updated_at > (select max(updated_at) from {{ this }})
{% endif %}
```

**How it works**:
1. `is_incremental()` returns true only during incremental runs (not full refresh)
2. Subquery finds the latest `updated_at` in the existing table
3. Only processes records newer than the watermark
4. Efficient for catching up on late arrivals

### Pattern 3: ODS Test Models (Data Generation)

**Best for**: Bootstrap/test data generation with append behavior

```sql
{% if is_incremental() %}
  {% set last_ts = run_query("select max(updated_at) from " ~ this)[0][0] %}
  {% if last_ts %}
    where created_at > '{{ last_ts }}'
  {% endif %}
{% endif %}
```

**How it works**:
1. Queries existing table for last timestamp
2. Generates new test data after that timestamp
3. Supports continuous data simulation

---

## Jinja Functions & Macros

### dbt Built-in Functions

| Function | Purpose | Example |
|----------|---------|---------|
| `source(name, table)` | Reference dbt source | `{{ source('ods', 'orders') }}` |
| `ref(model)` | Reference dbt model | `{{ ref('orders') }}` |
| `this` | Current model's relation | `select max(updated_at) from {{ this }}` |
| `is_incremental()` | Check if incremental run | `{% if is_incremental() %}` |
| `var(name, default)` | Get dbt variable | `{{ var('min_date') }}` |

### Custom Macros (dbt-dagsterizer)

| Macro | Purpose | Returns |
|-------|---------|---------|
| `luban_partition_window_date()` | Get date partition window | `{"min_date": "...", "max_date": "..."}` |
| `luban_partition_window_datetime()` | Get datetime partition window | `{"min_datetime": "...", "max_datetime": "..."}` |
| `starrocks_layer_schema(node)` | Resolve schema by layer | Database name based on model layer |

### StarRocks Schema Resolution

The `starrocks_layer_schema` macro automatically routes models to the correct database:

```sql
{% macro starrocks_layer_schema(node) -%}
  {%- set fqn_1 = node.fqn[1] if (node.fqn | length) > 1 else '' -%}
  
  {%- if fqn_1 == 'dwd' -%}
    {{ env_var('STARROCKS_DWD_DB', 'dwd') }}
  {%- elif fqn_1 == 'dws' -%}
    {{ env_var('STARROCKS_DWS_DB', 'dws') }}
  {%- else -%}
    {{ target.schema }}
  {%- endif -%}
{%- endmacro %}
```

**Behavior**:
- `dwd/` models → `STARROCKS_DWD_DB` environment variable (default: `dwd`)
- `dws/` models → `STARROCKS_DWS_DB` environment variable (default: `dws`)
- Other models → `target.schema` from dbt profiles

---

## Best Practices

### 1. Always Use Partition Windows for Time-Partitioned Models

✅ **Good**:
```sql
{% set w = luban_partition_window_date() %}
where order_date >= '{{ w["min_date"] }}'
  and order_date < '{{ w["max_date"] }}'
```

❌ **Bad** (processes all data every time):
```sql
where order_date >= '2024-01-01'
```

### 2. Use Incremental Materialization for Large Tables

✅ **Good**:
```sql
{{ config(materialized='incremental', unique_key='order_id') }}
```

❌ **Bad** (full rebuild on every run):
```sql
{{ config(materialized='table') }}
```

### 3. Define Primary Keys for StarRocks Tables

✅ **Good**:
```sql
{{
    config(
        table_type='PRIMARY',
        keys=['order_id']
    )
}}
```

### 4. Tag Dimensions for Automation

✅ **Good**:
```sql
{{ config(tags=["dim"]) }}
```

This enables `AutomationCondition.eager()` for automatic refresh when upstream changes.

### 5. Use `source()` for Raw Data, `ref()` for Models

✅ **Good**:
```sql
from {{ source('ods', 'orders') }}  -- Raw source table
from {{ ref('orders') }}            -- Upstream dbt model
```

### 6. Fail Fast on Missing Partition Vars

The `luban_partition_window_*()` macros automatically fail if vars are missing during execution, preventing accidental full-table scans.

---

## Common Patterns by Layer

### DWD Layer Pattern

```sql
{{
    config(
        materialized='incremental',
        incremental_strategy='default',
        table_type='PRIMARY',
        keys=['<primary_key>'],
    )
}}

select
  <columns>
from {{ source('<source_name>', '<table_name>') }}

{% set w = luban_partition_window_datetime() %}
where <datetime_column> >= '{{ w["min_datetime"] }}'
  and <datetime_column> < '{{ w["max_datetime"] }}'
```

**Use when**: Cleaning/standardizing ODS data with datetime partitioning

### DWS Fact Pattern

```sql
{{
  config(
    materialized="incremental",
    unique_key="<partition_key>"
  )
}}

select
  <partition_key>,
  <aggregations>
from {{ ref('<upstream_model>') }}

{% set w = luban_partition_window_date() %}
where <date_column> >= '{{ w["min_date"] }}'
  and <date_column> < '{{ w["max_date"] }}'

group by 1
```

**Use when**: Building daily/hourly aggregated fact tables

### DWS Dimension Pattern

```sql
{{
  config(
    tags=["dim"],
    materialized="incremental",
    incremental_strategy="default",
    table_type='PRIMARY',
    keys=['<primary_key>']
  )
}}

select
  <columns>
from {{ ref('<upstream_model>') }}

{% if is_incremental() %}
  where updated_at > (select max(updated_at) from {{ this }})
{% endif %}
```

**Use when**: Building slowly changing dimension tables

---

## Testing Models Locally

### Run a Single Model

```bash
dbt run --select orders
```

### Run with Partition Vars

```bash
dbt run --select orders \
  --vars '{"min_datetime": "2024-01-15 00:00:00", "max_datetime": "2024-01-16 00:00:00"}'
```

### Run with Dynamic Partition Key

```bash
dbt run --select orders_by_country \
  --vars '{"partition_key": "US", "min_datetime": "2024-01-15 00:00:00", "max_datetime": "2024-01-16 00:00:00"}'
```

Or for date-based dynamic partitions:

```bash
dbt run --select tenant_reports \
  --vars '{"partition_key": "tenant_001", "min_date": "2024-01-15", "max_date": "2024-01-16"}'
```

### Full Refresh

```bash
dbt run --select orders --full-refresh
```

### Test Models

```bash
dbt test --select orders
```

---

## Troubleshooting

### Error: Missing required dbt vars 'min_date'/'max_date'

**Cause**: Running a partitioned model outside of Dagster without passing vars

**Solution**:
```bash
dbt run --select <model> \
  --vars '{"min_date": "2024-01-15", "max_date": "2024-01-16"}'
```

Or run via Dagster:
```bash
dagster asset materialize --select <asset_key>
```

### Error: Compilation Error in macro luban_partition_window_datetime()

**Cause**: Malformed vars or missing vars during execution

**Solution**: Ensure vars are passed as JSON object with correct keys

### Model Runs But Processes All Data

**Cause**: Partition window not applied correctly

**Solution**:
1. Check that `luban_partition_window_*()` macro is called
2. Verify `where` clause uses the window variables
3. Ensure Dagster job is configured with correct partition type

### Incremental Not Working (Processes All Data Every Time)

**Cause**: Missing `is_incremental()` check or incorrect unique key

**Solution**:
1. Verify `materialized='incremental'` in config
2. Check `unique_key` or `keys` is defined
3. Ensure incremental filter is in `{% if is_incremental() %}` block

### Error: UndefinedVariableError: 'partition_key' is undefined

**Cause**: Running a dynamic partition model without passing the `partition_key` variable

**Solution**:
```bash
dbt run --select <model> \
  --vars '{"partition_key": "US"}'
```

Or run via Dagster which automatically passes the partition key:
```bash
dagster asset materialize --select <asset_key> --partition-key US
```

### Dynamic Partition Only Processes One Key

**Cause**: Schedule or job not configured to emit RunRequests for all partition keys

**Solution**:
1. Verify dynamic partition is configured in `dagsterization.yml` with `initial_partition_keys`
2. Check bootstrap sensor is running (not stopped) in Dagster UI
3. Verify schedule has correct `partitions: dynamic:<name>` configuration
4. Review sensor logs to confirm all partition keys are synced

---

## See Also

- [dagsterization.yml Reference](../../concepts/dagsterization-yml.md) - Configure partitioning and jobs
- [Developer Workflow](../../templates/dagster-dbt-starrocks-code-location/developer_workflow.md) - Orchestration workflow
- [Template Usage](../../templates/dagster-dbt-starrocks-code-location/template_usage.md) - Template features
- [dbt Documentation](https://docs.getdbt.com/docs/build/models) - Official dbt docs
- [dbt-starrocks Documentation](https://docs.getdbt.com/docs/core/connect-data-platform/starrocks-setup) - StarRocks adapter docs

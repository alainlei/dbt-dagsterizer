# dagsterization.yml Reference

The `dagsterization.yml` file is the **single source of truth** for Dagster orchestration intent in a dbt-dagsterizer project. It lives in your dbt project root (alongside `dbt_project.yml`) and declares how dbt models should be orchestrated in Dagster.

## Location

```
dbt_project/
├── dbt_project.yml
├── dagsterization.yml    ← Orchestration configuration
├── models/
└── target/
    └── manifest.json     ← dbt metadata (auto-generated)
```

**Important**: This file is NOT a dbt schema YAML and is intentionally kept outside `dbt_project/models/` so dbt will not parse it.

## Purpose

The file bridges dbt metadata (from `manifest.json`) with Dagster orchestration by declaring:

- **Partitioning strategy** for each model (daily, hourly, monthly, dynamic, or unpartitioned)
- **Job definitions** (per-model asset jobs or grouped jobs)
- **Schedules** (when jobs should run)
- **Partition change sensors** (detectors and propagators for handling late arrivals)

## Top-Level Structure

```yaml
version: 1                          # Config schema version
partitions:                         # Partition assignments by model
  daily: []
  dynamic: []
jobs:                               # Grouped job definitions
  job_name:
    models: []
    include_upstream: false
    partitions: daily
asset_jobs:                         # Per-model asset job enablement
  - model_name
schedules:                          # Schedule definitions
  schedule_name:
    type: daily_at
    job_name: ...
    hour: ...
    minute: ...
    lookback_days: ...
    offset_days: ...
    enabled: true
partition_change:                   # Partition change detection
  detectors: []
  propagators: []
```

---

## Partitions

The `partitions` section assigns partitioning strategies to dbt models. Each model can only belong to one partition type.

> ⚠️ **Important: Single Partition Scheme Constraint**
> 
> Dagster's `@dbt_assets` decorator only supports **one partitioning scheme per asset group**. This means:
> - You cannot mix daily, dynamic, and unpartitioned models in a single `@dbt_assets` definition
> - Each partition type must be isolated in its own asset group
> - If you try to mix partition types in one job, you'll get a `DagsterInvariantViolationError`


### Valid Partition Types

| Type | Description | Env Var Required | Asset Group Isolation |
|------|-------------|------------------|----------------------|
| `daily` | One partition per day | `DAGSTER_DAILY_PARTITIONS_START_DATE` (YYYY-MM-DD) | ✅ Separate group |
| `dynamic` | Custom partition keys (e.g., country codes) | N/A (defined inline) | ✅ Separate group (per name) |

All time-window partition types share: `DAGSTER_PARTITION_TIMEZONE` (e.g., `Asia/Shanghai`)

### Daily Partitions

```yaml
partitions:
  daily:
    - orders
    - customers
    - fact_orders_daily
```

**CLI equivalent**:
```bash
dbt-dagsterizer meta partition --models orders,customers,fact_orders_daily --type daily
```

### Dynamic Partitions

Dynamic partitions allow non-time-based partitioning (e.g., by country, tenant, data source).

```yaml
partitions:
  dynamic:
    - name: country_code
      initial_partition_keys: ['US', 'GB', 'DE', 'JP', 'AU']
      models:
        - orders_by_country
        - customers_by_country
    
    - name: tenant_id
      initial_partition_keys: ['tenant_001', 'tenant_002', 'tenant_003']
      models:
        - tenant_reports
```

**Key fields**:
- `name`: Unique identifier for the dynamic partition (required)
- `initial_partition_keys`: List of partition keys to sync at startup (required)
- `models`: dbt models assigned to this partition scheme (optional)

**CLI equivalent**:
```bash
dbt-dagsterizer meta partition dynamic \
  --name country_code \
  --initial-keys US,GB,DE,JP,AU \
  --models orders_by_country,customers_by_country
```

**How it works**:
1. A bootstrap sensor syncs `initial_partition_keys` to the Dagster instance at startup
2. Schedules emit `RunRequest` for each partition key
3. Partition change detectors can monitor upstream changes per key

---

## Jobs

Jobs define how dbt models are executed together. There are two job types:

### Asset Jobs (One Model Per Job)

Enable per-model asset jobs for fine-grained control:

```yaml
asset_jobs:
  - orders
  - customers
  - staging_events
```

This creates jobs named:
- `dbt_orders_asset_job`
- `dbt_customers_asset_job`
- `dbt_staging_events_asset_job`

**CLI equivalent**:
```bash
dbt-dagsterizer meta asset-job --models orders,customers,staging_events
```

**Best for**: `dwd` layer models where each model needs independent scheduling.

### Grouped Jobs (Multiple Models Per Job)

Group multiple models into a single job:

```yaml
jobs:
  daily_facts_job:
    models:
      - fact_orders_daily
      - fact_customer_orders_daily
      - fact_revenue_daily
    include_upstream: false
    partitions: daily
  
  country_reports_job:
    models:
      - country_summary
      - country_metrics
    include_upstream: true
    partitions: dynamic:country_code
```

**Fields**:
- `models`: List of dbt models in this job (required)
- `include_upstream`: Whether to include upstream dependencies (default: `false`)
- `partitions`: Partition strategy for the job (`daily`, `hourly`, `monthly`, `dynamic:name`, `unpartitioned`)

**CLI equivalent**:
```bash
dbt-dagsterizer meta job \
  --models fact_orders_daily,fact_customer_orders_daily,fact_revenue_daily \
  --name daily_facts_job \
  --partitions daily

dbt-dagsterizer meta job \
  --models country_summary,country_metrics \
  --name country_reports_job \
  --partitions dynamic:country_code \
  --include-upstream
```

**Best for**: `dws` layer models that should run together.

### Asset Identity

Auto-generated jobs use **relation-based AssetKeys** derived from dbt manifest metadata:

- Format: `dbt/<database>/<schema>/<identifier>`
- Empty components are omitted
- Example: `dbt/warehouse/dwd/orders`

This ensures consistent asset identity across different code locations referencing the same physical table.

---

## Schedules

Schedules define when jobs should run automatically.

### Daily Schedule

```yaml
schedules:
  orders_daily_schedule:
    type: daily_at
    job_name: dbt_orders_asset_job
    hour: 2
    minute: 30
    lookback_days: 0
    offset_days: 1
    enabled: true
```

**Fields**:
- `type`: Schedule type (`daily_at`, `hourly_at`, `monthly_at`)
- `job_name`: Target job name (required)
- `hour`: Hour of day (0-23)
- `minute`: Minute of hour (0-59)
- `lookback_days`: How many past partitions to process (default: 0)
- `offset_days`: Partition offset (default: 1 = yesterday, 0 = today)
- `enabled`: Whether schedule is active (default: `true`)

**CLI equivalent**:
```bash
dbt-dagsterizer meta schedule \
  --models orders \
  --name orders_daily_schedule \
  --hour 2 \
  --minute 30 \
  --lookback-days 0 \
  --offset-days 1 \
  --enabled
```

### Schedule Offset Days

- `offset_days: 1` (default): Run for **yesterday's** partition
  - Schedule runs at 2:00 AM on Day D → processes partition D-1
  - **Recommended** for most daily jobs
  
- `offset_days: 0`: Run for **today's** partition
  - Schedule runs at 2:00 AM on Day D → processes partition D
  - Use when you need same-day processing

---

## Partition Change Sensors

Partition change sensors handle **late arrivals** and **data updates** by detecting changes in upstream sources and triggering re-materialization of affected partitions.

### Detectors

Detectors monitor source tables for changes and trigger re-processing:

```yaml
partition_change:
  detectors:
    - model: orders
      enabled: true
      lookback_days: 7
      offset_days: 1
      detect_source:
        source: ods
        table: orders
      partition_date_expr: order_datetime
      updated_at_expr: updated_at
      minimum_interval_seconds: 60
```

**Fields**:
- `model`: dbt model to monitor (required)
- `enabled`: Whether detector is active (default: `true`)
- `lookback_days`: How far back to check for changes (default: 7)
- `offset_days`: Partition offset (default: 1)
- `detect_source`: Source table to monitor
  - `source`: dbt source name
  - `table`: dbt source table name
- `partition_date_expr`: SQL column for partition date (e.g., `order_datetime`)
- `updated_at_expr`: SQL column for watermark (e.g., `updated_at`)
- `minimum_interval_seconds`: Minimum time between sensor evaluations (default: 60)
- `name`: Custom sensor name (auto-generated if omitted)
- `job_name`: Target job name (auto-derived if omitted)

**CLI equivalent**:
```bash
dbt-dagsterizer meta partition-change detector \
  --model orders \
  --enabled \
  --detect-source ods.orders \
  --partition-date-expr order_datetime \
  --updated-at-expr updated_at \
  --lookback-days 7 \
  --offset-days 1
```

**How it works**:
1. Sensor queries `max(updated_at_expr)` grouped by partition date
2. Compares result with stored cursor (watermark per partition)
3. If watermark increased, schedules re-materialization for that partition
4. Deduplicates using per-partition watermark cursor

### Impact Range

Expand detected changes to neighboring partitions (useful for rolling metrics):

```yaml
partition_change:
  detectors:
    - model: orders
      enabled: true
      lookback_days: 7
      offset_days: 1
      detect_source:
        source: ods
        table: orders
      partition_date_expr: order_datetime
      updated_at_expr: updated_at
      impact:
        type: range
        start_offset_days: -1
        end_offset_days: 2
```

This schedules partitions `D-1` through `D+2` when partition `D` is detected as changed.

**Real-world example**: For a 7-day rolling metric, a late arrival on day `D-3` affects results for `D-3` through `D+3`:

```yaml
      impact:
        type: range
        start_offset_days: 0
        end_offset_days: 6
```

**CLI equivalent**:
```bash
dbt-dagsterizer meta partition-change detector \
  --model orders \
  --enabled \
  --detect-source ods.orders \
  --partition-date-expr order_datetime \
  --updated-at-expr updated_at \
  --lookback-days 7 \
  --offset-days 1 \
  --impact-type range \
  --impact-start-offset -1 \
  --impact-end-offset 2
```

### Propagators

Propagators trigger downstream jobs when upstream partitions materialize:

```yaml
partition_change:
  propagators:
    - upstream_model: orders
      enabled: true
      name: facts_from_orders_partitions_sensor
      minimum_interval_seconds: 30
      targets:
        - job_name: daily_facts_job
        - job_name: country_reports_job
```

**Fields**:
- `upstream_model`: dbt model to monitor (required)
- `enabled`: Whether propagator is active (default: `true`)
- `minimum_interval_seconds`: Minimum time between evaluations (default: 30)
- `name`: Custom sensor name (auto-generated if omitted)
- `targets`: List of downstream jobs to trigger
  - `job_name`: Target job name

**CLI equivalent**:
```bash
dbt-dagsterizer meta partition-change propagator \
  --model orders \
  --enabled \
  --name facts_from_orders_partitions_sensor \
  --minimum-interval-seconds 30 \
  --targets daily_facts_job,country_reports_job
```

**How it works**:
1. Sensor watches for new `AssetMaterialization` events from upstream model
2. When a partition materializes, extracts partition key
3. Triggers downstream jobs for that specific partition
4. Uses relation-based AssetKeys for consistent event lookup

---

## Complete Example

Here's a realistic `dagsterization.yml` for a multi-layer dbt project:

```yaml
version: 1

# Partition assignments
partitions:
  daily:
    - orders
    - customers
    - fact_orders_daily
    - fact_customer_orders_daily

# Per-model asset jobs (dwd layer)
asset_jobs:
  - orders
  - customers
  - real_time_events

# Grouped jobs (dws layer)
jobs:
  daily_facts_job:
    models:
      - fact_orders_daily
      - fact_customer_orders_daily
    include_upstream: false
    partitions: daily

# Schedules
schedules:
  orders_daily_schedule:
    type: daily_at
    job_name: dbt_orders_asset_job
    hour: 2
    minute: 0
    lookback_days: 0
    offset_days: 1
    enabled: true
  
  daily_facts_schedule:
    type: daily_at
    job_name: daily_facts_job
    hour: 3
    minute: 30
    lookback_days: 0
    offset_days: 1
    enabled: true

# Partition change sensors
partition_change:
  detectors:
    - model: orders
      enabled: true
      lookback_days: 7
      offset_days: 1
      detect_source:
        source: ods
        table: orders
      partition_date_expr: order_datetime
      updated_at_expr: updated_at
      impact:
        type: range
        start_offset_days: 0
        end_offset_days: 6
  
  propagators:
    - upstream_model: orders
      enabled: true
      name: facts_from_orders_partitions_sensor
      minimum_interval_seconds: 30
      targets:
        - job_name: daily_facts_job
```

```yaml
version: 1

# Partition assignments
partitions:
  dynamic:
    - name: country_code
      initial_partition_keys: ['US', 'GB', 'DE', 'JP', 'AU']
      models:
        - orders_by_country
        - country_summary

# Grouped jobs (dws layer)
jobs:
  country_reports_job:
    models:
      - orders_by_country
      - country_summary
    include_upstream: true
    partitions: dynamic:country_code

# Schedules
schedules:
  country_reports_schedule:
    type: daily_at
    job_name: country_reports_job
    hour: 4
    minute: 0
    lookback_days: 0
    offset_days: 1
    enabled: true
```

---

## Managing dagsterization.yml

### CLI Commands

The recommended workflow uses the CLI to manage the file:

```bash
# Initialize
dbt-dagsterizer meta init

# Set partitions
dbt-dagsterizer meta partition --models orders --type daily

# Create asset job
dbt-dagsterizer meta asset-job --models orders

# Create grouped job
dbt-dagsterizer meta job --models fact_orders,fact_customers --name daily_facts --partitions daily

# Create schedule
dbt-dagsterizer meta schedule --models orders --hour 2 --minute 0

# Create partition change detector
dbt-dagsterizer meta partition-change detector \
  --model orders \
  --detect-source ods.orders \
  --partition-date-expr order_datetime \
  --updated-at-expr updated_at

# Create propagator
dbt-dagsterizer meta partition-change propagator \
  --model orders \
  --targets daily_facts_job

# Validate
dbt-dagsterizer meta validate
```

### Manual Editing

You can also edit the file directly. The CLI and manual editing are fully compatible.

### Version Control

Commit `dagsterization.yml` to your dbt project repository. Changes should be reviewed alongside model changes.

---

## Best Practices

1. **Use CLI for common operations**: Less error-prone than manual YAML editing
2. **Validate after changes**: Run `dbt-dagsterizer meta validate` to catch issues early
3. **Start simple**: Begin with `daily` partitions, add complexity as needed
4. **Use grouped jobs for DWS**: Group related fact models together
5. **Use asset jobs for DWD**: Fine-grained control for core models
6. **Enable partition change detectors**: Handle late arrivals automatically
7. **Test offset_days**: Use `offset_days: 1` (yesterday) for most daily jobs
8. **Dynamic partitions**: Start with small `initial_partition_keys` and expand as needed
9. **Keep it in Git**: Review orchestration changes alongside model changes

---

## Troubleshooting

### DagsterInvariantViolationError: Cannot mix partition types

If you see an error like:
```
DagsterInvariantViolationError: Cannot subset assets with different partition definitions
```

This happens when you try to mix different partition types in a single job or asset group.

**Solution**:
- Ensure each job uses models with the **same partition type**
- `dbt-dagsterizer` automatically isolates partition types into separate asset groups
- If you're defining manual jobs, don't mix daily, dynamic, and unpartitioned models together
- Check your `dagsterization.yml` — each model should only appear in one partition section

### Model not appearing in Dagster UI

- Ensure model is in `asset_jobs` or referenced in `jobs`
- Run `dbt parse` to regenerate `manifest.json`
- Check `dagsterization.yml` syntax with `dbt-dagsterizer meta validate`

### Schedule not running

- Verify `enabled: true`
- Check job name matches exactly
- Ensure partition type matches job partition config
- Verify environment variables are set (e.g., `DAGSTER_DAILY_PARTITIONS_START_DATE`)

### Partition change sensor not triggering

- Check `detect_source` references valid dbt source
- Verify `updated_at_expr` column exists and has data
- Ensure StarRocks connectivity is working
- Check sensor is enabled (`enabled: true`)
- Review sensor logs in Dagster UI

### Dynamic partitions not syncing

- Bootstrap sensor should run automatically at startup
- Verify `initial_partition_keys` is non-empty
- Check sensor is running (not stopped)
- Review sensor logs for errors

---

## See Also

- [CLI Reference](../../concepts/cli.md)
- [Developer Workflow](../../templates/dagster-dbt-starrocks-code-location/developer_workflow.md)
- [Execution Model](../../concepts/execution-model.md)
- [Overview](../../concepts/overview.md)

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

- **Partitioning strategy** for each model (daily, or unpartitioned)
- **Job definitions** (per-model asset jobs or grouped jobs)
- **Schedules** (when jobs should run)
- **Partition change sensors** (detectors and propagators for handling late arrivals)
- **Replication** (optional StarRocks-to-SQL Server data replication via dlt)
- **SSRS reports** (optional triggering of SQL Server Reporting Services subscriptions after models materialize)

## Top-Level Structure

```yaml
version: 1                          # Config schema version
timezone: UTC                       # Global schedule execution timezone
partitions:
  daily: []
  daily_config:
    include_current_day_partition: false
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
replication:                        # Optional: StarRocks -> SQL Server replication
  enabled: false
  entries: []
ssrs_reports: []                    # Optional: SSRS subscription triggers
```

---

## Timezone

The top-level `timezone` field sets the execution timezone for **all** schedules declared in the file.

```yaml
version: 1
timezone: Asia/Macau

schedules:
  orders_daily_schedule:
    type: daily_at
    job_name: dbt_orders_asset_job
    hour: 2
    minute: 0
```

**Fields**:
- `timezone` (string, default: `UTC`): IANA timezone name used as `execution_timezone` for every schedule.
  - Examples: `UTC`, `America/New_York`, `Europe/Berlin`, `Asia/Hong_Kong`, `Asia/Macau`
  - When omitted, all schedules default to UTC.

**CLI equivalent**:
```bash
dbt-dagsterizer meta timezone --timezone "Asia/Shanghai"
```

> **Note**: This is a global setting. All schedules in the file share the same execution timezone. The `hour` and `minute` values in each schedule are interpreted in this timezone.

---

## Partitions

The `partitions` section assigns partitioning strategies to dbt models. Each model can only belong to one partition type.

### Valid Partition Types

| Type | Description | Env Var Required | Asset Group Isolation |
|------|-------------|------------------|----------------------|
| `daily` | One partition per day | `DAGSTER_DAILY_PARTITIONS_START_DATE` (YYYY-MM-DD) | ✅ Separate group |

Daily partition parameters can be configured in `partitions.daily_config` (see [Daily Partition Configuration](#daily-partition-configuration)). No environment variable override is supported.

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

### Daily Partition Configuration

Configure parameters for the `DailyPartitionsDefinition` used by all daily-partitioned models:

```yaml
partitions:
  daily:
    - orders
    - customers
  daily_config:
    include_current_day_partition: true
```

**Fields**:
- `include_current_day_partition` (bool, default: `false`): Whether today's partition should be available in the `DailyPartitionsDefinition`.
  - `false` (default): Only partitions ending *before* the current time are available (equivalent to `end_offset: 0`).
  - `true`: Today's partition is also available (equivalent to `end_offset: 1`, useful for same-day processing).

> **Note**: This is different from schedule `offset_days`, which controls *which partition* a schedule targets. `include_current_day_partition` controls the *set of available partitions* in the partition definition itself.

**CLI equivalent**:
```bash
dbt-dagsterizer meta partition-config --include-current-day-partition
```


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
```

**Fields**:
- `models`: List of dbt models in this job (required)
- `include_upstream`: Whether to include upstream dependencies (default: `false`)
- `partitions`: Partition strategy for the job (`daily`, `unpartitioned`)

**CLI equivalent**:
```bash
dbt-dagsterizer meta job \
  --models fact_orders_daily,fact_customer_orders_daily,fact_revenue_daily \
  --name daily_facts_job \
  --partitions daily
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
- `type`: Schedule type (only `daily_at` is currently supported)
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
- `lookback_days`: How far back to check for changes (default: 0)
- `offset_days`: Partition offset (default: 0)
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

## Replication

Replication copies transformed dbt model data from StarRocks to Microsoft SQL Server using [dlt](https://dlthub.com) (Data Load Tool). It is an **optional** feature, disabled by default.

### How it works

1. Each replicated dbt model gets a **replication asset** (`replicate_<model>`) in the `replication` asset group
2. The replication asset depends on the dbt model asset, so it executes **after** the dbt build completes
3. For partitioned models, replication is **partition-aware**: only the materialized partition is copied (filtered by `partition_column`)
4. For unpartitioned models, the entire table is copied
5. dlt handles schema inference, data type mapping, and incremental loading

### Configuration

```yaml
replication:
  enabled: true
  entries:
    - model: orders
      enabled: true
      destination_table: orders
      destination_schema: dbo
      write_disposition: replace
      partition_column: order_date
      primary_key: order_id    # Single column for PK constraint in destination
```

**Fields**:

| Field | Description | Default |
|-------|-------------|---------|
| `model` | dbt model name to replicate (required) | — |
| `enabled` | Whether this entry is active | `true` |
| `destination_table` | Target table name in SQL Server | model name |
| `destination_schema` | Target schema in SQL Server | `dbo` |
| `write_disposition` | dlt write disposition: `append`, `replace`, or `merge` | `replace` |
| `partition_column` | Column to filter by for partition-aware replication | (none) |
| `primary_key` | Single column name used as the primary key in the destination table (applies to all write dispositions) | (none) |

**Global toggle**:

- `replication.enabled` (bool, default: `false`): Master switch for the entire replication feature. When `false`, no replication assets or jobs are created.

### Partition-Aware Replication

When a dbt model is partitioned (daily) and `partition_column` is set:

- The replication asset uses the **same partition definition** as the dbt model
- Only the materialized partition's data is copied (filtered via `WHERE partition_column = '<partition_key>'`)
- This avoids full table scans on each partition materialization

When `partition_column` is not set on a partitioned model, a **full table copy** is performed on each materialization (with a validation warning).

### Write Dispositions

The `write_disposition` field controls how data is loaded into SQL Server:

- **`append`**: Appends new data to the existing table without modifying existing rows
- **`replace`**: Truncates the table before loading, replacing all existing data
- **`merge`**: Updates existing rows based on `primary_key` and inserts new rows. You **must** set `primary_key` to a column name that uniquely identifies a row. dlt uses this column to perform an `UPDATE` for matching rows and an `INSERT` for new rows.

> **Note**: When `primary_key` is set, dlt creates a primary key constraint in the destination SQL Server table. This applies to all write dispositions (`append`, `replace`, and `merge`).

**When to use each**:
- Use `append` for incremental loads where you only add new records
- Use `replace` for full refreshes or when the source is the complete truth
- Use `merge` when you need to update existing records (e.g., correcting historical data)

### Merge Replication

When `write_disposition: merge` is set, you must also provide `primary_key` — a single column name that uniquely identifies each row. dlt uses this column as the primary key to decide whether to update an existing row or insert a new one.

```yaml
replication:
  enabled: true
  entries:
    # Merge with primary key
    - model: orders
      enabled: true
      destination_table: orders
      destination_schema: dbo
      write_disposition: merge
      primary_key: order_id
      partition_column: order_date

    # Another merge example
    - model: order_items
      enabled: true
      destination_table: order_items
      destination_schema: dbo
      write_disposition: merge
      primary_key: order_item_id
```

> **Note**: `primary_key` can be used with any write disposition (`append`, `replace`, or `merge`). When set, dlt creates a primary key constraint in the destination SQL Server table.

### SQL Server Connection

SQL Server connection details are configured via environment variables (like StarRocks):

```bash
SQLSERVER_HOST=sqlserver.example.com
SQLSERVER_PORT=1433
SQLSERVER_USER=sa
SQLSERVER_PASSWORD=********
SQLSERVER_DATABASE=replication_target
SQLSERVER_DRIVER="ODBC Driver 18 for SQL Server"
```

### CLI equivalent

```bash
dbt-dagsterizer meta replication entry \
  --model orders \
  --enabled \
  --destination-table orders \
  --destination-schema dbo \
  --write-disposition replace \
  --partition-column order_date

# With primary key
dbt-dagsterizer meta replication entry \
  --model orders \
  --enabled \
  --destination-table orders \
  --destination-schema dbo \
  --write-disposition merge \
  --primary-key order_id \
  --partition-column order_date
```

---

## SSRS Reports

SSRS reports trigger pre-configured SQL Server Reporting Services (SSRS) subscriptions after their upstream dbt models materialize. It is an **optional** feature; no reports are configured by default (`ssrs_reports: []`).

### How it works

1. Each entry in `ssrs_reports` gets a **report asset** (asset key `ssrs/<name>`) in the `reports` asset group
2. The report asset depends on the dbt model asset, so it executes **after** the dbt build completes
3. When `enabled: true`, the asset uses `AutomationCondition.eager()` and auto-materializes as soon as the upstream model materializes; when `enabled: false`, it must be materialized manually
4. On materialization, the SSRS agent resource resolves the **SQL Server Agent job** backing the subscription by matching `subscription_description` against `ReportServer.dbo.Subscriptions`, then starts the job via `msdb.dbo.sp_start_job`
5. Report rendering and delivery are handled entirely by SSRS according to the subscription's own configuration

The asset records `subscription_description` and `agent_job_name` as materialization metadata in the Dagster UI.

### Prerequisites

- An SSRS subscription must already exist on the report server, with a **unique** `Description`. The lookup fails if the description matches zero or more than one SQL Server Agent job.
- The `pymssql` package must be installed in the code location (a clear runtime error is raised if it is missing).
- The SQL Server instance hosting the `ReportServer` and `msdb` databases must be reachable from Dagster (see [SSRS Agent Connection](#ssrs-agent-connection) below).

### Configuration

```yaml
ssrs_reports:
  - name: daily_sales
    model: fct_sales_daily
    subscription_description: "Daily sales report subscription"
    enabled: true
```

**Fields**:

| Field | Description | Default |
|-------|-------------|---------|
| `name` | Unique report name; used for the asset key `ssrs/<name>` (non-alphanumeric characters are replaced with `_`) (required) | — |
| `model` | dbt model name that triggers the report; must exist in the manifest (required) | — |
| `subscription_description` | Description of the SSRS subscription in the ReportServer catalog; used to look up its SQL Server Agent job (required, must be unique on the report server) | — |
| `enabled` | When `true`, the report auto-materializes eagerly after the upstream model | `false` |

### SSRS Agent Connection

The SSRS agent client connects to the SQL Server that hosts the `ReportServer` and `msdb` databases. Connection details are loaded from environment variables (like the StarRocks and replication SQL Server resources):

```bash
SSRS_DB_HOST=sqlserver.example.com
SSRS_DB_PORT=1433
SSRS_DB_USERNAME=ssrs_trigger
SSRS_DB_PASSWORD=********
SSRS_DB_DATABASE=msdb
SSRS_DB_TIMEOUT_SECONDS=60
```

| Variable | Description | Default |
|----------|-------------|---------|
| `SSRS_DB_HOST` | SQL Server host/IP for the ReportServer/msdb instance (**required** when `ssrs_reports` entries exist) | — |
| `SSRS_DB_PORT` | SQL Server port | `1433` |
| `SSRS_DB_USERNAME` | SQL Server login username; when empty, the connection is made without explicit credentials | (empty) |
| `SSRS_DB_PASSWORD` | SQL Server login password | (empty) |
| `SSRS_DB_DATABASE` | Database used for the connection | `msdb` |
| `SSRS_DB_TIMEOUT_SECONDS` | Connection and login timeout in seconds | `60` |

Notes:

- The configured login needs read access to `ReportServer.dbo.Catalog`, `ReportServer.dbo.Subscriptions`, `ReportServer.dbo.ReportSchedule`, and `msdb.dbo.sysjobs`, plus permission to execute `msdb.dbo.sp_start_job`.
- If `SSRS_DB_HOST` is not set, materializing a report asset fails with `SSRS_DB_HOST is not configured`.

### CLI equivalent

```bash
dbt-dagsterizer meta report \
  --name daily_sales \
  --model fct_sales_daily \
  --subscription-description "Daily sales report subscription" \
  --enabled

# Remove a report entry
dbt-dagsterizer meta report-delete --name daily_sales
```

---

## Complete Example

Here's a realistic `dagsterization.yml` for a multi-layer dbt project:

```yaml
version: 1
timezone: Asia/Macau

# Partition assignments
partitions:
  daily:
    - orders
    - customers
    - fact_orders_daily
    - fact_customer_orders_daily
  daily_config:
    include_current_day_partition: true

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
  
  propagators:
    - upstream_model: orders
      enabled: true
      name: facts_from_orders_partitions_sensor
      minimum_interval_seconds: 30
      targets:
        - job_name: daily_facts_job

# Optional: Replication to SQL Server
replication:
  enabled: false
  entries:
    - model: orders
      enabled: true
      destination_table: orders
      destination_schema: dbo
      write_disposition: replace
      partition_column: order_date
      primary_key: order_id

# Optional: SSRS subscription triggers
ssrs_reports:
  - name: daily_sales
    model: fact_orders_daily
    subscription_description: "Daily sales report subscription"
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

# Configure daily partition parameters
dbt-dagsterizer meta partition-config --include-current-day-partition

# Set global schedule timezone
dbt-dagsterizer meta timezone --timezone "Asia/Macau"

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

# Configure replication entry
dbt-dagsterizer meta replication entry \
  --model orders \
  --enabled \
  --destination-table orders \
  --write-disposition replace \
  --partition-column order_date

# Configure SSRS report trigger
dbt-dagsterizer meta report \
  --name daily_sales \
  --model fact_orders_daily \
  --subscription-description "Daily sales report subscription" \
  --enabled

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
8. **Keep it in Git**: Review orchestration changes alongside model changes

---

## Troubleshooting

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

---

## See Also

- [CLI Reference](../../concepts/cli.md)
- [Developer Workflow](../../templates/dagster-dbt-starrocks-code-location/developer_workflow.md)
- [Execution Model](../../concepts/execution-model.md)
- [Overview](../../concepts/overview.md)

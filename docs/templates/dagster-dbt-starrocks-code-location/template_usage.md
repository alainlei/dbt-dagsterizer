# Template usage guide (Dagster + dbt + StarRocks)

This template renders a Dagster code location that orchestrates a dbt project on StarRocks.

If you are developing the template itself, the template source is embedded in `dbt-dagsterizer` under `dbt_dagsterizer/project_templates/`.

## What the template includes

- dbt models (`dbt_project/`) that build `dwd/*` and `dws/*`
- a Dagster code location (`src/<package_name>/`) that loads dbt assets
- an ODS observation loop (observable source assets + a schedule)
- declarative automation (automation condition sensor + a dbt translator)

Notes:

- On startup, Dagster ensures `dbt_project/target/manifest.json` exists by running `dbt parse` if needed (and runs `dbt deps` when `packages.yml` contains packages). Control this via `LUBAN_DBT_PREPARE_ON_LOAD` (defaults to `1`).
- The daily partitions start date is controlled by `DAGSTER_DAILY_PARTITIONS_START_DATE` and must be set when using daily partitions. Monthly partitions use `DAGSTER_MONTHLY_PARTITIONS_START_DATE` (format `YYYY-MM-DD`), and hourly partitions use `DAGSTER_HOURLY_PARTITIONS_START_DATE`.
- The dbt models in this template expect to run in Dagster partitioned mode and will fail fast at execution time if required dbt vars are missing.
- For partitioned runs, Dagster passes dbt variables `min_date`/`max_date` and `min_datetime`/`max_datetime` based on the partition time window.
- In this template, `dbt_project/dagsterization.yml` partitioning (`daily`) means "Dagster orchestration partitioning" (processing slices / rebuild scope). It does not imply StarRocks physical table partitioning.
- The code location resolves `dbt_project/` paths relative to the repository root, not the process working directory. This keeps dbt config stable in containers and when executed via Dagster gRPC.

## How automation works

The template uses observable source assets to detect changes in ODS tables, and automation conditions to trigger downstream dbt models.

### 1) Observe ODS tables (data version)

Key pieces:

- ODS observation config: `dbt_project/models/dwd/sources.yml` (`meta.luban.observe`)
- ODS observable sources wiring: provided by `dbt_dagsterizer.assets.sources`
- Observation job/schedule: provided by `dbt_dagsterizer.jobs.sources` and `dbt_dagsterizer.schedules.sources`

The observable source assets compute a `DataVersion` in one of two ways:

1. Column-based watermark:

```sql
select max(<watermark_column>) from <ods_db>.<table>
```

2. Custom SQL watermark:

```sql
<watermark_sql>
```

Notes:

- `watermark_sql` must return a single scalar value because the result is passed through `query_scalar(...)` and stored as the source asset's `DataVersion`.
- If both `watermark_column` and `watermark_sql` are configured, `watermark_sql` takes precedence.
- `watermark_column` uses the resolved source database name automatically. `watermark_sql` is executed as-is, so include any required database/schema qualification in the SQL itself.
- dbt sources may declare `identifier` separately from `name` (for example, when the upstream table uses a case-sensitive physical name like `OrdersFact` or a cross-catalog dotted path `CatalogName.Schema.Orders` while your logical dbt name stays simple). dbt-dagsterizer uses `name` for Dagster source asset key resolution (matching `dagster_dbt`'s output-name indexing), while still using `identifier` (or falling back to `name`) for the SQL `FROM` clause in watermark queries. This lets you keep readable logical names while preserving the exact physical table path.

When that value changes, Dagster records a new observation event for the source asset.

### 2) Trigger downstream dbt models

Key pieces:

- dbt asset loading: provided by `dbt_dagsterizer.assets.dbt`

The dbt translator assigns `AutomationCondition.eager()` to models that match one of these rules:

- the model name matches a table listed in observable source metadata
- the model is daily-partitioned and `LUBAN_PARTITION_CHANGE_PROPAGATOR_MODE=eager`
- the model carries the `dim` tag
- the model carries the `automation_table` tag

When Dagster detects an upstream change, the automation sensor can request a run to materialize the affected downstream assets.

In the default template layout, this commonly means:

- source-driven refresh for models fed by observed ODS tables
- eager refresh for dimension models tagged `dim`
- eager refresh for daily-partitioned models when the propagator runs in `eager` mode

These rules are driven by model metadata and partition config rather than by whether a model lives under `dwd/` or `dws/`.

### 3) Partition-change (late arrivals)

Conceptually, partition-aware late arrivals are handled as two separate steps:

- Detector (source-driven): finds impacted partition keys and requests upstream partition runs (example: `orders`).
- Propagator (event-driven): listens for upstream materialization events and requests downstream partition runs (example: daily facts).

This keeps ordering correct: detectors request upstream work; successful upstream materializations produce events; propagators react to those events.

You can change this behavior via `LUBAN_PARTITION_CHANGE_PROPAGATOR_MODE`:

- `sensor` (default): downstream partitions are triggered after upstream partitions materialize
- `eager`: rely on automation conditions instead of propagation sensors

## Configuration

Orchestration intent is declared in `dbt_project/dagsterization.yml` and combined with dbt metadata discovered from `manifest.json`.

See `developer_workflow.md` for the supported `dagsterization.yml` schema and developer workflow.

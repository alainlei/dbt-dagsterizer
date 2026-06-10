# Developer workflow (Dagster + dbt + StarRocks)

This template is designed so most orchestration configuration is declared in dbt and discovered via `dbt_project/target/manifest.json`.

## Philosophy (dbt-first, Dagster-orchestrated)

This template follows a division of responsibilities that scales well for warehouse-style transformations:

- dbt is the transformation layer: SQL models, tests, sources, and selection primitives (tags/groups) live in dbt.
- Dagster is the orchestration and observability layer: schedules, sensors, retries, backfills, run history, and dependency-aware execution live in Dagster.

In practice, this means:

- You declare orchestration intent close to the models (dbt `meta` and tags), not in ad-hoc Python lists.
- Dagster reads the dbt manifest and turns that intent into jobs/schedules/sensors.
- Python config exists as an escape hatch for cases where dbt metadata is not expressive enough.

### Why declare orchestration intent in dbt meta?

- Single source of truth for orchestration intent (`dbt_project/dagsterization.yml`).
- Reviewability: changes to orchestration intent are part of dbt model reviews.
- Portability: the manifest is a stable interface between dbt and Dagster.

### What stays out of dbt meta

Keep model execution details in dbt config (materialization strategy, incremental keys), and keep runtime infrastructure in GitOps (resources, env vars, secrets).

## Source of truth

- dbt source metadata (observation): `meta.luban.observe` in `sources.yml`
- dbt model metadata (dbt-native): `schema.yml` in `dbt_project/models/**`
- Orchestration intent: `dbt_project/dagsterization.yml` (written/updated by `dbt-dagsterizer`)

## Quick start

1. Ensure `dbt_project/target/manifest.json` exists.
   - Local: keep `LUBAN_DBT_PREPARE_ON_LOAD=1` (default).
   - CI or explicit: run `dbt deps` and `dbt parse`.
   - Or: use `dbt-dagsterizer meta init --parse` to create the orchestration file and refresh the manifest.
2. Start the code location and verify `Definitions` load.

## Editing orchestration intent with the CLI

Instead of writing orchestration intent in model SQL, the recommended workflow is:

1. Run `dbt-dagsterizer meta init`.
2. Use `dbt-dagsterizer meta job|schedule|partition|asset-job|partition-change ...` to update `dbt_project/dagsterization.yml`.
3. Use `--parse` (or run `dbt parse`) so Dagster picks up the changes.

CLI reference: `../../concepts/cli.md`.

## Creating jobs

### One model per asset job (recommended for `dwd`)

Enable a per-model asset job in `dagsterization.yml`.

```yaml
asset_jobs:
  - orders

partitions:
  daily:
    - orders
```

CLI equivalent:

```bash
dbt-dagsterizer meta asset-job --models orders
dbt-dagsterizer meta partition --models orders --type daily
```

The generated job name is `dbt_<model>_asset_job`.

### Grouped job (recommended for `dws`)

Define a grouped job in `dagsterization.yml`.

```yaml
jobs:
  daily_facts_job:
    models:
      - fact_orders_daily
      - fact_customer_orders_daily
    include_upstream: false
    partitions: daily
```

CLI equivalent:

```bash
dbt-dagsterizer meta job \
  --models fact_orders_daily,fact_customer_orders_daily \
  --name daily_facts_job \
  --partitions daily
```

All models listed in the same job are grouped into the same asset job.

Defaults:

- `include_upstream`: `false`
- `partitions`: can be set per job (`daily|unpartitioned`)

### Asset identity for generated jobs

Auto-generated dbt asset jobs now select assets by relation-based AssetKeys instead of logical model names.

- AssetKey format: `dbt/<database>/<schema>/<identifier>` (empty components are omitted)
- Goal: keep asset identity aligned with the physical dbt relation
- Benefit: different code locations can refer to the same physical relation without drifting on asset identity

Example:

- dbt model `orders`
- relation metadata: `database=warehouse`, `schema=dwd`, `identifier=orders`
- generated Dagster AssetKey: `dbt/warehouse/dwd/orders`

For auto-generated jobs, you do not need to do anything: `dbt-dagsterizer` derives these keys from the manifest automatically.

If you define manual/custom Python `DBT_JOB_SPECS`, prefer relation-based keys. Legacy `["dbt", "<model>"]` selections are still upgraded at runtime for compatibility when the model exists in the manifest.

Manual Python specs are an escape hatch and are intentionally discouraged. They can be brittle across upgrades because the spec shape must remain backward compatible as features evolve. Prefer `dagsterization.yml` and the CLI for jobs/schedules/partition-change configuration whenever possible.

## Dagster asset grouping

Dagster group names are derived from dbt project layout and resource type:

- For dbt models, the group name is the first folder under `models/`
- For non-model dbt resources, the group falls back to the dbt resource type
- If folder derivation is not available, dbt FQN is used as a fallback

Examples:

- `models/dwd/orders.sql` -> group `dwd`
- `models/dws/fact_orders_daily.sql` -> group `dws`
- `sources.yml` source assets -> group `source`

This makes the Dagster UI reflect the dbt project structure more naturally without additional Python configuration.

## Creating schedules

Schedules are declared on the model that represents the job’s anchor.

Define a schedule in `dagsterization.yml`:

```yaml
schedules:
  orders_anchor_daily_schedule:
    type: daily_at
    job_name: dbt_orders_asset_job
    hour: 1
    minute: 0
    lookback_days: 0
    offset_days: 1
    enabled: true
```

CLI equivalent:

```bash
dbt-dagsterizer meta schedule \
  --models orders \
  --name orders_anchor_daily_schedule \
  --hour 1 \
  --minute 0 \
  --lookback-days 0 \
  --offset-days 1 \
  --enabled
```

Defaults:

- `offset_days`: `1` (run yesterday’s partition)
- `lookback_days`: `0`

Notes:

- Use `offset_days: 1` for the common “process yesterday” daily pattern.
- Use `offset_days: 0` when the schedule should target today’s partition.
- `offset_days` is part of `dagsterization.yml`, validated by the CLI, and preserved by auto-generated schedule specs.

## Partition-change sensors (late arrivals)

### Detector

Declare a partition-change detector entry in `dagsterization.yml`.

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
```

CLI equivalent:

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

Defaults:

- `job_name`: derived from the model’s job config (same rule as schedules)
- `minimum_interval_seconds`: `60`
- `name`: `<model>_partition_change_sensor`

Notes:

- Detectors dedupe per-partition using a watermark cursor (max(`updated_at_expr`) within the detector window). A partition is scheduled only when its watermark increases compared to the cursor.
- The sensor cursor stores a per-partition watermark map. If you change detector logic/config and want a fresh evaluation, reset the sensor cursor in Dagster.

Optional: impact range

`impact` expands a detected partition into neighbor partitions to re-materialize. This is useful when a late arrival in one partition can affect adjacent partitions (for example windowed aggregations).

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

This example schedules `D-1..D+2` when `D` is detected as changed (clamped to the detector window).

Concrete example (real-world)

If a daily-partitioned model computes rolling metrics (for example “7-day active customers ending on D” or “7-day revenue ending on D”), then partition `D` typically depends on source data from `D-6..D`. A late arrival for day `D-3` changes the correct results for `D-3`, `D-2`, `D-1`, … up through `D+3`. In that case, configure:

- `start_offset_days: 0`
- `end_offset_days: 6`

to re-materialize `D..D+6` whenever `D` changes.

### Propagation

Declare propagation on the upstream model.

Declare propagation in `dbt_project/dagsterization.yml`:

```yaml
partition_change:
  propagators:
    - upstream_model: orders
      enabled: true
      name: facts_from_orders_partitions_sensor
      minimum_interval_seconds: 30
      targets:
        - job_name: daily_facts_job
```

CLI equivalent:

```bash
dbt-dagsterizer meta partition-change propagator \
  --model orders \
  --enabled \
  --name facts_from_orders_partitions_sensor \
  --minimum-interval-seconds 30 \
  --targets daily_facts_job
```

Notes:

- `enabled: false` still registers the sensor definition but defaults it to STOPPED.
- The template enables propagation by default on the `orders` model; set `enabled: false` if you want to onboard gradually.
- `LUBAN_PARTITION_CHANGE_PROPAGATOR_MODE=eager` disables propagation sensors entirely.
- Propagation sensors only react to new upstream materializations by default. To replay recent upstream partitions after enabling a sensor, set `LUBAN_PARTITION_CHANGE_PROPAGATOR_CATCHUP_DAYS` (for example `7`) before the first time the sensor runs (or after resetting its cursor).
- Auto-generated propagation specs use the same relation-based AssetKeys as dbt assets, so upstream event lookup remains aligned after the relation-key migration.
- If you define manual Python `PARTITION_CHANGE_PROPAGATION_SPECS`, prefer `upstream_model_relation=["dbt", "<database>", "<schema>", "<identifier>"]`. Legacy model-name-only specs are upgraded at runtime for compatibility when the model can be resolved from the manifest.

## Source observation (DataVersion)

Define source observation under `meta.luban.observe` for the dbt source table.

Use `watermark_column` for the common case where `max(...)` over a single column is enough:

```yaml
sources:
  - name: ods
    tables:
      - name: customers
        meta:
          luban:
            observe:
              watermark_column: ods_updated_at
```

Use `watermark_sql` when you need custom logic, for example a cast, a filtered query, or a derived scalar:

```yaml
sources:
  - name: ods
    tables:
      - name: customers
        meta:
          luban:
            observe:
              watermark_sql: |
                select max(coalesce(updated_at, created_at))
                from ods.customers
                where is_deleted = 0
```

Rules:

- At least one of `watermark_column` or `watermark_sql` must be set.
- `watermark_sql` must return a single scalar value; the value is converted to a string and used as the source asset `DataVersion`.
- If both are set, `watermark_sql` takes precedence.
- `watermark_column` uses the resolved source database name automatically. `watermark_sql` runs as written, so include any needed database/schema qualification directly in the query.

This drives observable source assets and the observation job/schedule.

## Optional overrides (escape hatch)

Rendered projects depend on the `dbt-dagsterizer` Python package. The recommended customization path is via `dagsterization.yml`.

If you need a custom escape hatch, the intended long-term approach is to expose explicit override hooks in `dbt_dagsterizer`. Until then, treat library forks as temporary.

Manual Python specs are an escape hatch and are intentionally discouraged. Prefer `dagsterization.yml` and the CLI first. If you must define manual specs, follow this guidance to reduce upgrade risk:

- Prefer `dagsterization.yml` and the CLI for jobs, schedules, partitions, and partition-change config.
- If you must define manual `DBT_JOB_SPECS`, use relation-based AssetKeys.
- If you must define manual `PARTITION_CHANGE_PROPAGATION_SPECS`, include `upstream_model_relation` explicitly.
- Runtime compatibility shims exist for legacy model-name-only specs, but relation-based definitions are the recommended steady state.

## Troubleshooting

- Missing manifest: ensure `LUBAN_DBT_PREPARE_ON_LOAD=1` or run `dbt deps` + `dbt parse`.
- Duplicate names: job and schedule names must be unique after grouping.
- dbt fails with "already exists": Dagster retries the dbt invocation once (after a short delay); if it persists, treat it as a real failure.
- Partition-change sensor finds no partitions: verify `updated_at_expr` and StarRocks connectivity.

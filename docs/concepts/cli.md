# dbt-dagsterizer CLI

This document describes the `dbt-dagsterizer` CLI.

The CLI exists to:

- Keep the Dagster code location mostly static.
- Let developers declare orchestration intent close to dbt models, in a reviewable YAML file.
- Validate that intent early (before Dagster fails at import/runtime).

## Entry points

- `dbt-dagsterizer ...`
- `python -m dbt_dagsterizer ...`

## Installation

The recommended way to install the CLI is via a managed tool install, so it is available across repositories:

```bash
uv tool install dbt-dagsterizer
```

Upgrade:

```bash
uv tool upgrade dbt-dagsterizer
```

Version:

```bash
dbt-dagsterizer --version
```

## Environment

Some dbt configs (e.g. sources) use `env_var(...)`. To make CLI behavior match Dagster runtime behavior, the CLI loads dotenv files before running `dbt parse`:

- `<repo>/.env`
- `dbt_project/.env`

Existing process environment variables are not overridden.

When `--prepare` is enabled, the CLI may run `dbt parse` to refresh `dbt_project/target/manifest.json`. It writes a sidecar file `dbt_project/target/.luban_manifest_inputs.json` so future runs can detect when a refresh is needed (for example dotenv mtime or dbt target changes).

Current refresh rules when `--prepare` is enabled:

- Manifest missing
- Dbt target changed
- Dotenv inputs changed (dotenv paths changed, dotenv removed, or dotenv mtime increased)

## Orchestration file

The CLI reads and writes orchestration intent in a single file:

- `dbt_project/dagsterization.yml`

This file is not a dbt schema YAML, and is intentionally kept outside `dbt_project/models/` so dbt will not parse it.

## Commands

### `project list-templates`

Lists the embedded cookiecutter templates available to `project init`.

```bash
dbt-dagsterizer project list-templates
```

### `project init`

Renders a runnable Dagster + dbt (StarRocks) code-location repo using the embedded cookiecutter template.

```bash
dbt-dagsterizer project init \
  --output-dir . \
  --project-name "Orders Analytics" \
  --namespace "metasync" \
  --schedule-timezone "Asia/Macau" \
  --author-name "You" \
  --author-email "you@example.com"
```

Notes:

- `--project-name` (or `--name`) is required.
- `app_name` and `package_name` are derived automatically from `--project-name`.
- `--output-name` controls the output folder name. If not provided, it defaults to a kebab-case name derived from `--project-name`.
- `--dagster-version` pins Dagster (and `dagster-webserver` in the dev dependency group) in the rendered `pyproject.toml`.
- `--dbt-dagsterizer-version` pins `dbt-dagsterizer` in the rendered `pyproject.toml`. If not provided, it defaults to the installed CLI version (when available) so generated projects align with the generator.
- `--local-dbt-dagsterizer-path` writes a `dbt-dagsterizer @ file://...` dependency into the rendered `pyproject.toml` (mutually exclusive with `--dbt-dagsterizer-version` and `--no-pin-dbt-dagsterizer`).
- `--no-pin-dbt-dagsterizer` leaves `dbt-dagsterizer` unpinned in the rendered `pyproject.toml` (mutually exclusive with `--dbt-dagsterizer-version`).
- `--namespace` is optional and is used as a prefix for generated defaults (for example OTEL service naming and StarRocks DB names).
- `--schedule-timezone` writes the initial `timezone` field into `dbt_project/dagsterization.yml`. Use an IANA timezone such as `UTC`, `Asia/Macau`, or `America/New_York`.
- `--include-docker` is optional; when enabled, includes a local StarRocks docker-compose file and docker-related Make targets.
- `--output-dir` controls where the project directory is created.
- `--force` overwrites an existing output directory.
- By default the rendered dbt project is an empty skeleton; use `--include-sample-dbt-project` to include sample models.

Examples:

- Keep the default schedule timezone:

```bash
dbt-dagsterizer project init --output-dir . --project-name "Orders Analytics"
```

- Start a new project with a non-UTC schedule timezone:

```bash
dbt-dagsterizer project init \
  --output-dir . \
  --project-name "Orders Analytics" \
  --schedule-timezone "Asia/Macau"
```

See also: [../observability.md](../observability.md).

### `project gen-gitops-env`

Generates a kustomize-style `app/` directory containing `configmap.yaml` and `secret.yaml` files under:

- `app/base/`
- `app/overlays/snd/`
- `app/overlays/prd/`

The command reads values from the rendered project’s `.env` file and writes the generated files into `.gitops-env/` by default (and adds it to `.gitignore`).

```bash
dbt-dagsterizer project gen-gitops-env
```

Resource naming:

- The ConfigMap/Secret names are derived from the rendered project’s `pyproject.toml` name, normalized to a Kubernetes-safe form (underscores become hyphens).
- `--dagster-home` defaults to `/tmp/dagster_home`. Override if your GitOps template mounts a different path.

### `meta validate`

Validates `dagsterization.yml` against `dbt_project/target/manifest.json`.

```bash
dbt-dagsterizer meta validate --prepare
```

Flags:

- `--prepare/--no-prepare`: when enabled, runs `dbt parse` if the manifest is missing or stale.

### `meta init`

Creates the orchestration YAML file if it does not exist.

```bash
dbt-dagsterizer meta init
```

Flags:

- `--path`: relative to the dbt project dir (default `dagsterization.yml`)
- `--force`: overwrite an existing file
- `--parse`: run `dbt parse` after writing

### `meta job`

Creates/updates a grouped asset job in `dagsterization.yml`.

```bash
dbt-dagsterizer meta job \
  --models fact_orders_daily,fact_customer_orders_daily \
  --name daily_facts_job \
  --include-upstream \
  --partitions daily
```

Selection:

- `--models`: comma-separated model names
- `--tag`: select all models in the manifest that already have a dbt tag

Flags:

- `--partitions`: `daily|hourly|monthly|unpartitioned|none` (`none` leaves the job's partitions unset)
- `--prepare`: only used when selecting by `--tag` (needs the manifest)
- `--parse`: run `dbt parse` after writing

### `meta schedule`

Creates/updates a schedule in `dagsterization.yml`.

```bash
dbt-dagsterizer meta schedule \
  --models orders \
  --name orders_daily_schedule \
  --hour 2 \
  --minute 0 \
  --lookback-days 3 \
  --offset-days 1 \
  --enabled
```

Flags:

- `--schedule-type`: `daily_at` (default), `hourly_at` or `monthly_at`
- `--hour`: required for `daily_at` and `monthly_at`; optional for `hourly_at` (defaults to `0`). Valid `0..23`
- `--lookback-days` / `--offset-days`: partition window for `daily_at` (`--offset-days 1` = yesterday, `0` = today)
- `--lookback-hours` / `--offset-hours`: partition window for `hourly_at` (`--offset-hours 1` = previous hour)
- `--day-of-month`: day the `monthly_at` schedule fires. Valid `1..28` (default `1`); higher values are rejected because cron never fires on a 29th-31st during a shorter month
- `--lookback-months` / `--offset-months`: partition window for `monthly_at` (`--offset-months 1` = previous month, `0` = current month)
- `--parse`: run `dbt parse` after writing

Only the flags matching `--schedule-type` are written. `meta validate` rejects a schedule that mixes granularities, such as `offset_months` on a `daily_at` schedule.

Monthly example:

```bash
dbt-dagsterizer meta schedule \
  --models fact_revenue_monthly \
  --name revenue_monthly_schedule \
  --schedule-type monthly_at \
  --day-of-month 1 \
  --hour 4 \
  --minute 0 \
  --offset-months 1 \
  --enabled
```

### `meta timezone`

Sets the global schedule execution timezone in `dagsterization.yml`.

```bash
dbt-dagsterizer meta timezone --timezone "Asia/Macau"
```

Examples:

- Switch an existing project back to UTC:

```bash
dbt-dagsterizer meta timezone --timezone "UTC"
```

- Set a project to run schedules in local business time:

```bash
dbt-dagsterizer meta timezone \
  --dbt-project-dir ./dbt_project \
  --timezone "Asia/Macau" \
  --no-prepare
```

Notes:

- The timezone is global for schedules generated from `dagsterization.yml`.
- Use IANA timezone names such as `UTC`, `Asia/Macau`, or `America/New_York`.
- Invalid timezone names are rejected by the CLI before the config is written.

### `meta partition`

Sets partitioning for selected models in `dagsterization.yml`.

```bash
dbt-dagsterizer meta partition \
  --models fact_orders_daily,fact_customer_orders_daily \
  --type daily
```

Flags:

- `--type`: `daily|hourly|monthly|unpartitioned`
- `--parse`: run `dbt parse` after writing

### `meta partition-config`, `meta hourly-config`, `meta monthly-config`

Configure the shared `DailyPartitionsDefinition`, `HourlyPartitionsDefinition`, or `MonthlyPartitionsDefinition` used by every model of that partition type.

```bash
dbt-dagsterizer meta partition-config --include-current-day-partition
dbt-dagsterizer meta hourly-config --include-current-hour-partition
dbt-dagsterizer meta monthly-config --include-current-month-partition
```

Notes:

- Each flag has a negated form (`--no-include-current-day-partition`, `--no-include-current-hour-partition`, `--no-include-current-month-partition`), which sets the Dagster `end_offset` to `0`.
- These control the *set of available partitions*, not which partition a schedule targets — that is what `--offset-days` / `--offset-hours` / `--offset-months` do.
- Partitioned models still need the matching `DAGSTER_*_PARTITIONS_START_DATE` environment variable at load time.

### `meta asset-job`

Creates/deletes per-model asset jobs for selected models. When present, the derived job name becomes `dbt_<model>_asset_job`.

```bash
dbt-dagsterizer meta asset-job \
  --models orders \
  --no-parse
```

Flags:

- Adds selected models to `asset_jobs`
- `--parse`: run `dbt parse` after writing

### `meta asset-job-delete`

Deletes per-model asset jobs for selected models.

```bash
dbt-dagsterizer meta asset-job-delete \
  --models orders
```

Flags:

- `--force`: also remove referencing schedules

### `meta job-delete`

Deletes a grouped job by name.

```bash
dbt-dagsterizer meta job-delete \
  --name daily_facts_job
```

Flags:

- `--force`: also remove references from schedules/propagators

### `meta partition-change detector`

Creates/updates a partition-change detector entry for a model.

```bash
dbt-dagsterizer meta partition-change detector \
  --model orders \
  --enabled \
  --detect-source ods.orders \
  --partition-date-expr order_date \
  --updated-at-expr updated_at \
  --lookback-days 7 \
  --offset-days 1
```

Notes:

- Specify exactly one of `--detect-relation` or `--detect-source`.
- `--detect-source` uses `source.table` format.
- `--lookback-months` / `--offset-months` apply to models listed under `partitions.monthly`. Passing either one takes precedence over `--lookback-days` / `--offset-days` for that entry.

### `meta partition-change propagator`

Creates/updates a partition-change propagation entry for a model.

```bash
dbt-dagsterizer meta partition-change propagator \
  --model orders \
  --enabled \
  --targets daily_facts_job
```

Notes:

- Auto-generated propagation specs use relation-based AssetKeys derived from the dbt manifest.
- Manual Python propagation specs should prefer `upstream_model_relation` when constructing specs directly.
- For backward compatibility, legacy manual specs that only provide `upstream_dbt_model` are upgraded to relation-based upstream keys at runtime when the model exists in the manifest.
- Manual Python specs are an escape hatch and are intentionally discouraged because they can be brittle across upgrades. Prefer defining propagators in `dagsterization.yml` using this CLI.

### `meta report`

Creates/updates an SSRS report entry in `dagsterization.yml`. The report triggers a pre-defined SSRS subscription (via its SQL Server Agent job) after the given dbt model materializes.

```bash
dbt-dagsterizer meta report \
  --name daily_sales \
  --model fct_sales_daily \
  --subscription-description "Daily sales report subscription" \
  --enabled
```

Flags:

- `--name`: unique report name; becomes the Dagster asset key `ssrs/<name>`
- `--model`: dbt model whose materialization triggers the report (must exist in the manifest)
- `--subscription-description`: SSRS subscription description used to look up its SQL Server Agent job (must be unique on the report server)
- `--enabled/--disabled`: when enabled, the report asset auto-materializes eagerly after the upstream model (default: enabled)
- `--prepare/--no-prepare`: when enabled, runs `dbt parse` if the manifest is missing or stale

Notes:

- The SSRS agent connection is configured via environment variables: `SSRS_DB_HOST` (required), `SSRS_DB_PORT` (default `1433`), `SSRS_DB_USERNAME`, `SSRS_DB_PASSWORD`, `SSRS_DB_DATABASE` (default `msdb`), and `SSRS_DB_TIMEOUT_SECONDS` (default `60`).
- The subscription lookup expects the SSRS catalog database to be named `ReportServer` (not configurable today).
- See [dagsterization-yml.md](dagsterization-yml.md) (SSRS Reports section) for details on how the subscription lookup works.

### `meta report-delete`

Deletes an SSRS report entry by name.

```bash
dbt-dagsterizer meta report-delete --name daily_sales
```

### `macros sync`

Syncs namespaced macro templates shipped with `dbt-dagsterizer` into a dbt project at `macros/dbt_dagsterizer/`.

```bash
dbt-dagsterizer macros sync
```

Flags:

- `--force`: overwrite files if they already exist

Template selection:

- Uses `dbt_project/.dbt_dagsterizer_template` (rendered from the template) to determine which embedded template to use as the source of managed macros.
- Falls back to the default template if the file is missing.
- Override via `DBT_DAGSTERIZER_TEMPLATE` if needed.

## Why `--parse` exists

Dagster reads orchestration intent from the dbt manifest. If you update YAML but do not rebuild `target/manifest.json`, Dagster will not see the change. `--parse` updates the manifest immediately.

## Asset identity and grouping notes

- Auto-generated dbt asset jobs now select assets by relation-based AssetKeys: `dbt/<database>/<schema>/<identifier>` (empty components are omitted).
- This keeps job selection stable across code locations that point at the same physical relation.
- Dagster group names for dbt models are derived from the first folder under `models/`.
- Manual/custom Python specs are an escape hatch and are intentionally discouraged because they can be tricky to keep backward compatible as features evolve. If you must use them, legacy model-name-only job/propagation definitions are upgraded to relation-based keys at runtime when possible.

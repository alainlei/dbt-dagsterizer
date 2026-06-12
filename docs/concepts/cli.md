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
- `--include-docker` is optional; when enabled, includes a local StarRocks docker-compose file and docker-related Make targets.
- `--output-dir` controls where the project directory is created.
- `--force` overwrites an existing output directory.
- By default the rendered dbt project is an empty skeleton; use `--include-sample-dbt-project` to include sample models.

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

- `--partitions`: `daily|unpartitioned|none`
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

- `--offset-days`: partition offset for `daily_at` schedules (`1` = yesterday, `0` = today)
- `--parse`: run `dbt parse` after writing

### `meta partition`

Sets partitioning for selected models in `dagsterization.yml`.

```bash
dbt-dagsterizer meta partition \
  --models fact_orders_daily,fact_customer_orders_daily \
  --type daily
```

Flags:

- `--type`: `daily|unpartitioned|none` (`none` removes the model from `partitions.*`)
- `--parse`: run `dbt parse` after writing

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

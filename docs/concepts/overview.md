# dbt-dagsterizer

`dbt-dagsterizer` is the home for reusable Dagster automation driven by dbt metadata.

## Goals

- Keep dbt projects and Dagster code locations decoupled (BYO dbt project).
- Generate Dagster assets/jobs/schedules/sensors/resources from dbt `manifest.json` plus a small orchestration intent file.
- Provide a CLI to author and validate orchestration intent with a low learning curve.

## Core concepts

### Manifest-driven

The dbt manifest (`dbt_project/target/manifest.json`) is the stable interface between dbt and Dagster.

Dagster asset identity is derived from dbt relation metadata in the manifest:

- AssetKey format: `dbt/<database>/<schema>/<identifier>` (empty components are omitted)
- Goal: keep asset identity stable across code locations that reference the same physical table/view
- Impact: auto-generated jobs and partition-change propagation specs follow relation-based keys automatically

Dagster model grouping is also derived from manifest-backed metadata:

- For dbt models, the Dagster group name is the first folder under `models/`
- For non-model resources, the group falls back to the dbt resource type (for example `source`, `seed`, `snapshot`)

### Orchestration intent

Orchestration intent is stored in a single, reviewable YAML file:

- `dbt_project/dagsterization.yml`

This file is not a dbt schema YAML and is intentionally kept outside `dbt_project/models/` so dbt will not parse it.

### Always-loadable definitions

Code locations often need to import successfully even before the dbt project is “real”.

`dbt_dagsterizer.api.build_definitions()` returns a minimal Dagster `Definitions` when there are no dbt models yet, so skeleton repos remain runnable.

### Schedule offsets

`daily_at` schedules support `offset_days`:

- `1` means “run for yesterday’s partition” and remains the default
- `0` means “run for today’s partition”
- Use this when a daily job should target an earlier or same-day partition window explicitly

## See also

- CLI reference: `cli.md`
- Code location template concept: `dagster-dbt-code-location-template.md`
- Execution model and env propagation: `execution-model.md`
- Observability (OpenTelemetry + Elastic APM): [../observability.md](../observability.md)
- dagsterization.yml: `dagsterization-yml.md`

# dbt-dagsterizer docs

This directory contains developer-facing documentation for `dbt-dagsterizer`.

## Start here

- Getting started: [getting-started.md](getting-started.md)
- Concepts overview: [concepts/overview.md](concepts/overview.md)
- CLI reference: [concepts/cli.md](concepts/cli.md)
- Execution model and env propagation: [concepts/execution-model.md](concepts/execution-model.md)
- Observability (OpenTelemetry + Elastic APM): [observability.md](observability.md)
- `sources.yml` `meta.luban` reference: [concepts/sources-yml.md](concepts/sources-yml.md)
- Release notes: [../RELEASE_NOTES.md](../RELEASE_NOTES.md)

## Template docs (Dagster + dbt + StarRocks code location)

- Template overview: [templates/.../README.md](templates/dagster-dbt-starrocks-code-location/README.md)
- Usage and architecture: [templates/.../template_usage.md](templates/dagster-dbt-starrocks-code-location/template_usage.md)
- Developer workflow (jobs/schedules/sensors via `dagsterization.yml`): [templates/.../developer_workflow.md](templates/dagster-dbt-starrocks-code-location/developer_workflow.md)
- Local development (render + run): [templates/.../local_development.md](templates/dagster-dbt-starrocks-code-location/local_development.md)

## Developing this repo

- Package development: [development/package-development.md](development/package-development.md)
- Codebase tour: [development/codebase-tour.md](development/codebase-tour.md)

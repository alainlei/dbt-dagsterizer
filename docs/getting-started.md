# Getting started

`dbt-dagsterizer` helps you generate and validate Dagster orchestration for a dbt project using dbt metadata (`manifest.json`) plus a small, reviewable orchestration file (`dagsterization.yml`).

## Install

There are two common ways to install and use `dbt-dagsterizer`.

### Install as a CLI tool (recommended)

This makes `dbt-dagsterizer` available as a system tool (isolated from project environments):

```bash
uv tool install dbt-dagsterizer
```

To upgrade later:

```bash
uv tool upgrade dbt-dagsterizer
```

Use this for bootstrapping a repo from the embedded template (`project init`), managing `dagsterization.yml` (`meta ...`), and syncing managed macros (`macros sync`).

### Install as a Python dependency (required for Dagster runtime)

If your Dagster code location imports `dbt_dagsterizer` (for example `from dbt_dagsterizer.api import build_definitions`), then `dbt-dagsterizer` must be installed in that project’s environment (for example via the project’s `pyproject.toml` dependencies). Installing the CLI tool alone is not sufficient for runtime imports.

### Developing this repository

If you are developing in this repository:

```bash
uv sync --dev
```

For the local development workflow with a rendered validation project wired to your working tree, see `docs/development/local-development.md`.

If you are consuming the package from another repo, add it as a dependency in that repo and upgrade it like any other dependency (pinning is recommended for reproducible deployments).

## Quick start (CLI)

Show help:

```bash
dbt-dagsterizer --help
```

Show version:

```bash
dbt-dagsterizer --version
```

List embedded project templates:

```bash
dbt-dagsterizer project list-templates
```

Initialize orchestration intent and refresh the dbt manifest:

```bash
dbt-dagsterizer meta init --parse
dbt-dagsterizer meta validate --prepare
```

Render a new code-location project (only `--project-name`/`--name` is required; app/package names are derived automatically):

```bash
dbt-dagsterizer project init \
  --output-dir . \
  --project-name "Orders Analytics" \
  --namespace "metasync" \
  --author-name "You" \
  --author-email "you@example.com"
```

By default, rendered projects pin `dbt-dagsterizer` to the installed CLI version (when available) so the generated code location matches the generator. Override with `--dbt-dagsterizer-version` or leave it unpinned with `--no-pin-dbt-dagsterizer` (mutually exclusive).

To include sample dbt models in the rendered project:

```bash
dbt-dagsterizer project init --output-dir . --project-name "Orders Analytics" --namespace "metasync" --include-sample-dbt-project
```

## Quick start (Python)

Build Dagster `Definitions` from a dbt project directory:

```python
from dbt_dagsterizer.api import build_definitions

defs = build_definitions(dbt_project_dir="./dbt_project")
```

If the project has no dbt models yet (no `models/**/*.sql`), `build_definitions()` still returns a minimal, always-loadable `Definitions` so the code location can import successfully.

## Partitioning prerequisite

If you configure any model/job as `daily` partitioned (for example via `dbt_project/dagsterization.yml`), set:

- `DAGSTER_DAILY_PARTITIONS_START_DATE` (format `YYYY-MM-DD`)

## Next reads

- Concepts overview: `concepts/overview.md`
- CLI reference: `concepts/cli.md`
- Local package development: `development/local-development.md`
- Code location template docs: `templates/dagster-dbt-starrocks-code-location/README.md`

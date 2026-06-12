# Local development

This guide is for developing `dbt-dagsterizer` itself and validating changes against a runnable rendered Dagster code location.

Use this document when you want to:

- change `dbt-dagsterizer` Python code and verify runtime behavior
- change the embedded project template and verify rendered output
- use a rendered sample project as a local test harness

## Prerequisites

- `uv`
- Python `3.12`
- optional: Docker / StarRocks if you want to run the sample warehouse locally

## Set up the package repo

From the `dbt-dagsterizer` repository root:

```bash
uv sync --dev
```

Useful validation commands:

```bash
uv run ruff check .
uv run pytest
```

## Render a local validation project

Render a throwaway project that points back to your local `dbt-dagsterizer` checkout:

```bash
uv run dbt-dagsterizer project init \
  --output-dir /tmp \
  --project-name "Orders Analytics" \
  --namespace "metasync" \
  --include-sample-dbt-project \
  --include-docker \
  --local-dbt-dagsterizer-path /Users/metasync/Workspace/projects/luban/dbt-dagsterizer
```

This is the recommended local-development flow because the rendered project becomes a runnable validation harness for the package under development.

## Start the rendered project

In the rendered project:

```bash
cd /tmp/orders-analytics
make setup
make check-db
make dev
```

If you rendered with `--include-docker`, you can also start local StarRocks:

```bash
make docker-up
```

See the rendered-project guide for environment and Docker details: [../templates/dagster-dbt-starrocks-code-location/local_development.md](../templates/dagster-dbt-starrocks-code-location/local_development.md).

## Fast iteration loop

### Python/package code changes

If the rendered project uses `--local-dbt-dagsterizer-path`, refresh it after changing `dbt-dagsterizer` source code:

```bash
make refresh-dagsterizer
```

That target reinstalls the local package and re-syncs managed dbt macros.

### Template changes

If you change files under `src/dbt_dagsterizer/project_templates/`, re-render the validation project. Refreshing the installed package is not enough because the rendered files are already copied into the generated project.

## Dependency modes

### File URL dependency

Recommended for most local validation:

```toml
"dbt-dagsterizer @ file:///Users/metasync/Workspace/projects/luban/dbt-dagsterizer"
```

Behavior:

- source-code changes require reinstall or `make refresh-dagsterizer`
- template changes require re-rendering

### Editable install

Alternative when you want immediate pickup of Python source changes:

```bash
uv pip install -e /Users/metasync/Workspace/projects/luban/dbt-dagsterizer
```

Behavior:

- Python source changes are picked up immediately
- template changes still require re-rendering

## What to use for validation

Common checks in the rendered project:

- Dagster asset graph and group assignment
- observable source asset behavior
- manifest preparation / dbt parsing on load
- generated macros and CLI-managed metadata
- OTEL / runtime integration in a real code location

## Troubleshooting

### Local package change is not visible

- confirm the rendered project uses the local `file://` dependency or editable install
- rerun `make refresh-dagsterizer` for the `file://` flow
- restart `make dev` if the Dagster process is already running

### Template change is not visible

- re-render the project; refreshing the installed package does not update already-rendered files

### CLI render does not use your local checkout

- run the CLI from this repo with `uv run dbt-dagsterizer ...`
- if needed, install the current repo in editable mode first:

```bash
uv pip install -e .
```

## Related docs

- [package-development.md](package-development.md)
- [codebase-tour.md](codebase-tour.md)
- [../concepts/cli.md](../concepts/cli.md)
- [../templates/dagster-dbt-starrocks-code-location/local_development.md](../templates/dagster-dbt-starrocks-code-location/local_development.md)

# Package development

This document is for contributors working on `dbt-dagsterizer` in this repository, and for local workflows that require running with a working-tree version of the package.

## Setup

Create a local environment with dev dependencies:

```bash
uv sync --dev
```

## Run tests

```bash
uv run pytest
```

## Lint

```bash
uv run ruff check .
```

## Build artifacts

Wheel and sdist:

```bash
uv run hatch build
```

## Template development

The embedded cookiecutter template lives under `src/dbt_dagsterizer/project_templates/`.

To render a local throwaway project for validation:

```bash
uv run dbt-dagsterizer project init --help
```

Then follow the rendered project’s `README.md`.

## Local development guide

For the full local development workflow, including rendering a validation project, using `--local-dbt-dagsterizer-path`, `make refresh-dagsterizer`, and deciding when to re-render versus reinstall, see [local-development.md](local-development.md).

See also: [../observability.md](../observability.md).

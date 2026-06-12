# Local development (template and rendered project)

This guide is for developing the source template and verifying the rendered project locally.

If you are developing `dbt-dagsterizer` itself and want the rendered project to depend on your local checkout, see [../../development/local-development.md](../../development/local-development.md).

## Render the template

Use the embedded template renderer:

```bash
dbt-dagsterizer project init \
  --output-dir /tmp \
  --project-name "Orders Analytics" \
  --namespace "metasync" \
  --include-docker \
  --include-sample-dbt-project \
  --author-name "You" \
  --author-email "you@example.com"
```

## Setup and run the rendered project

```bash
cd /tmp/orders_analytics
make setup
```

`make setup` installs Python dependencies and installs the dbt macros required by this code location.

## Start StarRocks (Docker Compose)

```bash
make docker-up
```

This step requires rendering with `--include-docker`.

Wait until the `starrocks-init` container finishes. It registers the BE into FE.

StarRocks ports exposed locally:

- FE HTTP: `8030`
- FE MySQL: `9030`
- BE HTTP: `8040`

## Configure environment

`make setup` creates `.env` if missing. Update it with your StarRocks connection values.

For local StarRocks via Docker Compose, these defaults typically work:

- `STARROCKS_HOST=localhost`
- `STARROCKS_PORT=9030`
- `STARROCKS_USER=root`
- `STARROCKS_PASSWORD=`

Validate connectivity:

```bash
make check-db
```

## Observability (OpenTelemetry)

The rendered code location initializes OpenTelemetry at process startup. Export is disabled unless you enable it via environment variables.

See [../../observability.md](../../observability.md) for:

- OTEL environment variables and endpoint formatting
- Elastic APM smoke test steps
- dbt run-results span/event controls (`LUBAN_OTEL_DBT_RUN_RESULTS_*`)

## ODS demo data (optional)

If you plan to use the built-in ODS observation loop and automation, create the demo ODS tables before starting Dagster.

Bootstrap:

```bash
make ods-test-bootstrap
```

Append new rows (simulate incremental arrival):

```bash
make ods-test-append
```

Note: in the sample project, `ods_test_*` models exist only to bootstrap demo ODS tables locally. In a real project, the ODS layer is typically external and appears as dbt `source(...)` assets. Because the sample bootstrap models write to the same physical ODS relations that the dbt sources reference, the Dagster lineage graph may not present those demo bootstrap relationships exactly the way a production graph would.

## Run Dagster

```bash
make dev
```

Dagster Webserver should be available at `http://localhost:3000`.

## Partitioning notes

The template currently ships with daily partition support only.

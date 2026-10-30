# Environment configuration (`.env`)

A `.env` file supplies local environment variables for a generated dbt-dagsterizer code location: database connections, dbt target selection, partition boundaries, and optional runtime features.

Keep environment-specific values and credentials here, not in Python code. Keep orchestration intent (models, jobs, schedules, replication, and timezone) in [dagsterization.yml](dagsterization-yml.md). dbt reads environment variables through `env_var(...)` in its project and profile configuration.

## Location and setup

The generated project includes `.env.example`; the runtime loaders do not read that example directly. Copy it to `.env` in the **generated code-location repository**, alongside `pyproject.toml`, and replace the example values. Do not overwrite an existing `.env`.

```text
my-code-location/
├── .env.example             # Version-controlled template; no real credentials
├── .env                     # Local configuration; ignored by Git
├── pyproject.toml
├── dagster_home/
│   └── dagster.yaml
└── dbt_project/
    ├── .env                 # Optional additional configuration
    ├── dbt_project.yml
    ├── profiles.yml
    └── dagsterization.yml
```

If `.env` does not exist yet:

```bash
cp .env.example .env
```

In PowerShell, use `Copy-Item .env.example .env` instead. The template's `make setup` also copies the example when `.env` is missing, then installs dependencies.

Before starting:

1. Set the StarRocks connection and database names for your environment.
2. Choose `DBT_TARGET` matching an output in `dbt_project/profiles.yml`.
3. Set start dates for the partition types your project uses.
4. Set `DAGSTER_HOME` to an existing **absolute** local directory if using a persistent local Dagster instance, for example `D:/projects/my-code-location/dagster_home` on Windows. Do not leave it as an empty assignment when relying on a launcher default; remove the assignment instead.
5. Enable SQL Server, SSRS, or telemetry settings only when needed.

Both `.env` files are optional when the required values are already supplied by the process environment. `.env.example` values are a starting point, not production credentials or a guarantee that the database exists.

## How values are loaded

### Generated code location

The generated `definitions.py` reads the repository `.env`, then `dbt_project/.env`, using `os.environ.setdefault`. For duplicate keys, priority is:

1. Existing process environment, including values injected by a launcher or container.
2. Repository-root `.env`.
3. `dbt_project/.env`.
4. Defaults supplied by the generated code or the consuming resource.

The nested `.env` therefore **does not override** a key in the root `.env` on this path. A blank assignment such as `KEY=` still counts as present and prevents `setdefault` from filling it.

The generated entry point locates these files relative to its own repository and explicitly passes `dbt_project` as both the project and profiles directory to `build_definitions`. Changing `DBT_PROJECT_DIR` or `DBT_PROFILES_DIR` alone does not relocate that generated entry point; adjust its explicit arguments when adopting a different layout.

### CLI and manifest preparation

The dbt-dagsterizer dotenv helper used for manifest preparation merges the two files before temporarily adding missing environment variables. For ordinary dotenv keys, priority is:

1. Existing process environment.
2. `dbt_project/.env`.
3. Repository-root `.env`.

The previous process environment is restored after the operation. Explicit CLI/API arguments for paths or targets are separate from this dotenv merge.

**Avoid defining the same key differently in both files.** Their precedence differs between the generated entry point and CLI preparation. Prefer a single root `.env`, or supply authoritative values through the process environment.

### Make and direct commands

The generated Makefile includes and exports the root `.env` using **Make syntax**, not the Python dotenv parser. Make has its own quoting, `$`, `#`, and assignment-precedence rules; a file that works with the Python loader is not necessarily equivalent under Make. For example, quotes may become part of the exported value, and Makefile assignments normally override inherited shell values.

A direct `dbt` command does not invoke dbt-dagsterizer's dotenv loader. Supply its environment explicitly or use the template's Make targets with Make-compatible values. Other launchers can load `.env` themselves before the generated entry point runs; those values then count as existing process environment.

### Python dotenv syntax

The dbt-dagsterizer parser accepts UTF-8 text with one `KEY=value` assignment per line:

```dotenv
# Put comments on their own lines.
DBT_TARGET=development
STARROCKS_PORT=9030
LUBAN_OBSERVE_SOURCES_CRON=*/5 * * * *
```

- Blank lines and lines beginning with `#` are ignored.
- An optional `export ` prefix is accepted.
- Unquoted, single-quoted, and double-quoted values are supported; matching outer quotes are removed.
- Double-quoted values decode `\n`, `\r`, `\t`, `\"`, and `\\`.
- Values are strings; the consuming setting performs numeric or boolean conversion.
- `${OTHER_VAR}`, `$OTHER_VAR`, and shell commands are **not expanded**.
- Inline comments are **not stripped**: `DBT_THREADS=4 # workers` is not a valid integer value.
- If a key appears twice in the same file, the last assignment wins.

For secrets containing shell or Make special characters, prefer direct secret injection rather than assuming all loaders interpret the same text identically.

## Variable reference

The following tables cover the generated `.env.example`. Values labelled “template” describe what is written into the example; runtime defaults can differ when a variable is omitted. Optional features do not need working credentials unless used, but populated numeric settings must still be valid numbers.

### Dagster and dbt

| Variable | Template value | Purpose |
| --- | --- | --- |
| `DAGSTER_WEBSERVER_HOST` | `0.0.0.0` | Intended webserver bind address. The generated `make dev` does not explicitly pass it as a CLI option; check the launcher you use rather than assuming it changes the binding. |
| `DAGSTER_WEBSERVER_PORT` | `3000` | Intended local webserver port; the same launcher caveat applies. |
| `DAGSTER_HOME` | Empty | Dagster instance directory. Use an existing absolute path for persistent local instance configuration/storage. |
| `DAGSTER_MAX_CONCURRENT_RUNS` | `2` | Maximum concurrent runs for the template's queued run coordinator. Required when using its `dagster_home/dagster.yaml`, which references this variable directly. Not dbt thread concurrency. |
| `DBT_TARGET` | Selected at project generation | dbt profile output: the template provides `development`, `sandbox`, and `production`. Selecting a target does not by itself rewrite explicitly configured database names. |
| `DBT_THREADS` | `4` | dbt execution threads. If omitted, the template profile defaults to `4` for development/sandbox and `8` for production. |
| `DBT_PROJECT_DIR` | `./dbt_project` | Directory containing `dbt_project.yml` for environment-based discovery and Make commands; see the explicit-path caveat above. |
| `DBT_PROFILES_DIR` | `./dbt_project` | Directory containing `profiles.yml`, not the file itself; same explicit-path caveat. |
| `LUBAN_REPO_ROOT` | Empty | Optional repository-root override for library path discovery. Relative project/profile paths resolved by the library use this root. Prefer an absolute path when setting it. |
| `LUBAN_DBT_PREPARE_ON_LOAD` | `1` | Enables automatic manifest preparation on definition load when the manifest is missing or stale according to manifest-input checks. Accepted enabled values are `1`, `true`, and `yes`; use `0` to disable and prepare artifacts separately. |

The dbt resource's target fallback is `DBT_TARGET`, then `LUBAN_DEFAULT_DBT_TARGET` (set from the generated entry point's `default_dbt_target` argument), then `development`.

### StarRocks

These settings must describe a reachable database with appropriate permissions. The built-in localhost/root/empty-password defaults are for local setup, not recommended production credentials.

| Variable | Template value or default | Purpose |
| --- | --- | --- |
| `STARROCKS_HOST` | `localhost` | StarRocks frontend host used by dbt and the Dagster StarRocks resource. |
| `STARROCKS_PORT` | `9030` | MySQL-compatible query port. |
| `STARROCKS_USER` | `root` | Connection username. Use a suitably restricted account outside local development. |
| `STARROCKS_PASSWORD` | Empty | Connection password. |
| `STARROCKS_CONNECT_TIMEOUT` | `10` | Seconds for connection, read, and write timeouts in the Dagster resource's PyMySQL client. This is not the dbt adapter's query timeout. |
| `STARROCKS_USE_PURE` | `true` | Value passed to the dbt StarRocks profile's `use_pure` option. |
| `STARROCKS_ODS_DB` | Generated ODS database name | Source-layer database; the default observable-source mapping uses this for `ods`. |
| `STARROCKS_DWD_DB` | Generated DWD database name | Detail-layer database setting for dbt configuration/macros that reference it. |
| `STARROCKS_DWS_DB` | Generated DWS database name | Serving-layer database setting for dbt configuration/macros that reference it. |
| `STARROCKS_DB` | Generated DWS database name | Default database/schema in the dbt profile; the profile falls back to `dbt` when omitted. |
| `STARROCKS_QUERY_TIMEOUT_SECONDS` | `3600` (commented example) | dbt profile session `query_timeout`, in seconds. |
| `STARROCKS_PLANNER_OPTIMIZE_TIMEOUT_MS` | `300000` (commented example) | dbt profile session `new_planner_optimize_timeout`, in milliseconds. |

Generated layer names follow `[namespace_]package_name_<layer>_<env>`, where the environment suffix is `dev`, `snd`, or `prd`. Review all four database variables when switching environments; environment variables do not create databases or automatically change models that do not reference them.

### Partitions and source observation

| Variable | Default or example | Purpose |
| --- | --- | --- |
| `DAGSTER_DAILY_PARTITIONS_START_DATE` | Required for daily partitions; example `2026-03-01` | Earliest daily partition date, in `YYYY-MM-DD` format. |
| `DAGSTER_HOURLY_PARTITIONS_START_DATE` | Required for hourly partitions; use `2026-03-01-00:00` | Earliest hourly partition start, in `YYYY-MM-DD-HH:MM` format. |
| `DAGSTER_MONTHLY_PARTITIONS_START_DATE` | Required for monthly partitions; example `2026-03-01` | Earliest monthly partition start. Use `YYYY-MM-01` for a month boundary. |
| `LUBAN_PARTITION_CHANGE_PROPAGATOR_MODE` | `sensor` | `sensor` uses partition-change propagation sensors. `eager` disables those sensors and relies on Dagster automation for eligible models that opt in; it does not automatically opt every model into automation. |
| `LUBAN_PARTITION_CHANGE_PROPAGATOR_CATCHUP_DAYS` | `0` | Non-negative days of upstream materialization history to replay when a propagation sensor has no cursor. `0` disables historical catch-up. Changing it does not reset an existing cursor. |
| `LUBAN_OBSERVE_SOURCES_CRON` | `*/5 * * * *` | Cron shared by source observation batch schedules; every five minutes by default. |
| `LUBAN_OBSERVE_SOURCES_BATCH_SIZE` | `5` | Maximum source assets per observation job. Must be a positive integer; smaller batches limit the number observed in each run. |

Partition timezone and observation schedule timezone come from the top-level `timezone` in `dagsterization.yml`. The template currently includes an offset in its hourly example (`2026-03-01-00:00+08:00`); the library does not customize Dagster's hourly string format, so use the offset-free format above and configure the timezone separately.

Partition start dates control available partition history, not a schedule's launch time. Choosing a start date does not itself launch a backfill.

### OpenTelemetry (optional)

Export is disabled by default. Enable it only when an OTLP collector is available.

| Variable | Template value or default | Purpose |
| --- | --- | --- |
| `OTEL_TRACES_EXPORTER` | `none` | Set to `otlp` to export traces. |
| `OTEL_METRICS_EXPORTER` | `none` | Optional metric export; current instrumentation primarily emits traces. |
| `OTEL_EXPORTER_OTLP_PROTOCOL` | `grpc` | `grpc` or `http/protobuf`. |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `localhost:8200` | Collector address. For gRPC use `host:port`; for HTTP use an `http://` or `https://` URL. Match the actual collector configuration. |
| `OTEL_EXPORTER_OTLP_HEADERS` | Unset; commented example | Optional authentication headers, such as `Authorization=Bearer <token>`. Treat as a secret. |
| `OTEL_SERVICE_NAME` | Generated from namespace/package | Service identity in the telemetry backend. |
| `OTEL_RESOURCE_ATTRIBUTES` | Generated namespace/environment | Comma-separated resource attributes, including `service.namespace` and `deployment.environment.name`. |
| `LUBAN_OTEL_DBT_RUN_RESULTS_MODE` | `spans` | Emit dbt node breakdowns as `spans`, `events`, or disable with `none`. |
| `LUBAN_OTEL_DBT_RUN_RESULTS_TOP_N` | `20` | Number of slowest nodes selected alongside failures for the breakdown. |
| `LUBAN_OTEL_DBT_RUN_RESULTS_MIN_SECONDS` | `0` | Minimum duration in seconds for slow-node selection. |

See [Observability](../observability.md) for exporter setup and additional instrumentation options.

### SQL Server replication (optional)

Configure these when using StarRocks-to-SQL Server replication enabled in `dagsterization.yml`. Assigning connection variables alone does not enable replication.

| Variable | Runtime default | Purpose |
| --- | --- | --- |
| `SQLSERVER_HOST` | `localhost` | Replication destination host. |
| `SQLSERVER_PORT` | `1433` | SQL Server port. |
| `SQLSERVER_USER` | `sa` | Destination username; use an appropriately restricted account. |
| `SQLSERVER_PASSWORD` | Empty | Destination password. |
| `SQLSERVER_DATABASE` | `master` | Destination database; explicitly set your intended application database. |
| `SQLSERVER_DRIVER` | `ODBC Driver 18 for SQL Server` | Installed system ODBC driver name. Setting the variable does not install the driver. |

### SSRS subscriptions (optional)

SSRS uses a separate SQL Server connection, not the `SQLSERVER_*` replication credentials. Configure these when `dagsterization.yml` defines `ssrs_reports` entries.

| Variable | Runtime default | Purpose |
| --- | --- | --- |
| `SSRS_DB_HOST` | Empty | Required when triggering subscriptions; SQL Server hosting the report catalog and SQL Server Agent jobs. |
| `SSRS_DB_PORT` | `1433` | SQL Server port. |
| `SSRS_DB_USERNAME` | Empty | Database login, not an SSRS web-service username. |
| `SSRS_DB_PASSWORD` | Empty | Database password. |
| `SSRS_DB_DATABASE` | `msdb` | Initial connection database. Subscription lookup also accesses `ReportServer`. |
| `SSRS_DB_TIMEOUT_SECONDS` | `60` | Login and query timeout in seconds. |

The account needs access to the subscription lookup tables and permission to start the corresponding SQL Server Agent job.

## Production and secret handling

- Keep real `.env` files out of Git. Commit only sanitized `.env.example` files; verify ignore rules in each generated repository.
- Do not bake credentials into container images, paste them into logs, or include them in support screenshots.
- Inject production values through your deployment's environment and secret management. A local `.env` is not automatically available inside containers or remote run workers.
- With `K8sRunLauncher`, configure credentials for **both** the code-location pod (sensor/schedule evaluation) and run pods (job execution). Run pods do not inherit the code location's `envFrom` automatically.
- `LUBAN_RUN_ENV_CONFIGMAP` and `LUBAN_RUN_ENV_SECRET` are additional deployment variables naming the ConfigMap/Secret to inject into run pods. See [Execution model and env propagation](execution-model.md).
- `dbt-dagsterizer project gen-gitops-env` generates local ConfigMap/Secret manifests from `.env`. It excludes `OTEL_*` settings and writes empty Secret placeholders for `STARROCKS_PASSWORD` and `SSRS_DB_PASSWORD`, rather than copying their values. Supply those secrets through your deployment workflow. See the [CLI reference](cli.md#project-gen-gitops-env).

**GitOps export warning:** the generator is not a general-purpose secret classifier. It currently does not exclude `SQLSERVER_PASSWORD` or arbitrary custom secret keys from ConfigMap output. Use a sanitized input file without those secrets, and inject them separately through a Secret. Review generated manifests before sharing or applying them; a Kubernetes Secret manifest is not itself encrypted.

## Applying changes and troubleshooting

Restart the code-location process after changing `.env`: values already loaded into `os.environ` are not replaced by the generated loader, and some definitions/resources are cached. Restart local launchers as needed; update/restart deployed processes when changing injected configuration.

| Symptom | Check |
| --- | --- |
| An edited value has no effect | An existing environment value may win; check duplicate keys in both dotenv files, the launcher used, and whether the process was restarted. |
| A numeric setting fails to parse | Remove inline comments, empty assignments, or quotes preserved by a different loader. |
| dbt selects the wrong database | Check `DBT_TARGET`, all `STARROCKS_*_DB` values, and the actual `env_var(...)` references in the profile/models. Refresh the manifest after target/schema changes. |
| Partition definitions fail to load | Set the required start date for each enabled partition type; use the documented hourly format and YAML timezone. |
| CLI and Dagster disagree | Avoid conflicting root/nested `.env` values; their dotenv merge orders differ. |
| Sensors connect but job runs fail | Ensure run pods receive the same required connection settings and secrets as the code location. |
| SQL Server driver is not found | Install the named ODBC driver in the environment that executes replication. |

For CLI operations, `--prepare` can refresh a missing or stale manifest when supported; dotenv changes and target changes are inputs to that refresh. See [CLI environment handling](cli.md#environment) for details.

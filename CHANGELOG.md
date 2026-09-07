# Changelog

All notable changes to `dbt-dagsterizer` will be documented in this file.

The format is based on Keep a Changelog, and this project adheres to Semantic Versioning.

## [Unreleased]

## [0.5.3] - 2026-09-08

### Added

- Added monthly partition support for dbt assets, jobs, schedules, replication, and partition-change sensors, at parity with the existing `daily` and `hourly` strategies.
  - New `monthly` partition type in `dagsterization.yml` under `partitions.monthly`, configured with `DAGSTER_MONTHLY_PARTITIONS_START_DATE` (`YYYY-MM-DD`). Partition keys are the first day of each month (`2026-03-01`, `2026-04-01`, ...), matching Dagster's `MonthlyPartitionsDefinition`.
  - New `partitions.monthly_config` section with `include_current_month_partition` (boolean, default `true`) controlling whether the in-progress month is available (`end_offset=1`) or not (`end_offset=0`). Dagster's `day_offset` is intentionally not exposed, so keys always start on day 1.
  - New `monthly_at` schedule type with `day_of_month`, `hour`, `minute`, `lookback_months`, and `offset_months`. `day_of_month` is validated to `1..28` because cron never fires on a 29th-31st during a shorter month, which would silently stall the schedule. `offset_months` defaults to `1`, mirroring `offset_days: 1` — a schedule firing on the 1st materializes the month that just closed.
  - New `monthly_at()` schedule preset in `schedules/dbt/presets.py`. Schedule spec dicts grew from 11 to 13 keys: all three schedule types now emit `partition_offset_months` and `partition_lookback_months`, zeroed when unused, matching the existing convention for the other granularity's fields.
  - Monthly partition-change detectors. `sensors/partition_change/detector/sparse_lookback.py` gained a `granularity` parameter (`"day"` or `"month"`); month granularity groups watermarks with `date_trunc('month', CAST((<expr>) AS DATE))` so detected values line up with monthly partition keys. New `monthly_partition_change()` preset alongside `daily_partition_change()`.
  - `SparseLookbackImpactRange` gained `start_offset_months` / `end_offset_months`. `impact.type: range` now rejects the wrong granularity's offset pair rather than silently misapplying it: day offsets on a monthly detector would produce mid-month dates such as `2026-03-15`, which are invalid monthly partition keys and would fail the `RunRequest`.
  - Timezone propagation: `meta timezone` CLI command now sets the code location's execution timezone (`dagsterization.yml.timezone`), defaulting to UTC, covering `MonthlyPartitionsDefinition` and the `timezone` field on generated schedules. `test_timezone.py` covers it.
  - CLI: `meta monthly-config`, `meta partition --type monthly`, `meta job --partitions monthly`, `meta schedule --schedule-type monthly_at` with `--day-of-month` / `--lookback-months` / `--offset-months`, and `meta partition-change detector --lookback-months` / `--offset-months`, plus the previously missing `meta partition-config` (daily) and `meta hourly-config` commands.
  - Validation: `partitions.monthly_config` structure checks, `monthly` in the partition and job-partition enums, `monthly_at` in the schedule-type enum, and per-type rejection of cross-granularity offset/lookback fields (a `daily_at` schedule setting `offset_months` is now an error, and likewise for the other combinations). Timezone structure and IANA-tz validation added to be unknown-name rejection.
  - Test coverage: monthly partition definitions and env-var enforcement, `monthly_at` preset defaults/validation/cron format, monthly schedule factory partition keys including year rollover, monthly replication schedules, monthly partition-change sensors (watermark dedupe, month-based impact ranges, generated SQL), timezone inheritance, CLI round-trips, `meta timezone`, and an end-to-end `build_definitions` load test (`tests/test_integration_monthly.py`) that drives `meta validate` plus a full definitions build over a scratch dbt project containing a monthly-partitioned *replicated* model.
- External source ownership can now be declared directly inside `dbt_project/models/sources.yml` via `meta.luban.external_code_location: true`, replacing the earlier `external_asset_sources` key previously required in `dagsterization.yml`. The declaration is per-table (table-level `meta` wins, source-level `source_meta` covers every table under that source entry when set on the source itself). A pair `(source_name, dbt_name)`, not a whole source group, so marking one table external no longer accidentally excludes shared-source sibling tables. Detected external pairs exclude those sources from two places: (1) observable-source asset generation via `load_filtered_observable_sources()` skips the per-table spec, and (2) `@dbt_assets` via the `exclude=source:<src>.<name>` dbt selector, preventing "Multiple asset definitions found" warnings when two code locations each own a piece of the same upstream physical source. Documented in `docs/concepts/sources-yml.md` (new, added by PR #11), with worked examples for the table-level vs source-level semantics.
- New concept doc: `docs/concepts/sources-yml.md` (343 lines) covering the full `meta.luban.*` namespace for dbt sources: `observe.watermark_column`, `observe.watermark_sql`, new `group`, and new `external_code_location`.

### Fixed

- **A `monthly`-partitioned replicated model no longer prevents the whole code location from loading.** `schedules/replication/factory.py` raised `Unsupported partition_type` for anything other than `unpartitioned`/`daily`/`hourly` at *definitions-build* time — triggered as soon as `replication.enabled: true` was set on a monthly-partitioned model. The factory now handles `monthly`, emitting `YYYY-MM-01` keys, and the auto-generated replication cron for monthly models is `30 0 1 * *` (00:30 on the 1st) so that, with the default `offset_months: 1`, it replicates the month that just closed — the exact analogue of the daily `30 0 * * *` schedule replicating yesterday.
- **Per-table `meta.luban.external_code_location: true` no longer accidentally excludes its whole dbt source group.** `load_external_source_names()` was returning `set[str]` of `source_name` values, so marking `mydb.ext_table` external incorrectly filtered sibling `mydb.local_table` out of both observable-source assets and the `@dbt_assets` graph. Fixed by switching to per-table `set[tuple[str,str]]` of `(source_name, dbt_name)` pairs; `load_filtered_observable_sources()` filters by that pair, `@dbt_assets exclude=` uses `source:<src>.<name>` dbt selector syntax; tests updated.
- **Total row-count `AssetObservation` scoped to the partition of the emitting run.** `_emit_partition_row_counts()` emitted the first `last_run_affected_row_count` observation with `partition=partition_key` but the sibling `dagster/row_count` total-count observation with no partition key. Now both observations carry the same `partition=` value; per-partition materializations now correctly attach metadata snapshots to their partition, and the Dagster UI Partitions tab shows row-count metadata for each specific partition rather than racing on a single global key.

### Changed

- Detector partition-type derivation is deliberately narrow: only models explicitly listed under `partitions.monthly` get a month-granular detector. Hourly and unpartitioned models keep the existing daily-granular detector behaviour. Deriving the type the way replication `auto_config` does would flip hourly models to `partition_type="hourly"` and hit the detector factory's `Unsupported partition_type` raise — a regression for existing users.
- `dagsterization.yml` OrchestrationIndex removed the `external_asset_sources` field; ownership is now declared inside sources.yml `meta.luban.external_code_location` instead (see Added, above).
- CLI `meta schedule` validation now accepts the new `--day-of-month`/`--lookback-months`/`--offset-months` flags only when schedule type is `monthly_at`, and rejects them otherwise; `meta partition-config`/`meta hourly-config`/`meta monthly-config`/`meta timezone` commands all now exist for config round-trips that previously were manual yml edits only.
- No sensor cursor migration is needed. The `partition_watermark_v1` cursor is keyed by partition key string, and each sensor is either daily or monthly, so a daily `"2026-03-01"` and a monthly `"2026-03-01"` never share a cursor.
- Docs: `dagsterization-yml.md` gained Monthly Partitions, Monthly Partition Configuration, Monthly Schedule, and Schedule Offset Months sections plus complete-example and troubleshooting updates. `cli.md` gained sections for the `meta partition-config` / `meta hourly-config` / `meta monthly-config` commands, full `meta schedule` flag documentation, and the new `meta timezone` command. `getting-started.md` and the template's `template_usage.md` and `.env.example` document `DAGSTER_MONTHLY_PARTITIONS_START_DATE`.

### Known issues (pre-existing, not fixed here)

- **`assets/replication/executor.py` filters partitions by equality on the raw partition key** (`WHERE {partition_column} = :pk`). A monthly key such as `2026-03-01` therefore matches only rows on the 1st of the month, silently under-replicating the rest. The identical bug already affects hourly keys (`2026-03-01-10:00`). Fixing it means range predicates instead of equality, which is a behaviour change for existing hourly users, so it is left for a separate issue.
- **`build_replication_schedules` closures capture loop variables**, so every generated replication schedule reads the *last* spec's offset/lookback values instead of its own. The monthly branch follows the existing pattern so a single future fix covers all four branches.
- **`cli_parts/validation.py` only populates `tags_by_model` when daily models exist**, so the `materialize_at_startup` warning does not fire for monthly-only or hourly-only projects.
- **Hourly `partition_change` detectors remain daily-granular** (see the detector derivation note above).
- **`docs/concepts/overview.md:L57` references sources-yml filename in plain text instead of a link** — cosmetic; a future docs cleanup.

## [0.5.2] - 2026-08-21

### Added

- **OTel span naming — optional code_location disambiguation (opt-in):** Added `LUBAN_OTEL_SPAN_NAME_INCLUDE_CODE_LOCATION` env var to `otel_dagster_transaction_info()`. When set to a truthy value (`1`, `true`, `yes`, `on`), the emitted span display name switches from the 2-segment default `{tx_type}/{tx_name}` to a 3-segment form `{tx_type}/{code_location}/{tx_name}`. This lets shared APM backends distinguish identically-named schedules/jobs (`schedule/daily`) across multiple deployed code locations (`schedule/orders_analytics/daily` vs `schedule/crm_analytics/daily`). The `__ASSET_JOB__` path intentionally skips the `code_location` segment when it already equals `tx_name` so default untitled asset jobs stay `asset_job/demo` instead of producing the redundant `asset_job/demo/demo`.
- **Template `definitions.py` now auto-loads `.env` on startup.** The rendered code-location template loads dotenv files from both `<repo-root>/.env` and `<repo-root>/dbt_project/.env` (using `dbt_dagsterizer.env_utils.parse_dotenv_file` with quoted-string and escape-sequence support) via `os.environ.setdefault` *before* applying hardcoded defaults for `LUBAN_REPO_ROOT`, `DBT_PROJECT_DIR`, and `DBT_PROFILES_DIR`. Effective precedence: shell env → repo-root `.env` → `dbt_project/.env` → baked-in defaults. Local `dagster dev` workflows now pick up a developer's workspace `.env` without extra ceremony.

### Fixed

- **Observable source assets now correctly handle dbt sources whose `identifier` differs from `name`** (for example an MSSQL upstream with a case-sensitive table name `OrdersFact` or a cross-catalog dotted path `CatalogName.Schema.Orders` while the dbt logical name stays simple). Previously the observable source asset key was built from the physical identifier, while `dagster_dbt` indexes source output names by the logical `name` field, producing a `KeyError` on `resolve_source_asset_key` for any source where the two diverged. `automation.py` now stores both `spec["table"]` (physical identifier, used for SQL FROM / watermark queries) and `spec["name"]` (logical dbt name, used for asset-key lookup), and `factory.py` resolves the source asset key using the logical name while keeping the physical identifier in the emitted SQL query.
- **`__ASSET_JOB__` span-name duplicate-segment guard** so the new 3-segment code-location span form never produces `asset_job/demo/demo` for default untitled asset jobs where `tx_name` is already the code location name.

### Changed

- **Template documentation for dotenv loading precedence and OTel span-name env var added:** developer_workflow.md now documents the `.env` two-file load order and parser rules, template_usage.md documents the dbt source `identifier` vs `name` disambiguation behavior, and observability.md documents the `LUBAN_OTEL_SPAN_NAME_INCLUDE_CODE_LOCATION` env var and the two span-name formats.
- **`build_definitions()` no longer needs template-local dotenv shimming** because the generated `definitions.py` performs the same `env_utils`-backed dotenv load that the CLI already uses.

## [0.5.1] - 2026-08-15

### Fixed

- **OTel bootstrap — disabled `none/none` default now warns (provider-agnostic)**: `configure_otel()` now emits a single `WARNING` per process when both `OTEL_TRACES_EXPORTER` and `OTEL_METRICS_EXPORTER` are `none` (or empty), so operators discover immediately why a code location / run pod produces zero OpenTelemetry output instead of silently succeeding with a noop provider. Diagnostic text points at any OTLP-compatible backend and the required endpoint + protocol setup.
- **OTel bootstrap — missing endpoint now leads with 'endpoint not set' and keeps the scheme-prefix warning as an important follow-up**: When export is enabled (`otlp`) but `OTEL_EXPORTER_OTLP_ENDPOINT` is empty, the warning now explicitly says this is the *platform-default noop* until an OTLP backend is installed, directs the user to set the endpoint via centralized Luban config or a workspace gitops overlay, and follows with the critical scheme-prefix note (always use explicit `http://` for plaintext or `https://` for TLS to avoid gRPC default-TLS `WRONG_VERSION_NUMBER` handshake failures against plaintext backends).
- **OTel bootstrap — force_flush no longer lies about transport-level failures (provider-agnostic root causes)**. Wrapped `OTLPSpanExporter` and `OTLPMetricExporter` with a `_ResultRecording*Exporter` shim that emits a `WARNING` for every batch the downstream exporter returns `FAILURE` for, including the span/metric count and the exact endpoint/protocol being hit. A unified 5-item triage list valid for *any* OTLP backend (network path / port, scheme omission → explicit `http://`/`https://`, auth via `OTEL_EXPORTER_OTLP_HEADERS` or mTLS, backend 5xx/429/disk/OOM, per-signal path mismatch) is enumerated in each log. Previously `BatchSpanProcessor.force_flush()`/`PeriodicExportingMetricReader` returned `True` even when every batch errored permanently.
- **OTel bootstrap — double `configure_otel()` call now detected and warned instead of silently using the first (noop) provider**: Both `trace.set_tracer_provider()` and `metrics.set_meter_provider()` are wrapped in try/except for the RuntimeError `"Overriding of current TracerProvider/MeterProvider is not allowed"`; when triggered we log a single warning that tells the user to invoke `configure_otel()` once, after applying all `OTEL_*` env overrides, and correctly report `traces_configured=False` instead of incorrectly claiming success. This directly fixes the exact bug reproduced in smoke #5 where a second enable-override call silently fell back to the first-call noop provider.

### Changed

- **OTel bootstrap warning messages are now backend-agnostic** (no longer Fleet/Elastic/APM specific). The `none/none` master-switch warning, the empty-endpoint warning, and the `_ResultRecording*Exporter` FAILURE triage lists all use generic OTLP wording so they are equally correct for OpenObserve, Elastic APM, Grafana Tempo/Prometheus, a standalone OTel Collector, or any other OTLP-compatible backend.

## [0.5.0] - 2026-08-09

### Breaking

- BREAKING: Default value for `partitions.daily_config.include_current_day_partition` changed from `false` to `true`. When omitted, `DailyPartitionsDefinition` now exposes today's partition as available (`end_offset=1`) instead of excluding it (`end_offset=0`).
  - Impact: code locations that omit `partitions.daily_config.include_current_day_partition` will now include today's partition in the available partition set. Combined with `materialize_at_startup` tags and `AutomationCondition.missing()`, this can trigger a backfill of today on code location restart.
  - Migration: if you relied on today's partition being excluded, explicitly set `partitions.daily_config.include_current_day_partition: false` in `dagsterization.yml`.

### Added

- Added hourly partition support for dbt assets, schedules, and sensors alongside the existing daily partition strategy.
  - New `hourly` partition type in `dagsterization.yml` under `partitions.hourly`, configured with `DAGSTER_HOURLY_PARTITIONS_START_DATE` (YYYY-MM-DD-HH:MM format, optionally with timezone offset such as `+08:00`).
  - New `partitions.hourly_config` section with `include_current_hour_partition` (boolean, default `true`) to control whether the current hour's partition is available in the `HourlyPartitionsDefinition`.
  - New `hourly_at` schedule type for hourly cadence schedules, with `offset_hours` and `lookback_hours` parameters analogous to the existing `offset_days`/`lookback_days` daily schedule parameters.
  - New `hourly_at()` schedule preset in `schedules/dbt/presets.py` for convenient hourly schedule creation with `offset_hours`, `lookback_hours`, and `minute` parameters.
  - CLI `meta schedule --schedule-type hourly_at` support for creating hourly schedules via the command line.
  - CLI `meta hourly-config` command for managing hourly partition configuration.
  - Validation for `hourly_config` structure and `hourly` partition assignments in `cli_parts/validation.py`.
- Added comprehensive test coverage for hourly partition functionality:
  - `get_hourly_partitions_def()` env var enforcement, caching, end_offset resolution, and reset behavior.
  - `get_partitions_def()` routing for `"hourly"`, `"daily"`, and unpartitioned specs.
  - Orchestration config parsing for `hourly_config` and hourly partition assignments.
  - `hourly_at()` schedule preset defaults, validation, cron format, and field propagation.
  - Hourly schedule factory evaluation producing correct partition keys.

### Fixed

- Fixed timezone propagation from `dagsterization.yml` to asset partition definitions. The `timezone` setting (e.g. `Asia/Macau`) is now correctly passed through to `DailyPartitionsDefinition` and `HourlyPartitionsDefinition` constructors so partition boundaries align with the configured timezone instead of always defaulting to UTC.
  - Added `timezone` parameter to `get_daily_partitions_def()`, `get_hourly_partitions_def()`, and `get_partitions_def()` in `partitions.py`.
  - Threaded `timezone` through dbt asset creation (`assets/dbt/assets.py`), dbt job factory (`jobs/dbt/factory.py`, `jobs/dbt/jobs.py`), replication asset factory (`assets/replication/factory.py`), and replication job factory (`jobs/replication/factory.py`).
  - Made the singleton partition definition cache timezone-aware so changing the timezone correctly invalidates the cached definition.
  - Threaded `execution_timezone` through replication schedule definitions so cron and partition boundaries align with the configured timezone.
- Added timezone propagation tests verifying daily and hourly partition definitions respect the configured timezone, default to UTC when unspecified, and invalidate the cache on timezone changes.
- Consolidated orchestration config loading in the replication assets factory so the config is loaded and indexed once per startup instead of three separate times.
- Fixed dbt schedule factory daily offset default so `partition_offset_days` defaults to 1 (yesterday) when omitted, matching preset/auto_config behavior instead of incorrectly defaulting to 0.
- Added schedule-structure validation for hourly partition schedules: struct validation in `cli_parts/validation.py` now validates `lookback_hours`/`offset_hours` for `hourly_at` schedules and rejects cross-granularity field misuse (e.g. `hourly_at` setting `offset_days` or `daily_at` setting `lookback_hours`).
- Made CLI `meta schedule --hour` conditionally required by schedule type: mandatory for `daily_at`, optional (default 0) for `hourly_at`, instead of silently defaulting to 0 for all schedule types.

### Changed

## [0.4.0] - 2026-08-03

### Added

- Added package version display to dbt asset descriptions in Dagster UI, showing `dbt_dagsterizer`, `dagster`, and `dagster_dbt` versions for each code location.
- Added optional `include_current_day_partition` setting under `partitions.daily_config` in `dagsterization.yml` to include today's partition in the `DailyPartitionsDefinition` (useful for same-day materialization).
- Added optional StarRocks-to-MSSQL data replication feature using dlt, configured via a `replication` section in `dagsterization.yml`.
  - Replication assets run as Dagster asset dependencies after their source dbt models are materialized.
  - Creates and populates user-defined destination tables in Microsoft SQL Server.
- Added optional SSRS (SQL Server Reporting Services) subscription triggering, configured via an `ssrs_reports` section in `dagsterization.yml` and managed with the `meta report` / `meta report-delete` CLI commands.
  - Each report entry becomes a Dagster asset (`ssrs/<name>`, group `reports`) that depends on its upstream dbt model and, when enabled, auto-materializes eagerly after the model materializes.
  - The SSRS agent resource resolves the subscription's SQL Server Agent job by its `subscription_description` in the ReportServer catalog and starts it via `msdb.dbo.sp_start_job`; rendering and delivery stay in SSRS.
  - Connection is configured via `SSRS_DB_HOST`, `SSRS_DB_PORT` (default `1433`), `SSRS_DB_USERNAME`, `SSRS_DB_PASSWORD`, `SSRS_DB_DATABASE` (default `msdb`), and `SSRS_DB_TIMEOUT_SECONDS` (default `60`) environment variables.
- Added row count metadata for dbt assets to improve observability in Dagster UI:
  - Added `AssetObservation` events with `last_run_affected_row_count` metadata showing rows inserted/updated in the current run.
  - Added `AssetObservation` events with `dagster/row_count` metadata showing total table row count after materialization.
  - Row count observations include partition context via `partition` parameter for partitioned assets.
  - Created `get_row_counts_from_starrocks()` helper in `dbt/row_counts.py` for direct database queries using relation metadata from manifest.

### Fixed

- Fixed replication executor logging to avoid leaking URL-encoded database credentials.
- Fixed SSRS report `enabled` default to be consistent across `dagsterization.yml` docs, CLI defaults, and runtime auto-configuration (defaults to enabled).
- Fixed orchestration validation so `materialize_at_startup` warnings for daily-partitioned models trigger reliably without scanning manifest tags unnecessarily.
- Fixed `build_definitions()` environment handling to avoid overriding `DBT_PROJECT_DIR` unless explicitly provided.
- Fixed rendered template test modules to remain syntactically valid before cookiecutter rendering.

### Changed

## [0.3.3] - 2026-07-11

### Added

- Added regression coverage for metadata-driven automation selection and dotted observable-source identifiers.

### Fixed

- Fixed observable source SQL generation to quote each part of dotted database, table, and watermark-column identifiers separately so external catalog references resolve correctly.
- Clarified template docs and sample environment comments so the documented automation behavior matches the current metadata-driven translator rules.

### Changed

- Changed dbt automation selection to use observable-source metadata, daily partition configuration, and model tags instead of relying on `dwd`/`dws` FQN path segments.

## [0.3.2] - 2026-07-11

### Added

- Added `--local-dbt-dagsterizer-path` to `project init` to render projects that depend on a local `dbt-dagsterizer` checkout via a `file://` dependency.
- Added `make refresh-dagsterizer` to the rendered project template to reinstall a local `dbt-dagsterizer` dependency (`uv sync --reinstall-package dbt-dagsterizer`) and re-sync template macros.
- Added a dedicated local development guide for iterating on `dbt-dagsterizer` with a rendered validation project.
- Added `--schedule-timezone` to `project init` so rendered projects can set the initial schedule execution timezone in `dbt_project/dagsterization.yml`.
- Added `meta timezone` documentation and examples covering both scaffold-time and later timezone changes.

### Fixed

- Fixed observable source assets to set `group_name="source"` so source-like assets do not appear in Dagster’s `default` group.
- Clarified in the sample template docs that `ods_test_*` models only bootstrap demo ODS tables locally and may make the Dagster lineage graph differ from a production project.
- Fixed relation-based asset key sanitization to run through the shared key builder so dbt assets, jobs, and sensors continue to reference the same keys.
- Fixed timezone validation to reject invalid IANA timezone names before schedule definitions are created.

### Changed

- Changed the rendered project template to accept a `schedule_timezone` Cookiecutter parameter, defaulting to `UTC`.

## [0.3.1] - 2026-06-10

### Added

- Added optional `watermark_sql` support for observable source assets as an alternative to `watermark_column`.
- Documented `watermark_sql` usage, precedence, and scalar-result expectations in the template docs.

## [0.3.0] - 2026-06-10

### Breaking

- BREAKING: dbt AssetKeys now use relation-based keys instead of logical names: `dbt/<database>/<schema>/<identifier>`.
- For adapters that omit the `database` field (common for StarRocks), empty components are omitted and `schema` is treated as the database, so keys may look like `dbt/<database>/<identifier>`.
- Migration: update any asset selections/configs that reference old model-name keys. Legacy manual `DBT_JOB_SPECS` and `PARTITION_CHANGE_PROPAGATION_SPECS` are upgraded at runtime when the model exists in the manifest, but relation-based keys are the recommended steady state.

### Added

- Added a dedicated observability guide (OpenTelemetry + Elastic APM) and linked it from relevant docs.
- Added `offset_days` support for `daily_at` schedules across orchestration config, CLI metadata commands, validation, and schedule preset tests.
- Added relation metadata (`database`, `schema`, `identifier`) to parsed dbt manifest model records for downstream job and sensor generation.
- Added documentation for relation-based dbt AssetKeys, folder-derived Dagster group names, `offset_days`, and manual-spec compatibility guidance.

### Changed

- Changed auto-generated dbt job specs and partition-change propagation specs to use relation-based AssetKeys consistently.
- Changed dbt model group naming in Dagster to derive the group from the first folder under `models/`, with fallback to non-model resource type or dbt FQN when needed.
- Changed the sample ODS test models to use StarRocks `PRIMARY` tables with explicit primary keys.

### Fixed

- Fixed partition-change propagators to query upstream materialization events using the same relation-based AssetKeys emitted by dbt assets.
- Fixed compatibility for manual/custom `DBT_JOB_SPECS` so legacy `["dbt", "<model>"]` asset-key selections are upgraded to relation-based keys at runtime.
- Fixed compatibility for manual `PARTITION_CHANGE_PROPAGATION_SPECS` so legacy model-name-only configs are upgraded to relation-based upstream asset keys at runtime.
- Fixed rendered sample projects to enable `ods_test` models by default so they appear in Dagster assets and lineage without extra dbt parse flags.

## [0.2.4] - 2026-05-20

### Added

- Added documentation describing the Dagster Kubernetes execution model (daemon vs code location vs run pods) and how environment variables propagate.
- Added `--output-name` to `project init` to control the rendered project folder name.

### Changed

- `project init` now defaults the output folder name to a kebab-case name derived from `--project-name` (package/module names are still normalized separately).

## [0.2.3] - 2026-05-17

### Added

- Added `project gen-gitops-env` to generate kustomize-style app ConfigMap/Secret YAML from a rendered project’s `.env` file.

### Changed

- `project gen-gitops-env` now normalizes Kubernetes resource names (underscores become hyphens) by default.
- `project gen-gitops-env` now defaults `--dagster-home` to `/tmp/dagster_home`.

## [0.2.2] - 2026-05-16

### Added

- Added a root Makefile with `build` / `publish` targets for packaging and publishing via `uv`.
- Added `--dbt-dagsterizer-version` to `project init` to optionally pin `dbt-dagsterizer` in rendered projects (defaults to the installed CLI version when available).
- Added `--no-pin-dbt-dagsterizer` to `project init` to leave `dbt-dagsterizer` unpinned in rendered projects.

### Changed

- Made `--dbt-dagsterizer-version` and `--no-pin-dbt-dagsterizer` mutually exclusive, and require `--dbt-dagsterizer-version` to be non-empty when explicitly provided.

## [0.2.1] - 2026-05-15

### Added

- Added Kubernetes run pod environment injection via the per-job `dagster-k8s/config` tag, controlled by `LUBAN_RUN_ENV_CONFIGMAP` and `LUBAN_RUN_ENV_SECRET`.

## [0.2.0] - 2026-05-13

### Added

- Added OpenTelemetry (OTEL) bootstrap support and safe-by-default exporter configuration controlled by `OTEL_*` environment variables.
- Added Dagster-aware OTEL transaction naming (schedule/sensor/backfill/job/asset-job) and improved span structure for dbt execution.
- Added `--namespace` to `project init` to align rendered code locations with Luban CI project/app naming conventions.
- Added template defaults for namespace-aware OTEL service naming and StarRocks database naming.
- Added local development and Elastic APM smoke-test documentation for OTEL trace export.
- Added optional dbt `run_results.json` breakdown telemetry to emit per-node child spans or span events under the `dbt.cli` span (controlled by `LUBAN_OTEL_DBT_RUN_RESULTS_*`).
- Added watermark-based dedupe for partition-change detectors (per-partition cursor + `run_key` includes partition watermark) to avoid repeatedly scheduling the same partitions under backlog.
- Added StarRocks client row query helper and shared connection helper for reuse across query methods.
- Added documentation for detector `impact` range configuration and real-world examples.

### Changed

- Improved OTLP HTTP endpoint handling for Elastic APM by normalizing endpoints and ensuring OTLP HTTP signal paths are correct.
- Improved observability docs and template `.env.example` to make OTEL configuration self-explanatory.
- Changed the template ODS test append behavior so incremental appends set `updated_at`/`ods_updated_at` to “now”, enabling partition-change detectors that use `updated_at_expr`.
- Changed default `daily_at` schedule preset to target yesterday (`partition_offset_days=1`) to avoid scheduling partitions that do not exist yet.

### Fixed

- Fixed OTEL tagging for non-partitioned Dagster runs by safely handling partition context access.
- Fixed stray top-level “transactions” in Elastic APM by ensuring helper spans are created as children under the transaction span.
- Fixed `impact`/impact range behavior for partition-change detectors when using watermark-based dedupe by restoring neighbor-partition expansion and clamping to the detector window.

## [0.1.13] - 2026-04-18

### Added

- Added a template fingerprint file (`dbt_project/.dbt_dagsterizer_template`) to allow `macros sync` to select the correct embedded template without CLI flags.

### Changed

- Managed dbt macros now live inside the embedded template under `dbt_project/macros/dbt_dagsterizer/` so freshly rendered projects include required macros by default.
- Replaced the `macros install` workflow with `macros sync`, which syncs the packaged managed macros into `macros/dbt_dagsterizer/` in a dbt project.

### Removed

- Removed the example `row_count_greater_than` test macro and its references from the sample dbt project.
- Removed the legacy `macro_templates/` directory; managed macros are now sourced from the embedded template.

## [0.1.12] - 2026-04-16

### Fixed

- Fixed Dagster runs without a partition key against partitioned dbt assets by providing a safe default daily dbt vars window instead of failing with missing `min_datetime`/`max_datetime`.

## [0.1.11] - 2026-04-16

### Fixed

- Fixed unpartitioned execution of partitioned dbt assets by handling Dagster partition context access safely.
- Improved template CLI behavior and docs:
  - Narrowed exception handling for project template helpers.
  - Clarified when `dbt deps` runs during manifest preparation.
- Disabled Dagster telemetry by default in the rendered template Dagster instance config.

## [0.1.10] - 2026-04-16

### Added

- Added CLI equivalents to template developer workflow docs to make it easy to manage `dagsterization.yml` via CLI or direct edits.
- Added tests for daily partitions env var enforcement and dbt retry decision logic.

### Changed

- Improved template runtime ergonomics:
  - Rendered projects now get an absolute `DAGSTER_HOME` written into `.env.example` at generation time.
  - `make setup` copies `.env.example` to `.env` before installing dependencies/macros.
- Hardened partition-change propagation sensor:
  - Resets invalid cursors safely instead of scanning history unexpectedly.
  - Emits stable upstream asset key tags.
- Aligned template schedule spec tests with supported schedule type(s).
- Clarified docs about when `dbt deps` runs during manifest preparation.
- Improved CLI error handling:
  - Narrowed Dagster version fallback to only “package not installed”.
  - `project list-templates` now surfaces errors instead of silently printing nothing.
- Refined docs for the two primary usage modes:
  - Install/use as a CLI tool (via `uv tool install/upgrade`).
  - Use as a runtime Python dependency inside a Dagster code location.
- Simplified doc examples by removing redundant `--dbt-project-dir dbt_project` where the default applies.

## [0.1.9] - 2026-04-16

### Added

- Centralized daily partition start-date handling into a shared helper.

### Changed

- Daily partitions now require `DAGSTER_DAILY_PARTITIONS_START_DATE` when daily partitions are used.
- Replaced manual dbt retry (sleep + rerun) with a Dagster retry request.
- Narrowed broad exception capture in orchestration validation to expected error types.
- Updated template docs to reflect current behavior and defaults.

## [0.1.8] - 2026-04-15

### Added

- Initial published version.

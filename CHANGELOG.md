# Changelog

All notable changes to `dbt-dagsterizer` will be documented in this file.

The format is based on Keep a Changelog, and this project adheres to Semantic Versioning.

## [Unreleased]

### Added

- Added `--local-dbt-dagsterizer-path` to `project init` to render projects that depend on a local `dbt-dagsterizer` checkout via a `file://` dependency.
- Added `make refresh-dagsterizer` to the rendered project template to reinstall a local `dbt-dagsterizer` dependency (`uv sync --reinstall-package dbt-dagsterizer`) and re-sync template macros.
- Added a dedicated local development guide for iterating on `dbt-dagsterizer` with a rendered validation project.
- Added dynamic partitioning support for non-time-based partition strategies (e.g., country codes, tenant IDs, data sources):
  - Introduced `partitions.dynamic` section in `dagsterization.yml` to define named dynamic partitions with `initial_partition_keys` and model assignments.
  - Added `DynamicPartitionsDefinition` caching and management system (`partitions_dynamic.py`) for creating and updating dynamic partition definitions.
  - Extended orchestration configuration system to parse, index, and manage dynamic partition specs alongside daily and unpartitioned models.
  - Updated asset translator (`LubanDagsterDbtTranslator`) to resolve dynamic partition specs and assign correct `PartitionsDefinition` to assets.
  - Extended job factory to create asset jobs with dynamic partition support using `dynamic:partition_name` specs.
  - Added schedule factory support for dynamic partitions with schedules that emit `RunRequest` for all dynamic partition keys.
  - Implemented dynamic partitions bootstrap sensor that synchronizes partition keys from `dagsterization.yml` to Dagster instance at startup.
  - Added partition change detector framework for dynamic partitions (watermark-based deduplication and change detection).
  - Created centralized partitions registry for initializing and caching dynamic partition definitions across all components.
  - Added comprehensive unit tests for dynamic partition caching, configuration parsing, spec resolution, and asset translator integration.

### Fixed

- Fixed observable source assets to set `group_name="source"` so source-like assets do not appear in Dagster’s `default` group.
- Clarified in the sample template docs that `ods_test_*` models only bootstrap demo ODS tables locally and may make the Dagster lineage graph differ from a production project.

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

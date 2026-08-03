# Release Notes

Version-specific release notes for `dbt-dagsterizer`.

These notes are intended to be a polished companion to `CHANGELOG.md`:

- `CHANGELOG.md` remains the cumulative project history.
- This document provides concise, version-by-version release summaries that are easy to reuse for GitHub Releases and upgrade communication.

## v0.4.0

Release date: 2026-08-03

### Summary

`v0.4.0` adds optional StarRocks→SQL Server replication, SSRS subscription triggering, and row-count observability for dbt assets, and rounds out the operator workflow with clearer docs and safer defaults.

### Added

- Added optional StarRocks-to-MSSQL data replication using dlt, configured via `replication` entries in `dagsterization.yml`.
- Added optional SSRS subscription triggering via `ssrs_reports` entries in `dagsterization.yml` (and `meta report` / `meta report-delete` CLI commands).
- Added row-count observations for dbt assets (affected rows and total row count) to improve Dagster UI observability.
- Added package version display in dbt asset descriptions to make deployed code-location versions easier to verify.
- Added `partitions.daily_config.include_current_day_partition` to optionally include today’s partition for daily-partitioned orchestration.

### Fixed

- Fixed replication executor logging to avoid leaking URL-encoded credentials.
- Fixed SSRS `enabled` defaults to be consistent across docs, CLI defaults, and runtime auto-configuration (defaults to enabled).
- Fixed orchestration validation to warn reliably when `materialize_at_startup` is used on daily-partitioned models without scanning manifest tags unnecessarily.
- Fixed `build_definitions()` environment handling to avoid overriding `DBT_PROJECT_DIR` unless explicitly provided.
- Fixed rendered template test modules to remain syntactically valid before Cookiecutter rendering.

### Upgrade Notes

- No migration is required.
- If you configure SSRS reports, `enabled` now defaults to `true` when omitted in `dagsterization.yml`; set `enabled: false` to opt out for specific reports.

## v0.3.3

Release date: 2026-07-11

### Summary

`v0.3.3` makes the new PR 6 automation behavior easier to trust in production by documenting the broader metadata-driven rules, fixing observable-source SQL generation for external catalogs, and adding focused regression coverage around both paths.

### Added

- Added regression tests for translator automation selection across observable-source matches, daily eager mode, `dim` tags, and `automation_table` tags.
- Added regression coverage for observable source SQL generation when database, table, or watermark-column identifiers contain dotted catalog-style names.

### Changed

- Changed dbt automation selection so it no longer depends on `dwd`/`dws` FQN path segments and instead uses observable-source metadata, daily partition configuration, and model tags.
- Updated the template usage guide to explain the current automation rules in terms of model metadata and partition config rather than template folder layout.

### Fixed

- Fixed observable source SQL generation to quote each part of dotted identifiers separately, which restores correct query construction for external catalog references.
- Fixed the sample `.env.example` comment so `LUBAN_PARTITION_CHANGE_PROPAGATOR_MODE=eager` is described in terms of the models that actually opt into eager automation.

### Upgrade Notes

- No migration is required.
- If you previously assumed eager automation only applied to `dwd`/`dws` folder placement, review your model tags and daily partition config instead; those are now the relevant control points.

## v0.3.2

Release date: 2026-07-11

### Summary

`v0.3.2` improves schedule-timezone ergonomics across project scaffolding and day-2 configuration, tightens validation so invalid timezone values are rejected earlier, and removes a Dagster deprecation warning from the job-selection path.

### Added

- Added `--schedule-timezone` to `project init` so rendered projects can set the initial schedule execution timezone in `dbt_project/dagsterization.yml`.
- Added direct documentation and examples for `meta timezone` so users can update schedule execution timezone after project generation.

### Changed

- Changed the rendered project template to accept a `schedule_timezone` Cookiecutter parameter, defaulting to `UTC`.
- Updated CLI, getting-started, and template workflow docs to explain both scaffold-time and later timezone configuration paths with concrete examples.

### Fixed

- Fixed relation-based asset key sanitization to flow through the shared key builder so dbt assets, jobs, and sensors keep referencing the same keys.
- Fixed timezone validation to reject invalid IANA timezone names consistently across runtime config, `project init`, and direct Cookiecutter rendering.
- Fixed the remaining Dagster deprecation warning by replacing `AssetSelection.keys(...)` with `AssetSelection.assets(...)`.

### Upgrade Notes

- No migration is required.
- New projects still default to `UTC`; use `--schedule-timezone` during `project init` or `dbt-dagsterizer meta timezone --timezone ...` later if you want a different schedule execution timezone.

## v0.3.1

Release date: 2026-06-10

### Summary

`v0.3.1` adds optional `watermark_sql` support for observable source assets while preserving compatibility with the existing `watermark_column` workflow.

### Added

- Added optional `watermark_sql` support for source observation as an alternative to `watermark_column`.

### Changed

- Preserved backward compatibility for existing observable source specs that only define `watermark_column`.

### Documentation

- Documented `watermark_sql` usage in the template developer workflow.
- Documented precedence rules when both `watermark_sql` and `watermark_column` are set.
- Documented the requirement that `watermark_sql` must return a single scalar value.

### Upgrade Notes

- No migration is required for existing `watermark_column` configurations.
- If both fields are configured, `watermark_sql` takes precedence.

## v0.3.0

Release date: 2026-06-10

### Summary

`v0.3.0` introduces relation-based dbt AssetKeys, enabling stable cross-code-location alignment in Dagster, and expands orchestration ergonomics around schedules, observability, and docs.

### Added

- Added relation metadata (`database`, `schema`, `identifier`) to parsed dbt manifest models for downstream orchestration generation.
- Added `offset_days` support for `daily_at` schedules across config, metadata commands, validation, and tests.
- Added an observability guide for OpenTelemetry and Elastic APM.
- Added documentation for relation-based AssetKeys, group naming, `offset_days`, and manual-spec compatibility.

### Changed

- Switched dbt AssetKeys to relation-based keys instead of logical model-name keys.
- Updated auto-generated job specs and partition-change propagation specs to use relation-based keys consistently.
- Derived Dagster model group names from the first folder under `models/`.
- Updated the sample ODS models to use StarRocks `PRIMARY` tables with explicit primary keys.

### Fixed

- Fixed partition-change propagators to query upstream materialization events with the same relation-based keys emitted by dbt assets.
- Fixed runtime compatibility for legacy manual `DBT_JOB_SPECS` and `PARTITION_CHANGE_PROPAGATION_SPECS` where matching models exist in the manifest.
- Fixed rendered sample projects so `ods_test` models are enabled by default.

### Breaking Changes

- dbt AssetKeys now use relation-based keys such as `dbt/<database>/<schema>/<identifier>`.
- For adapters that omit `database` values, empty components are omitted and `schema` is treated as the database component.

### Upgrade Notes

- Review any asset selections, sensors, or manual config that reference legacy model-name dbt AssetKeys.
- Runtime compatibility helpers are available, but relation-based keys are the recommended steady state.

## v0.2.4

Release date: 2026-05-20

### Summary

`v0.2.4` improves project initialization ergonomics and expands the documentation around Dagster's Kubernetes execution model.

### Added

- Added documentation describing the Dagster Kubernetes execution model and environment-variable propagation.
- Added `--output-name` to `project init` to control the rendered project folder name.

### Changed

- `project init` now defaults the output folder name to a kebab-case value derived from `--project-name`.

### Upgrade Notes

- No migration is required.
- Use `--output-name` when you need the rendered folder name to differ from the project name.

## v0.2.3

Release date: 2026-05-17

### Summary

`v0.2.3` adds GitOps-friendly generation of Dagster runtime environment manifests from a rendered project's `.env` file.

### Added

- Added `project gen-gitops-env` to generate kustomize-style ConfigMap and Secret YAML from a rendered project's `.env` file.

### Changed

- Normalized generated Kubernetes resource names so underscores become hyphens by default.
- Defaulted `--dagster-home` to `/tmp/dagster_home` for generated manifests.

### Upgrade Notes

- No migration is required.
- Review generated resource names if your previous manual manifests used underscores.

## v0.2.2

Release date: 2026-05-16

### Summary

`v0.2.2` improves packaging and project initialization by making `dbt-dagsterizer` version pinning explicit and easier to control.

### Added

- Added a root `Makefile` with `build` and `publish` targets for packaging and publishing via `uv`.
- Added `--dbt-dagsterizer-version` to `project init` so rendered projects can pin a specific package version.
- Added `--no-pin-dbt-dagsterizer` to `project init` for intentionally unpinned rendered projects.

### Changed

- Made `--dbt-dagsterizer-version` and `--no-pin-dbt-dagsterizer` mutually exclusive.
- Required `--dbt-dagsterizer-version` to be non-empty when explicitly provided.

### Upgrade Notes

- No migration is required.
- Choose between explicit pinning and no pinning when regenerating projects.

## v0.2.1

Release date: 2026-05-15

### Summary

`v0.2.1` adds Kubernetes run-pod environment injection so jobs launched by Dagster can inherit the code location's runtime configuration more easily.

### Added

- Added run pod environment injection via the per-job `dagster-k8s/config` tag.
- Added support for configuring injected ConfigMap and Secret names through `LUBAN_RUN_ENV_CONFIGMAP` and `LUBAN_RUN_ENV_SECRET`.

### Upgrade Notes

- No migration is required.
- Set the environment variables on the code-server deployment when using `K8sRunLauncher` and shared runtime credentials.

## v0.2.0

Release date: 2026-05-13

### Summary

`v0.2.0` significantly expands observability support, improves partition-change detection, and tightens the rendered template defaults for Dagster and dbt projects.

### Added

- Added OpenTelemetry bootstrap support and safe-by-default exporter configuration controlled by `OTEL_*` environment variables.
- Added Dagster-aware OTEL transaction naming and improved span structure for dbt execution.
- Added `--namespace` to `project init` to align rendered code locations with Luban CI naming conventions.
- Added template defaults for namespace-aware OTEL service naming and StarRocks database naming.
- Added local development and Elastic APM smoke-test documentation.
- Added optional dbt `run_results.json` breakdown telemetry.
- Added watermark-based dedupe for partition-change detectors.
- Added a reusable StarRocks client row-query helper and shared connection helper.
- Added documentation for detector `impact` range configuration.

### Changed

- Improved OTLP HTTP endpoint handling for Elastic APM.
- Improved observability docs and template `.env.example` defaults.
- Changed template ODS test append behavior so incremental appends set `updated_at` timestamps to current time.
- Changed the default `daily_at` schedule preset to target yesterday (`partition_offset_days=1`).

### Fixed

- Fixed OTEL tagging for non-partitioned Dagster runs.
- Fixed stray top-level transactions in Elastic APM by ensuring helper spans stay under the transaction span.
- Fixed `impact` range behavior for partition-change detectors when watermark-based dedupe is enabled.

### Upgrade Notes

- Review schedule expectations if you relied on the old same-day `daily_at` default.
- Enable the new OTEL settings gradually if you are integrating with an existing observability stack.

## v0.1.13

Release date: 2026-04-18

### Summary

`v0.1.13` improves managed dbt macro distribution by embedding macros directly in the packaged template and replacing the old install workflow with sync.

### Added

- Added a template fingerprint file so `macros sync` can select the correct embedded template without extra CLI flags.

### Changed

- Moved managed dbt macros into the embedded template so freshly rendered projects include the required macros by default.
- Replaced the `macros install` workflow with `macros sync`.

### Removed

- Removed the example `row_count_greater_than` test macro and its references.
- Removed the legacy `macro_templates/` directory.

### Upgrade Notes

- Switch any automation or docs that still reference `macros install` to `macros sync`.

## v0.1.12

Release date: 2026-04-16

### Summary

`v0.1.12` fixes Dagster runs without a partition key when they target partitioned dbt assets.

### Fixed

- Provided a safe default daily dbt vars window for runs without a partition key instead of failing on missing `min_datetime` and `max_datetime`.

### Upgrade Notes

- No migration is required.
- This release is recommended if you run partitioned dbt assets from non-partitioned Dagster contexts.

## v0.1.11

Release date: 2026-04-16

### Summary

`v0.1.11` stabilizes unpartitioned execution of partitioned dbt assets and cleans up generated template behavior and docs.

### Fixed

- Fixed unpartitioned execution of partitioned dbt assets by handling Dagster partition context access safely.

### Changed

- Narrowed exception handling for project template helpers.
- Clarified when `dbt deps` runs during manifest preparation.
- Disabled Dagster telemetry by default in the rendered template Dagster instance config.

### Upgrade Notes

- No migration is required.
- This release is recommended if you saw partition-context failures in unpartitioned runs.

## v0.1.10

Release date: 2026-04-16

### Summary

`v0.1.10` improves template runtime ergonomics, expands CLI coverage for docs workflows, and hardens retry and partition-change behavior.

### Added

- Added CLI equivalents to the template developer workflow docs so `dagsterization.yml` can be managed from the CLI or by editing files directly.
- Added tests for daily partition start-date enforcement and dbt retry decision logic.

### Changed

- Rendered projects now get an absolute `DAGSTER_HOME` written into `.env.example`.
- `make setup` now copies `.env.example` to `.env` before installing dependencies and macros.
- Hardened the partition-change propagation sensor to reset invalid cursors safely and emit stable upstream asset key tags.
- Aligned template schedule spec tests with supported schedule types.
- Improved CLI error handling and docs for installation, usage modes, and manifest preparation.
- Simplified doc examples by removing redundant default `--dbt-project-dir` usage.

### Upgrade Notes

- No migration is required.
- Re-run setup in existing rendered projects if you want the newer `.env.example` and bootstrap behavior.

## v0.1.9

Release date: 2026-04-16

### Summary

`v0.1.9` tightens partition configuration requirements and improves retry behavior for dbt execution.

### Added

- Added a shared helper for daily partition start-date handling.

### Changed

- Daily partitions now require `DAGSTER_DAILY_PARTITIONS_START_DATE` when daily partitions are used.
- Replaced manual dbt retry logic with a Dagster retry request.
- Narrowed broad exception capture in orchestration validation.
- Updated template docs to reflect current behavior and defaults.

### Upgrade Notes

- Set `DAGSTER_DAILY_PARTITIONS_START_DATE` in environments that use daily partitions.

## v0.1.8

Release date: 2026-04-15

### Summary

`v0.1.8` is the initial published release of `dbt-dagsterizer`.

### Highlights

- Published the first installable version of the package.
- Established the initial CLI and runtime package workflow for dbt-driven Dagster automation.

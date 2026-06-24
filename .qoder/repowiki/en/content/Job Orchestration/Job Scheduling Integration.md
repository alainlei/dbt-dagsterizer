# Job Scheduling Integration

<cite>
**Referenced Files in This Document**
- [schedules/__init__.py](file://src/dbt_dagsterizer/schedules/__init__.py)
- [schedules/dbt/schedules.py](file://src/dbt_dagsterizer/schedules/dbt/schedules.py)
- [schedules/dbt/auto_config.py](file://src/dbt_dagsterizer/schedules/dbt/auto_config.py)
- [schedules/dbt/factory.py](file://src/dbt_dagsterizer/schedules/dbt/factory.py)
- [schedules/dbt/presets.py](file://src/dbt_dagsterizer/schedules/dbt/presets.py)
- [schedules/sources/schedules.py](file://src/dbt_dagsterizer/schedules/sources/schedules.py)
- [jobs/dbt/jobs.py](file://src/dbt_dagsterizer/jobs/dbt/jobs.py)
- [jobs/dbt/factory.py](file://src/dbt_dagsterizer/jobs/dbt/factory.py)
- [jobs/dbt/auto_config.py](file://src/dbt_dagsterizer/jobs/dbt/auto_config.py)
- [jobs/dbt/presets.py](file://src/dbt_dagsterizer/jobs/dbt/presets.py)
- [orchestration_config.py](file://src/dbt_dagsterizer/orchestration_config.py)
- [sensors/partition_change/detector/factory.py](file://src/dbt_dagsterizer/sensors/partition_change/detector/factory.py)
- [sensors/partition_change/propagator/factory.py](file://src/dbt_dagsterizer/sensors/partition_change/propagator/factory.py)
- [sensors/dynamic_partitions_bootstrap.py](file://src/dbt_dagsterizer/sensors/dynamic_partitions_bootstrap.py)
- [cli_parts/project.py](file://src/dbt_dagsterizer/cli_parts/project.py)
- [cli_parts/macros.py](file://src/dbt_dagsterizer/cli_parts/macros.py)
- [partitions_dynamic.py](file://src/dbt_dagsterizer/partitions_dynamic.py)
- [partitions_registry.py](file://src/dbt_dagsterizer/partitions_registry.py)
- [partitions.py](file://src/dbt_dagsterizer/partitions.py)
- [assets/dbt/assets.py](file://src/dbt_dagsterizer/assets/dbt/assets.py)
- [test_dynamic_partitions.py](file://src/dbt_dagsterizer/tests/test_dynamic_partitions.py)
</cite>

## Update Summary
**Changes Made**
- Enhanced dynamic partition scheduling support with new `_build_dynamic_partitioned_schedule` function
- Updated schedule factory section to document the new dynamic partition schedule builder
- Added comprehensive documentation for dynamic partition configuration and management
- Enhanced partition management section with detailed dynamic partition integration
- Updated dependency analysis to include dynamic partition components and their relationships
- Added new section covering dynamic partition scheduling patterns and best practices

## Table of Contents
1. [Introduction](#introduction)
2. [Project Structure](#project-structure)
3. [Core Components](#core-components)
4. [Architecture Overview](#architecture-overview)
5. [Detailed Component Analysis](#detailed-component-analysis)
6. [Dependency Analysis](#dependency-analysis)
7. [Performance Considerations](#performance-considerations)
8. [Troubleshooting Guide](#troubleshooting-guide)
9. [Conclusion](#conclusion)
10. [Appendices](#appendices)

## Introduction
This document explains how job scheduling integrates with Dagster's built-in scheduler and complementary event-driven mechanisms. It covers schedule configuration via orchestration metadata, cron-based triggers, partition-aware execution, and event-driven triggers for partition changes and propagation. It also documents monitoring and alerting patterns, hybrid scheduling approaches, failover and high availability considerations, and operational procedures for validation, drift detection, and maintenance.

**Updated** Enhanced with comprehensive support for dynamic partition scheduling, allowing schedules to emit RunRequests across arbitrary partition dimensions beyond time-based daily partitions using the new `_build_dynamic_partitioned_schedule` function.

## Project Structure
The scheduling system is organized around three pillars:
- Orchestration configuration: centralized YAML that defines jobs, schedules, partitions, and partition-change automation.
- Schedules: cron-based schedules that trigger partitioned runs for dbt asset jobs.
- Sensors: event-driven triggers for partition-change detection and propagation.

```mermaid
graph TB
OC["OrchestrationConfig<br/>defines jobs, schedules, partitions"] --> SAuto["Schedule Auto-Config<br/>build_auto_dbt_schedule_specs"]
OC --> JAuto["Job Auto-Config<br/>build_auto_dbt_job_specs"]
SAuto --> SFac["Schedule Factory<br/>build_dbt_schedules"]
JAuto --> JFac["Job Factory<br/>build_dbt_asset_jobs"]
SFac --> SDef["Schedule Definitions"]
JFac --> JObjs["Job Objects"]
SDef --> DAgg["get_schedules() aggregator"]
JObjs --> DAgg
ObsSrc["Sources Observe Schedule"] --> DAgg
PCDet["Partition-Change Detector Sensors"] --> DAgg
PCProp["Partition-Propagation Sensors"] --> DAgg
DynParts["Dynamic Partitions Registry"] --> SFac
DynParts --> JFac
DynBootstrap["Dynamic Partitions Bootstrap Sensor"] --> DynParts
```

**Diagram sources**
- [orchestration_config.py](file://src/dbt_dagsterizer/orchestration_config.py)
- [schedules/dbt/auto_config.py](file://src/dbt_dagsterizer/schedules/dbt/auto_config.py)
- [schedules/dbt/factory.py](file://src/dbt_dagsterizer/schedules/dbt/factory.py)
- [jobs/dbt/auto_config.py](file://src/dbt_dagsterizer/jobs/dbt/auto_config.py)
- [jobs/dbt/factory.py](file://src/dbt_dagsterizer/jobs/dbt/factory.py)
- [schedules/__init__.py](file://src/dbt_dagsterizer/schedules/__init__.py)
- [schedules/sources/schedules.py](file://src/dbt_dagsterizer/schedules/sources/schedules.py)
- [sensors/partition_change/detector/factory.py](file://src/dbt_dagsterizer/sensors/partition_change/detector/factory.py)
- [sensors/partition_change/propagator/factory.py](file://src/dbt_dagsterizer/sensors/partition_change/propagator/factory.py)
- [partitions_registry.py](file://src/dbt_dagsterizer/partitions_registry.py)
- [sensors/dynamic_partitions_bootstrap.py](file://src/dbt_dagsterizer/sensors/dynamic_partitions_bootstrap.py)

**Section sources**
- [orchestration_config.py](file://src/dbt_dagsterizer/orchestration_config.py)
- [schedules/dbt/auto_config.py](file://src/dbt_dagsterizer/schedules/dbt/auto_config.py)
- [jobs/dbt/auto_config.py](file://src/dbt_dagsterizer/jobs/dbt/auto_config.py)
- [schedules/__init__.py](file://src/dbt_dagsterizer/schedules/__init__.py)

## Core Components
- Orchestration configuration: loads and normalizes a YAML file that defines jobs, schedules, partitions, and partition-change automation. It validates and indexes the configuration to support auto-scheduling and job derivation.
- Schedule auto-config: reads orchestration metadata to produce schedule specs with cron expressions, partition offsets, and lookbacks.
- Schedule factory: converts schedule specs into Dagster ScheduleDefinition instances, binding jobs and partition windows.
- Job auto-config and factory: derive jobs from orchestration metadata and dbt manifests, supporting asset-based and CLI-based jobs with partitioning.
- Sources observe schedule: a periodic schedule to scan observable sources at a configurable cadence.
- Partition-change sensors: detect watermark changes and emit RunRequests for impacted partitions.
- Propagation sensors: react to upstream materializations and trigger downstream jobs per partition.
- Dynamic partitions: manage arbitrary partition dimensions (e.g., country codes, tenant IDs) with runtime key management through the registry system.

**Updated** Enhanced with dynamic partition scheduling support through the new `_build_dynamic_partitioned_schedule` function and comprehensive dynamic partition management infrastructure.

**Section sources**
- [orchestration_config.py](file://src/dbt_dagsterizer/orchestration_config.py)
- [schedules/dbt/auto_config.py](file://src/dbt_dagsterizer/schedules/dbt/auto_config.py)
- [schedules/dbt/factory.py](file://src/dbt_dagsterizer/schedules/dbt/factory.py)
- [jobs/dbt/auto_config.py](file://src/dbt_dagsterizer/jobs/dbt/auto_config.py)
- [jobs/dbt/factory.py](file://src/dbt_dagsterizer/jobs/dbt/factory.py)
- [schedules/sources/schedules.py](file://src/dbt_dagsterizer/schedules/sources/schedules.py)
- [sensors/partition_change/detector/factory.py](file://src/dbt_dagsterizer/sensors/partition_change/detector/factory.py)
- [sensors/partition_change/propagator/factory.py](file://src/dbt_dagsterizer/sensors/partition_change/propagator/factory.py)
- [partitions_dynamic.py](file://src/dbt_dagsterizer/partitions_dynamic.py)
- [partitions_registry.py](file://src/dbt_dagsterizer/partitions_registry.py)

## Architecture Overview
The system composes schedules and sensors from orchestration metadata and dbt manifests. Cron-based schedules trigger partitioned runs; event-driven sensors complement or replace cron for near-real-time responsiveness.

```mermaid
sequenceDiagram
participant Orch as "OrchestrationConfig"
participant SAuto as "Schedule Auto-Config"
participant SFac as "Schedule Factory"
participant JAuto as "Job Auto-Config"
participant JFac as "Job Factory"
participant DynReg as "Dynamic Partitions Registry"
participant DynBoot as "Dynamic Partitions Bootstrap Sensor"
participant Sched as "Dagster Scheduler"
participant Job as "Job Instance"
Orch-->>SAuto : "Load and index schedules"
SAuto-->>SFac : "Provide schedule specs"
SFac-->>DynReg : "Resolve dynamic partitions"
SFac-->>Sched : "Register ScheduleDefinition(s)"
SFac-->>DynBoot : "Initialize dynamic partitions"
Orch-->>JAuto : "Load and index jobs"
JAuto-->>JFac : "Provide job specs"
JFac-->>DynReg : "Resolve dynamic partitions"
JFac-->>Sched : "Register Job(s)"
Sched->>Job : "On cron tick : submit RunRequest(s)"
Job-->>Sched : "Execution logs and result"
```

**Diagram sources**
- [orchestration_config.py](file://src/dbt_dagsterizer/orchestration_config.py)
- [schedules/dbt/auto_config.py](file://src/dbt_dagsterizer/schedules/dbt/auto_config.py)
- [schedules/dbt/factory.py](file://src/dbt_dagsterizer/schedules/dbt/factory.py)
- [jobs/dbt/auto_config.py](file://src/dbt_dagsterizer/jobs/dbt/auto_config.py)
- [jobs/dbt/factory.py](file://src/dbt_dagsterizer/jobs/dbt/factory.py)
- [partitions_registry.py](file://src/dbt_dagsterizer/partitions_registry.py)
- [sensors/dynamic_partitions_bootstrap.py](file://src/dbt_dagsterizer/sensors/dynamic_partitions_bootstrap.py)

## Detailed Component Analysis

### Schedule Configuration and Execution Triggers
- Orchestration metadata defines schedules with type, cadence, job binding, and partition parameters. The loader ensures defaults and validates presence of required fields.
- Schedule presets convert human-friendly daily-at parameters into cron expressions and standardized schedule specs.
- The schedule factory enforces uniqueness, builds cron-based schedules, and computes partition windows for each tick.

```mermaid
flowchart TD
Start(["Load Orchestration"]) --> Parse["Parse Schedules Section"]
Parse --> Validate{"Validate Fields"}
Validate --> |Invalid| Error["Raise Validation Error"]
Validate --> |Valid| Preset["Apply Preset (e.g., daily_at)"]
Preset --> BuildSpecs["Build Schedule Specs"]
BuildSpecs --> Factory["Factory Builds Schedules"]
Factory --> Register["Register with Dagster Scheduler"]
Register --> End(["Ready"])
Error --> End
```

**Diagram sources**
- [orchestration_config.py](file://src/dbt_dagsterizer/orchestration_config.py)
- [schedules/dbt/presets.py](file://src/dbt_dagsterizer/schedules/dbt/presets.py)
- [schedules/dbt/factory.py](file://src/dbt_dagsterizer/schedules/dbt/factory.py)

**Section sources**
- [orchestration_config.py](file://src/dbt_dagsterizer/orchestration_config.py)
- [schedules/dbt/presets.py](file://src/dbt_dagsterizer/schedules/dbt/presets.py)
- [schedules/dbt/factory.py](file://src/dbt_dagsterizer/schedules/dbt/factory.py)

### Timing Coordination and Partition Windows
- Each schedule computes an anchor day from the scheduled execution time and applies offset and lookback parameters to generate a contiguous window of partitions.
- Run keys incorporate schedule identity and partition date to ensure idempotency across ticks when configured.

**Updated** Enhanced to support both daily and dynamic partition types with different partition key iteration strategies. Daily partitions use date-based iteration while dynamic partitions iterate through all configured partition keys.

```mermaid
flowchart TD
Tick["Scheduled Execution Time"] --> Anchor["Compute Anchor Day<br/>with Offset"]
Anchor --> Window["Define Partition Window<br/>using Lookback"]
Window --> Emit["Emit RunRequests per Partition"]
Emit --> Keys["Attach Run Keys<br/>Optionally Suffix with Timestamp"]
DynTick["Dynamic Partition Tick"] --> KeysList["Iterate Through<br/>All Dynamic Keys"]
KeysList --> EmitDyn["Emit RunRequests for<br/>Each Dynamic Key"]
EmitDyn --> DynKeys["Attach Run Keys<br/>with Optional Tick Suffix"]
```

**Diagram sources**
- [schedules/dbt/factory.py](file://src/dbt_dagsterizer/schedules/dbt/factory.py)
- [schedules/dbt/factory.py:51-100](file://src/dbt_dagsterizer/schedules/dbt/factory.py#L51-L100)

**Section sources**
- [schedules/dbt/factory.py](file://src/dbt_dagsterizer/schedules/dbt/factory.py)
- [schedules/dbt/factory.py:51-100](file://src/dbt_dagsterizer/schedules/dbt/factory.py#L51-L100)

### Dynamic Partition Scheduling Support
- Dynamic partitions enable scheduling across arbitrary partition dimensions (e.g., country codes, tenant IDs) rather than being limited to time-based daily partitions.
- The `_build_dynamic_partitioned_schedule` function emits RunRequests for all dynamic partition keys on each scheduled tick.
- Partition key iteration handles optional tick suffixes for idempotency across schedule executions.
- Dynamic partitions are managed through a registry that loads definitions from orchestration configuration and caches them for reuse.

**New Section** Comprehensive documentation for the new dynamic partition scheduling capabilities.

```mermaid
flowchart TD
DynConfig["Dynamic Partition Config"] --> DynReg["Dynamic Partitions Registry"]
DynReg --> DynDefs["Cached Dynamic Partitions"]
DynDefs --> DynFactory["_build_dynamic_partitioned_schedule"]
DynFactory --> TickLoop["For Each Partition Key"]
TickLoop --> RunReq["Create RunRequest"]
RunReq --> RunKey["Generate Run Key<br/>(with optional tick suffix)"]
RunKey --> Emit["Emit RunRequest"]
DynBootstrap["Dynamic Partitions Bootstrap"] --> DynReg
DynBootstrap --> DynKeys["Initialize Partition Keys"]
DynKeys --> DynReg
```

**Diagram sources**
- [schedules/dbt/factory.py:51-100](file://src/dbt_dagsterizer/schedules/dbt/factory.py#L51-L100)
- [partitions_registry.py](file://src/dbt_dagsterizer/partitions_registry.py)
- [partitions_dynamic.py](file://src/dbt_dagsterizer/partitions_dynamic.py)
- [sensors/dynamic_partitions_bootstrap.py](file://src/dbt_dagsterizer/sensors/dynamic_partitions_bootstrap.py)

**Section sources**
- [schedules/dbt/factory.py:51-100](file://src/dbt_dagsterizer/schedules/dbt/factory.py#L51-L100)
- [partitions_dynamic.py](file://src/dbt_dagsterizer/partitions_dynamic.py)
- [partitions_registry.py](file://src/dbt_dagsterizer/partitions_registry.py)
- [sensors/dynamic_partitions_bootstrap.py](file://src/dbt_dagsterizer/sensors/dynamic_partitions_bootstrap.py)
- [test_dynamic_partitions.py](file://src/dbt_dagsterizer/tests/test_dynamic_partitions.py)

### Integration with External Schedulers and Cloud-Native Platforms
- The system relies on Dagster's built-in scheduler for cron-based scheduling. There is no explicit integration code for external schedulers or cloud-native platforms in the analyzed files.
- To integrate with external schedulers, export the schedule definitions and mirror them externally, ensuring equivalent cron expressions and partition semantics. Alternatively, mirror the orchestration configuration to external systems and reconcile differences periodically.

[No sources needed since this section provides general guidance]

### Event-Driven Triggers: Partition-Change Detection
- Partition-change sensors compute watermarks over a sliding window and compare against previous cursors. They emit RunRequests for partitions whose watermarks have advanced within the configured window.
- The detector supports sparse lookback metadata and impact-range expansion to propagate changes to downstream partitions.

```mermaid
sequenceDiagram
participant Sensor as "Partition-Change Sensor"
participant Store as "Watermark Store"
participant Curs as "Cursor Parser"
participant Job as "Target Job"
Sensor->>Curs : "Parse Previous Cursor"
Sensor->>Store : "Query Max Watermarks in Window"
Store-->>Sensor : "Watermarks per Partition"
Sensor->>Sensor : "Compare with Previous"
Sensor->>Job : "Emit RunRequest per Changed Partition"
Sensor->>Curs : "Update Cursor with New Watermarks"
```

**Diagram sources**
- [sensors/partition_change/detector/factory.py](file://src/dbt_dagsterizer/sensors/partition_change/detector/factory.py)

**Section sources**
- [sensors/partition_change/detector/factory.py](file://src/dbt_dagsterizer/sensors/partition_change/detector/factory.py)

### Event-Driven Triggers: Propagation
- Propagation sensors watch upstream materialization events and emit RunRequests for the latest partition per partition key. They support catch-up behavior controlled by an environment variable and maintain a cursor for progress tracking.

```mermaid
sequenceDiagram
participant Up as "Upstream Asset"
participant Prop as "Propagation Sensor"
participant Inst as "Dagster Instance"
participant Job as "Downstream Job"
Up-->>Inst : "Materialization Events"
Prop->>Inst : "Query Latest Materializations"
Inst-->>Prop : "Latest per Partition"
Prop->>Job : "Emit RunRequest per Partition"
Prop->>Prop : "Update Cursor"
```

**Diagram sources**
- [sensors/partition_change/propagator/factory.py](file://src/dbt_dagsterizer/sensors/partition_change/propagator/factory.py)

**Section sources**
- [sensors/partition_change/propagator/factory.py](file://src/dbt_dagsterizer/sensors/partition_change/propagator/factory.py)

### Hybrid Scheduling Approaches
- Combine cron schedules for baseline coverage with event-driven sensors for near-real-time updates. Use partition-change detectors for data-driven triggers and propagation sensors for downstream cascading runs.
- Maintain separate orchestration entries for cron-triggered jobs and event-triggered sensors to avoid duplication and ensure predictable partition boundaries.

[No sources needed since this section provides general guidance]

### Failover Mechanisms and High Availability
- Run multiple Dagster instance workers behind a shared storage backend to achieve high availability for the scheduler and sensors.
- Use idempotent run keys and partition-aware execution to tolerate retries and overlapping ticks gracefully.

[No sources needed since this section provides general guidance]

### Monitoring, Alerting, and Notifications
- Leverage Dagster's logging and event streams to monitor schedule ticks, RunRequest emissions, and job outcomes.
- Tag runs with detector or propagation metadata to enable targeted alerts and dashboards.
- Integrate with external observability stacks to track SLAs, latency, and failure rates.

[No sources needed since this section provides general guidance]

### Schedule Validation, Drift Detection, and Maintenance
- Validate orchestration configuration on load and at generation time to prevent misconfigurations (e.g., unknown models, unsupported partition types).
- Detect drift by comparing current dbt models against configured schedules and jobs; reconcile discrepancies via maintenance scripts or CLI commands.
- Maintain a process to review and update partition-change detectors and propagators when upstream schemas evolve.

**Section sources**
- [orchestration_config.py](file://src/dbt_dagsterizer/orchestration_config.py)
- [schedules/dbt/auto_config.py](file://src/dbt_dagsterizer/schedules/dbt/auto_config.py)
- [jobs/dbt/auto_config.py](file://src/dbt_dagsterizer/jobs/dbt/auto_config.py)

## Dependency Analysis
The scheduling subsystem depends on orchestration metadata and dbt manifests to construct schedules and jobs. Sensors depend on the job registry and resource backends to evaluate conditions and emit RunRequests.

**Updated** Enhanced dependency graph to include dynamic partition components and their relationships with the scheduling system.

```mermaid
graph LR
Orch["orchestration_config.py"] --> SAuto["schedules/dbt/auto_config.py"]
Orch --> JAuto["jobs/dbt/auto_config.py"]
SAuto --> SFac["schedules/dbt/factory.py"]
JAuto --> JFac["jobs/dbt/factory.py"]
SFac --> SInit["schedules/__init__.py"]
JFac --> SInit
SInit --> Agg["get_schedules()"]
ObsSrc["schedules/sources/schedules.py"] --> Agg
PCDet["sensors/partition_change/detector/factory.py"] --> Agg
PCProp["sensors/partition_change/propagator/factory.py"] --> Agg
DynReg["partitions_registry.py"] --> SFac
DynReg --> JFac
DynCache["partitions_dynamic.py"] --> DynReg
DynAssets["assets/dbt/assets.py"] --> DynReg
DynBoot["sensors/dynamic_partitions_bootstrap.py"] --> DynReg
PartSpec["partitions.py"] --> SFac
```

**Diagram sources**
- [orchestration_config.py](file://src/dbt_dagsterizer/orchestration_config.py)
- [schedules/dbt/auto_config.py](file://src/dbt_dagsterizer/schedules/dbt/auto_config.py)
- [schedules/dbt/factory.py](file://src/dbt_dagsterizer/schedules/dbt/factory.py)
- [jobs/dbt/auto_config.py](file://src/dbt_dagsterizer/jobs/dbt/auto_config.py)
- [jobs/dbt/factory.py](file://src/dbt_dagsterizer/jobs/dbt/factory.py)
- [schedules/__init__.py](file://src/dbt_dagsterizer/schedules/__init__.py)
- [schedules/sources/schedules.py](file://src/dbt_dagsterizer/schedules/sources/schedules.py)
- [sensors/partition_change/detector/factory.py](file://src/dbt_dagsterizer/sensors/partition_change/detector/factory.py)
- [sensors/partition_change/propagator/factory.py](file://src/dbt_dagsterizer/sensors/partition_change/propagator/factory.py)
- [partitions_registry.py](file://src/dbt_dagsterizer/partitions_registry.py)
- [partitions_dynamic.py](file://src/dbt_dagsterizer/partitions_dynamic.py)
- [assets/dbt/assets.py](file://src/dbt_dagsterizer/assets/dbt/assets.py)
- [sensors/dynamic_partitions_bootstrap.py](file://src/dbt_dagsterizer/sensors/dynamic_partitions_bootstrap.py)
- [partitions.py](file://src/dbt_dagsterizer/partitions.py)

**Section sources**
- [schedules/__init__.py](file://src/dbt_dagsterizer/schedules/__init__.py)
- [schedules/dbt/factory.py](file://src/dbt_dagsterizer/schedules/dbt/factory.py)
- [jobs/dbt/factory.py](file://src/dbt_dagsterizer/jobs/dbt/factory.py)

## Performance Considerations
- Limit lookback windows to reduce the number of emitted RunRequests per tick.
- Use partition-aware jobs to constrain work per run and improve throughput.
- Tune minimum interval for sensors to balance responsiveness and overhead.
- Prefer asset-based jobs for efficient downstream propagation; use CLI jobs sparingly for specialized tasks.
- For dynamic partitions, consider the cardinality of partition keys when designing schedule frequency to avoid overwhelming the system with too many concurrent runs.
- Monitor dynamic partition key growth and implement key rotation strategies for large-scale deployments.

[No sources needed since this section provides general guidance]

## Troubleshooting Guide
- Duplicate names: Both schedules and sensors enforce uniqueness and raise errors on duplicates.
- Unsupported partition types: Schedules currently support daily partitioning; specifying other types raises errors.
- Unknown models or jobs: References to missing dbt models or undefined jobs cause validation failures during auto-config.
- Missing relations in detectors: Sensors skip or warn when required relations are absent and update cursors accordingly.
- Propagation cursor resets: Sensors reset invalid cursors and initialize from latest materialization when appropriate.
- Dynamic partition configuration errors: Empty partition names or missing initial keys in orchestration configuration raise validation errors.
- Unknown dynamic partitions: Referencing dynamic partitions not defined in configuration causes errors during schedule building.
- Dynamic partition key synchronization: Ensure dynamic partition keys are properly bootstrapped and synchronized between the registry and instance.

**Section sources**
- [schedules/dbt/factory.py](file://src/dbt_dagsterizer/schedules/dbt/factory.py)
- [sensors/partition_change/detector/factory.py](file://src/dbt_dagsterizer/sensors/partition_change/detector/factory.py)
- [sensors/partition_change/propagator/factory.py](file://src/dbt_dagsterizer/sensors/partition_change/propagator/factory.py)
- [jobs/dbt/auto_config.py](file://src/dbt_dagsterizer/jobs/dbt/auto_config.py)
- [orchestration_config.py](file://src/dbt_dagsterizer/orchestration_config.py)
- [partitions_dynamic.py](file://src/dbt_dagsterizer/partitions_dynamic.py)
- [sensors/dynamic_partitions_bootstrap.py](file://src/dbt_dagsterizer/sensors/dynamic_partitions_bootstrap.py)

## Conclusion
The scheduling system integrates cron-based and event-driven mechanisms around a central orchestration configuration. By combining schedules for steady-state coverage with sensors for responsive reactions, teams can achieve robust, partition-aware execution. The addition of dynamic partition support extends scheduling capabilities beyond time-based daily partitions to handle arbitrary partition dimensions like country codes and tenant IDs through the new `_build_dynamic_partitioned_schedule` function. The comprehensive dynamic partition management infrastructure ensures proper key lifecycle management, registry synchronization, and bootstrap processes. Operational procedures around validation, drift detection, and maintenance keep the system reliable and aligned with evolving data and model structures.

## Appendices

### Appendix A: CLI and Project Templates
- Project initialization and GitOps environment generation are supported via CLI groups, enabling reproducible environments and consistent scheduling setups.

**Section sources**
- [cli_parts/project.py](file://src/dbt_dagsterizer/cli_parts/project.py)
- [cli_parts/macros.py](file://src/dbt_dagsterizer/cli_parts/macros.py)

### Appendix B: Dynamic Partition Configuration Examples
Dynamic partitions are configured in the orchestration YAML under the `partitions.dynamic` section. Each dynamic partition requires a unique name and an initial list of partition keys.

**Section sources**
- [orchestration_config.py](file://src/dbt_dagsterizer/orchestration_config.py)
- [test_dynamic_partitions.py](file://src/dbt_dagsterizer/tests/test_dynamic_partitions.py)

### Appendix C: Dynamic Partition Scheduling Patterns
- **Country-based scheduling**: Configure country_code dynamic partition with ISO country codes and schedule per-country runs
- **Tenant-based scheduling**: Set up tenant_id dynamic partition for multi-tenant deployments with per-tenant isolation
- **Region-based scheduling**: Define region dynamic partition for geographic data processing
- **Hybrid patterns**: Combine daily and dynamic partitions for complex scheduling scenarios

**Section sources**
- [partitions_dynamic.py](file://src/dbt_dagsterizer/partitions_dynamic.py)
- [partitions_registry.py](file://src/dbt_dagsterizer/partitions_registry.py)
- [sensors/dynamic_partitions_bootstrap.py](file://src/dbt_dagsterizer/sensors/dynamic_partitions_bootstrap.py)
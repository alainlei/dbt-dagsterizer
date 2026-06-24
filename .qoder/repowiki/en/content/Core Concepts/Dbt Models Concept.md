# Dbt Models Concept

<cite>
**Referenced Files in This Document**
- [dbt-models.md](file://docs/concepts/dbt-models.md)
- [assets.py](file://src/dbt_dagsterizer/assets/dbt/assets.py)
- [translator.py](file://src/dbt_dagsterizer/assets/dbt/translator.py)
- [vars.py](file://src/dbt_dagsterizer/assets/dbt/vars.py)
- [manifest.py](file://src/dbt_dagsterizer/dbt/manifest.py)
- [manifest_prepare.py](file://src/dbt_dagsterizer/dbt/manifest_prepare.py)
- [prepare.py](file://src/dbt_dagsterizer/assets/dbt/prepare.py)
- [run_results.py](file://src/dbt_dagsterizer/dbt/run_results.py)
- [orchestration_config.py](file://src/dbt_dagsterizer/orchestration_config.py)
- [orders.sql](file://src/dbt_dagsterizer/project_templates/luban-dagster-dbt-starrocks-code-location-source-template/{{cookiecutter.output_name}}/dbt_project/models/dwd/orders.sql)
- [fact_orders_daily.sql](file://src/dbt_dagsterizer/project_templates/luban-dagster-dbt-starrocks-code-location-source-template/{{cookiecutter.output_name}}/dbt_project/models/dws/fact_orders_daily.sql)
- [dim_customer.sql](file://src/dbt_dagsterizer/project_templates/luban-dagster-dbt-starrocks-code-location-source-template/{{cookiecutter.output_name}}/dbt_project/models/dws/dim_customer.sql)
- [partition_vars.sql](file://src/dbt_dagsterizer/project_templates/luban-dagster-dbt-starrocks-code-location-source-template/{{cookiecutter.output_name}}/dbt_project/macros/dbt_dagsterizer/partition_vars.sql)
</cite>

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

## Introduction
This document explains the concept and implementation of dbt models within a dbt-dagsterizer project targeting StarRocks. It covers the layered architecture (DWD, DWS), partition execution patterns, incremental logic, and how dbt models integrate with Dagster for orchestrated, partition-aware execution. The guide references concrete model files and supporting infrastructure to help both newcomers and experienced users adopt consistent patterns for reliable, scalable data transformations.

## Project Structure
The dbt models are organized under a layered structure that aligns with StarRocks table types and partitioning strategies:
- DWD (Data Warehouse Detail): Clean, standardized detail-level data with datetime partitioning and Primary Key tables for upserts.
- DWS (Data Warehouse Summary): Aggregated fact tables and slowly changing dimensions with appropriate partitioning and incremental strategies.
- Macros: Custom dbt macros for partition window handling and StarRocks schema routing.

```mermaid
graph TB
subgraph "dbt Project"
A["models/dwd/"] --> A1["orders.sql"]
B["models/dws/"] --> B1["fact_orders_daily.sql"]
B --> B2["dim_customer.sql"]
C["macros/dbt_dagsterizer/"] --> C1["partition_vars.sql"]
end
```

**Diagram sources**
- [orders.sql:1-22](file://src/dbt_dagsterizer/project_templates/luban-dagster-dbt-starrocks-code-location-source-template/{{cookiecutter.output_name}}/dbt_project/models/dwd/orders.sql#L1-L22)
- [fact_orders_daily.sql:1-19](file://src/dbt_dagsterizer/project_templates/luban-dagster-dbt-starrocks-code-location-source-template/{{cookiecutter.output_name}}/dbt_project/models/dws/fact_orders_daily.sql#L1-L19)
- [dim_customer.sql:1-21](file://src/dbt_dagsterizer/project_templates/luban-dagster-dbt-starrocks-code-location-source-template/{{cookiecutter.output_name}}/dbt_project/models/dws/dim_customer.sql#L1-L21)
- [partition_vars.sql:1-19](file://src/dbt_dagsterizer/project_templates/luban-dagster-dbt-starrocks-code-location-source-template/{{cookiecutter.output_name}}/dbt_project/macros/dbt_dagsterizer/partition_vars.sql#L1-L19)

**Section sources**
- [dbt-models.md:9-31](file://docs/concepts/dbt-models.md#L9-L31)

## Core Components
- Layered Architecture: DWD for detail-level, time-partitioned, Primary Key tables; DWS for facts and dimensions with appropriate partitioning and incremental strategies.
- Partition Execution: Dagster passes partition windows (date/datetime) and dynamic partition keys to dbt via variables, enforced by custom macros.
- Incremental Logic: Partition window filtering for time-partitioned models and watermark-based incremental for dimensions.
- Automation and Observability: Dagster automation conditions, partition propagation, and run result telemetry.

**Section sources**
- [dbt-models.md:33-303](file://docs/concepts/dbt-models.md#L33-L303)

## Architecture Overview
The end-to-end flow connects Dagster orchestration with dbt model execution and StarRocks materialization:

```mermaid
sequenceDiagram
participant DS as "Dagster Sensor/Schedule"
participant AS as "Asset Definition<br/>get_dbt_assets()"
participant TR as "Translator<br/>LubanDagsterDbtTranslator"
participant DBT as "DbtCliResource"
participant MR as "Manifest/Run Results"
DS->>AS : "Partitioned run request"
AS->>TR : "Resolve partitions and automation"
AS->>DBT : "dbt build --vars {partition/window}"
DBT-->>AS : "Stream events and artifacts"
AS->>MR : "Parse run_results.json and manifest.json"
AS-->>DS : "AssetObservation with row counts"
```

**Diagram sources**
- [assets.py:150-242](file://src/dbt_dagsterizer/assets/dbt/assets.py#L150-L242)
- [translator.py:44-140](file://src/dbt_dagsterizer/assets/dbt/translator.py#L44-L140)
- [run_results.py:75-144](file://src/dbt_dagsterizer/dbt/run_results.py#L75-L144)

## Detailed Component Analysis

### DWD Detail Models (Primary Key, Incremental)
DWD models clean and standardize source data into StarRocks Primary Key tables with datetime partitioning and incremental processing.

```mermaid
flowchart TD
Start(["Model Entry"]) --> Config["Configure materialized='incremental'<br/>table_type='PRIMARY'<br/>keys=[...]"]
Config --> SourceRef["Select from source(...)"]
SourceRef --> PartitionWindow["Call luban_partition_window_datetime()"]
PartitionWindow --> WhereClause["Filter by min/max datetime"]
WhereClause --> Upsert["StarRocks upsert via keys"]
Upsert --> End(["Success"])
```

**Diagram sources**
- [orders.sql:1-22](file://src/dbt_dagsterizer/project_templates/luban-dagster-dbt-starrocks-code-location-source-template/{{cookiecutter.output_name}}/dbt_project/models/dwd/orders.sql#L1-L22)
- [partition_vars.sql:11-18](file://src/dbt_dagsterizer/project_templates/luban-dagster-dbt-starrocks-code-location-source-template/{{cookiecutter.output_name}}/dbt_project/macros/dbt_dagsterizer/partition_vars.sql#L11-L18)

**Section sources**
- [dbt-models.md:35-78](file://docs/concepts/dbt-models.md#L35-L78)
- [orders.sql:1-22](file://src/dbt_dagsterizer/project_templates/luban-dagster-dbt-starrocks-code-location-source-template/{{cookiecutter.output_name}}/dbt_project/models/dwd/orders.sql#L1-L22)

### DWS Fact Models (Daily Partitioned)
DWS fact models aggregate DWD outputs into daily partitions using unique_key for incremental upserts.

```mermaid
flowchart TD
StartF(["Fact Model Entry"]) --> RefUpstream["Select from ref('orders')"]
RefUpstream --> DailyWindow["Call luban_partition_window_date()"]
DailyWindow --> GroupBy["Aggregate by partition key"]
GroupBy --> UpsertF["Incremental upsert via unique_key"]
UpsertF --> EndF(["Success"])
```

**Diagram sources**
- [fact_orders_daily.sql:1-19](file://src/dbt_dagsterizer/project_templates/luban-dagster-dbt-starrocks-code-location-source-template/{{cookiecutter.output_name}}/dbt_project/models/dws/fact_orders_daily.sql#L1-L19)
- [partition_vars.sql:1-8](file://src/dbt_dagsterizer/project_templates/luban-dagster-dbt-starrocks-code-location-source-template/{{cookiecutter.output_name}}/dbt_project/macros/dbt_dagsterizer/partition_vars.sql#L1-L8)

**Section sources**
- [dbt-models.md:79-116](file://docs/concepts/dbt-models.md#L79-L116)
- [fact_orders_daily.sql:1-19](file://src/dbt_dagsterizer/project_templates/luban-dagster-dbt-starrocks-code-location-source-template/{{cookiecutter.output_name}}/dbt_project/models/dws/fact_orders_daily.sql#L1-L19)

### DWS Dimension Models (Event-Driven Watermarks)
DWS dimension models refresh when upstream data changes using watermark comparisons.

```mermaid
flowchart TD
StartD(["Dimension Model Entry"]) --> RefUpstreamD["Select from ref('customers')"]
RefUpstreamD --> IsIncremental{"is_incremental() ?"}
IsIncremental --> |Yes| Watermark["Filter where updated_at > max(existing.updated_at)"]
IsIncremental --> |No| FullScan["Skip incremental filter"]
Watermark --> EndD(["Success"])
FullScan --> EndD
```

**Diagram sources**
- [dim_customer.sql:1-21](file://src/dbt_dagsterizer/project_templates/luban-dagster-dbt-starrocks-code-location-source-template/{{cookiecutter.output_name}}/dbt_project/models/dws/dim_customer.sql#L1-L21)

**Section sources**
- [dbt-models.md:118-151](file://docs/concepts/dbt-models.md#L118-L151)
- [dim_customer.sql:1-21](file://src/dbt_dagsterizer/project_templates/luban-dagster-dbt-starrocks-code-location-source-template/{{cookiecutter.output_name}}/dbt_project/models/dws/dim_customer.sql#L1-L21)

### Dynamic Partition Models (Business Keys)
Dynamic partitions split work by business keys (e.g., country_code, tenant_id) while optionally combining with time windows.

```mermaid
flowchart TD
StartP(["Dynamic Partition Entry"]) --> VarCheck{"partition_key available?"}
VarCheck --> |Yes| FilterByKey["Filter by partition_key = var('partition_key')"]
VarCheck --> |No| DefaultVars["Fallback to default daily window"]
FilterByKey --> CombineTime["Optionally combine with luban_partition_window_*()"]
CombineTime --> Execute["Execute model for the partition key"]
DefaultVars --> Execute
Execute --> EndP(["Success"])
```

**Diagram sources**
- [dbt-models.md:152-244](file://docs/concepts/dbt-models.md#L152-L244)

**Section sources**
- [dbt-models.md:152-244](file://docs/concepts/dbt-models.md#L152-L244)

### Partition Window Macros and Variable Injection
Dagster injects partition variables into dbt runs; macros validate and return partition windows.

```mermaid
sequenceDiagram
participant DG as "Dagster Context"
participant VARS as "_get_dbt_vars_for_context()"
participant MAC as "luban_partition_window_*()"
participant SQL as "Model SQL"
DG->>VARS : "partition_key or time_window"
VARS-->>MAC : "min/max date/datetime"
MAC-->>SQL : "Dictionary with window bounds"
SQL-->>DG : "Filtered execution per partition"
```

**Diagram sources**
- [vars.py:25-61](file://src/dbt_dagsterizer/assets/dbt/vars.py#L25-L61)
- [partition_vars.sql:1-19](file://src/dbt_dagsterizer/project_templates/luban-dagster-dbt-starrocks-code-location-source-template/{{cookiecutter.output_name}}/dbt_project/macros/dbt_dagsterizer/partition_vars.sql#L1-L19)

**Section sources**
- [dbt-models.md:305-375](file://docs/concepts/dbt-models.md#L305-L375)
- [vars.py:25-61](file://src/dbt_dagsterizer/assets/dbt/vars.py#L25-L61)

### Asset Definition and Row Count Telemetry
The dbt asset definition orchestrates dbt CLI execution, captures run results, and emits row count observations with partition context.

```mermaid
sequenceDiagram
participant AD as "get_dbt_assets()"
participant DBT as "DbtCliResource"
participant RR as "parse_run_results()"
participant OBS as "Emit AssetObservation"
AD->>DBT : "dbt build --vars ..."
DBT-->>AD : "Stream events"
AD->>RR : "Load run_results.json"
RR-->>AD : "Affected row counts"
AD->>OBS : "Emit observation with partition_key"
```

**Diagram sources**
- [assets.py:150-242](file://src/dbt_dagsterizer/assets/dbt/assets.py#L150-L242)
- [run_results.py:75-144](file://src/dbt_dagsterizer/dbt/run_results.py#L75-L144)

**Section sources**
- [assets.py:150-242](file://src/dbt_dagsterizer/assets/dbt/assets.py#L150-L242)
- [run_results.py:214-227](file://src/dbt_dagsterizer/dbt/run_results.py#L214-L227)

### Translator and Automation Conditions
The translator maps dbt resources to Dagster assets, applies partition definitions, and sets automation conditions based on tags and layers.

```mermaid
classDiagram
class LubanDagsterDbtTranslator {
+daily_partitions_def
+dynamic_partitions_defs
+automation_observable_tables
+partitions_by_model
+get_automation_condition(dbt_resource_props)
+get_asset_key(dbt_resource_props)
+get_group_name(dbt_resource_props)
+get_partitions_def(dbt_resource_props)
}
```

**Diagram sources**
- [translator.py:44-140](file://src/dbt_dagsterizer/assets/dbt/translator.py#L44-L140)

**Section sources**
- [translator.py:44-140](file://src/dbt_dagsterizer/assets/dbt/translator.py#L44-L140)

### Manifest Loading and Model Indexing
Manifest preparation ensures dbt artifacts are present and indexed for automation and telemetry.

```mermaid
flowchart TD
StartM(["Manifest Load"]) --> Prepare["ensure_manifest()"]
Prepare --> Parse["run_dbt parse"]
Parse --> WriteInputs["write_manifest_inputs()"]
WriteInputs --> Load["json.load('manifest.json')"]
Load --> Index["iter_models() -> DbtModel list"]
Index --> EndM(["Ready for automation"])
```

**Diagram sources**
- [manifest_prepare.py:57-72](file://src/dbt_dagsterizer/dbt/manifest_prepare.py#L57-L72)
- [manifest.py:28-64](file://src/dbt_dagsterizer/dbt/manifest.py#L28-L64)

**Section sources**
- [manifest_prepare.py:57-72](file://src/dbt_dagsterizer/dbt/manifest_prepare.py#L57-L72)
- [manifest.py:28-64](file://src/dbt_dagsterizer/dbt/manifest.py#L28-L64)

## Dependency Analysis
The dbt models concept integrates with Dagster through a clear dependency chain: orchestration configuration defines partitions and automation; the translator resolves assets and partitions; the asset definition executes dbt builds with injected variables; run results feed telemetry and row count observations.

```mermaid
graph TB
OC["orchestration_config.py<br/>Partitions & Jobs"] --> TR["translator.py<br/>Asset/Partition Mapping"]
TR --> AD["assets.py<br/>get_dbt_assets()"]
AD --> DBT["DbtCliResource<br/>dbt build"]
DBT --> RR["run_results.py<br/>Telemetry & Row Counts"]
AD --> PR["prepare.py<br/>Manifest Prep"]
PR --> MP["manifest_prepare.py<br/>ensure_manifest()"]
MP --> M["manifest.py<br/>iter_models()"]
```

**Diagram sources**
- [orchestration_config.py:120-191](file://src/dbt_dagsterizer/orchestration_config.py#L120-L191)
- [translator.py:44-140](file://src/dbt_dagsterizer/assets/dbt/translator.py#L44-L140)
- [assets.py:150-242](file://src/dbt_dagsterizer/assets/dbt/assets.py#L150-L242)
- [prepare.py:9-18](file://src/dbt_dagsterizer/assets/dbt/prepare.py#L9-L18)
- [manifest_prepare.py:57-72](file://src/dbt_dagsterizer/dbt/manifest_prepare.py#L57-L72)
- [manifest.py:40-64](file://src/dbt_dagsterizer/dbt/manifest.py#L40-L64)
- [run_results.py:258-370](file://src/dbt_dagsterizer/dbt/run_results.py#L258-L370)

**Section sources**
- [orchestration_config.py:120-191](file://src/dbt_dagsterizer/orchestration_config.py#L120-L191)
- [assets.py:150-242](file://src/dbt_dagsterizer/assets/dbt/assets.py#L150-L242)

## Performance Considerations
- Prefer incremental materialization for large tables to avoid full rebuilds.
- Use Primary Key tables in StarRocks for efficient upserts and partition pruning.
- Apply partition window filtering consistently to limit scanned data per run.
- Tag dimensions for eager automation to reduce latency in downstream recomputation.
- Leverage run result telemetry to monitor long-running nodes and optimize execution.

[No sources needed since this section provides general guidance]

## Troubleshooting Guide
Common issues and resolutions:
- Missing partition variables: Ensure runs are executed via Dagster or pass partition variables explicitly.
- Dynamic partition key undefined: Provide the partition_key variable or run via Dagster’s partitioned execution.
- Incremental not working: Verify materialized='incremental', unique_key or keys, and is_incremental() checks.
- Model processes all data: Confirm luban_partition_window_*() macro usage and where clause application.

**Section sources**
- [dbt-models.md:658-724](file://docs/concepts/dbt-models.md#L658-L724)
- [vars.py:25-61](file://src/dbt_dagsterizer/assets/dbt/vars.py#L25-L61)

## Conclusion
Dbt models in this project follow a disciplined layered architecture optimized for StarRocks and Dagster orchestration. By adopting partition window macros, incremental strategies, and automation conditions, teams can achieve reliable, observable, and scalable data transformations. The provided patterns and integrations enable consistent execution across time-partitioned and dynamic partition scenarios while maintaining strong observability through run result telemetry.
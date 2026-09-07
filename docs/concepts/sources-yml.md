# sources.yml — `meta.luban` Namespace Reference

This document describes every `meta.luban` property that dbt-dagsterizer reads from `sources.yml`. All custom properties live under `meta.luban` because dbt validates `sources.yml` against a strict schema and rejects unknown top-level keys.

## Overview

dbt `sources.yml` declares external tables that your project reads from. dbt-dagsterizer extends this with a `meta.luban` namespace to control:

- **Observation** — watermark queries that detect new data in source tables
- **Grouping** — Dagster UI group assignment for source assets
- **Cross-code-location ownership** — marking sources owned by another code location

```
meta:
  luban:
    group: <string>                    # Dagster group name
    external_code_location: <bool>     # owned by another code location
    observe:
      watermark_column: <string>       # simple max() watermark
      watermark_sql: <string>          # custom SQL watermark query
```

## Property Reference

### `meta.luban.observe.watermark_column`

| Attribute | Value |
|-----------|-------|
| **Type** | `string` |
| **Default** | `None` |
| **Required** | Yes, unless `watermark_sql` is set |
| **Cascade** | Table-level `meta` wins over source-level `meta` |

Column name to query with `max()` to determine the data version of a source table. dbt-dagsterizer generates the following SQL automatically:

```sql
select max(`<watermark_column>`) from `<database>`.`<table>`
```

The database name is resolved from the `STARROCKS_<SOURCE>_DB` environment variable (e.g., `STARROCKS_ODS_DB`), falling back to the source name.

**Example** — table-level:

```yaml
sources:
  - name: ods
    tables:
      - name: customers
        meta:
          luban:
            observe:
              watermark_column: ods_updated_at
```

**Example** — source-level (applies to every table):

```yaml
sources:
  - name: ods
    meta:
      luban:
        observe:
          watermark_column: ods_updated_at
    tables:
      - name: customers
      - name: orders
```

---

### `meta.luban.observe.watermark_sql`

| Attribute | Value |
|-----------|-------|
| **Type** | `string` (SQL query) |
| **Default** | `None` |
| **Required** | Yes, unless `watermark_column` is set |
| **Cascade** | Table-level `meta` wins over source-level `meta` |
| **Precedence** | Wins over `watermark_column` when both are set |

Custom SQL query that returns a single scalar value used as the source asset `DataVersion`. Use this when `max()` over a single column is not sufficient — for example, coalesced columns, filtered queries, or derived values.

The query runs as written, so include any needed database/schema qualification directly in the SQL.

**Example**:

```yaml
sources:
  - name: ods
    tables:
      - name: customers
        meta:
          luban:
            observe:
              watermark_sql: |
                select max(coalesce(updated_at, created_at))
                from ods.customers
                where is_deleted = 0
```

**Rules**:

- The query must return a single scalar value.
- The value is converted to a string and used as the Dagster `DataVersion`.
- If both `watermark_column` and `watermark_sql` are set, `watermark_sql` takes precedence.

---

### `meta.luban.group`

| Attribute | Value |
|-----------|-------|
| **Type** | `string` |
| **Default** | `"source"` |
| **Cascade** | Table-level `meta` wins over source-level `meta` |
| **Legacy fallback** | `meta.luban.observe.group` is accepted when `meta.luban.group` is not set |

Custom Dagster group name for observable source assets. Controls how source assets are organized in the Dagster UI.

**Example** — per-table group:

```yaml
sources:
  - name: ods
    tables:
      - name: customers
        meta:
          luban:
            group: ods_customers
            observe:
              watermark_column: ods_updated_at
```

**Example** — source-level group (covers all tables):

```yaml
sources:
  - name: ods
    meta:
      luban:
        group: ods_sources
    tables:
      - name: customers
        meta:
          luban:
            observe:
              watermark_column: ods_updated_at
      - name: orders
        meta:
          luban:
            observe:
              watermark_column: ods_updated_at
```

In this example, both `customers` and `orders` appear under the `ods_sources` group in the Dagster UI.

**Resolution order**:

1. Table-level `meta.luban.group`
2. Source-level `meta.luban.group` (via `source_meta`)
3. Table-level `meta.luban.observe.group` (legacy)
4. Source-level `meta.luban.observe.group` (legacy)
5. Default: `"source"`

---

### `meta.luban.external_code_location`

| Attribute | Value |
|-----------|-------|
| **Type** | `boolean` |
| **Default** | `false` |
| **Cascade** | Table-level `meta` wins over source-level `meta` |

Marks a source as belonging to an **external** (other) code location. When two code locations reference the same physical table — one as a dbt model (producer) and one as a dbt source (consumer) — Dagster emits a "Multiple asset definitions found" warning. Setting `external_code_location: true` on the consuming side tells dbt-dagsterizer to skip generating asset definitions for that source.

**Effect**:

1. The source is excluded from the `@dbt_assets` decorator (`exclude="source:<name>"`).
2. No `@observable_source_asset` is created for the source.
3. The observation job and schedule skip the source.

**Example** — source-level (all tables in the source are external):

```yaml
sources:
  - name: ext_db
    meta:
      luban:
        external_code_location: true
    tables:
      - name: mkt_ext
      - name: campaigns
```

**Example** — table-level (only one table is external):

```yaml
sources:
  - name: shared_db
    tables:
      - name: ext_table
        meta:
          luban:
            external_code_location: true
      - name: local_table
        meta:
          luban:
            observe:
              watermark_column: updated_at
```

In the table-level example, only `ext_table` is marked external. `local_table` is owned by this code location and will generate a full observable source asset.

**When to use**:

- Code location A produces `ext_db.mkt_ext` as a dbt model.
- Code location B references `ext_db.mkt_ext` as a dbt source.
- Set `external_code_location: true` in code location B's `sources.yml` for that source.

---

## Cascade Rules

All `meta.luban` source properties follow the same cascade pattern:

| Priority | Level | Manifest field | dbt YAML location |
|----------|-------|----------------|-------------------|
| 1 (wins) | Table | `meta` | Under `tables[].meta` |
| 2 (fallback) | Source | `source_meta` | Under the source-level `meta` |

dbt propagates source-level `meta` into each table entry in the manifest as `source_meta`. Table-level `meta` is stored as `meta`. When both are present, **table-level wins**.

This means you can set a property once at the source level and override it for specific tables:

```yaml
sources:
  - name: ods
    meta:
      luban:
        group: ods_sources                          # applies to all tables
        observe:
          watermark_column: ods_updated_at          # applies to all tables
    tables:
      - name: customers                             # inherits group + watermark
      - name: orders                                # inherits group + watermark
      - name: special_table
        meta:
          luban:
            group: ods_special                      # overrides source-level group
            observe:
              watermark_sql: |                       # overrides source-level watermark
                select max(processed_at)
                from ods.special_table
```

---

## Complete Example

A full `sources.yml` combining all `meta.luban` properties:

```yaml
version: 2

sources:
  # ── Local sources (owned by this code location) ──────────────
  - name: ods
    database: "{{ env_var('STARROCKS_ODS_DB', 'ods') }}"
    schema: public
    meta:
      luban:
        group: ods_sources
        observe:
          watermark_column: ods_updated_at
    tables:
      - name: customers
        identifier: customers
      - name: orders
        identifier: orders
      - name: payments
        identifier: payments
        meta:
          luban:
            group: ods_payments
            observe:
              watermark_sql: |
                select max(coalesce(updated_at, created_at))
                from ods.payments
                where status != 'deleted'

  # ── External sources (owned by another code location) ────────
  - name: dwd
    database: "{{ env_var('STARROCKS_DWD_DB', 'dwd') }}"
    schema: public
    meta:
      luban:
        external_code_location: true
    tables:
      - name: dim_customer
        identifier: dim_customer
      - name: fact_orders
        identifier: fact_orders
```

In this example:

- `ods.customers`, `ods.orders`, and `ods.payments` are observable source assets owned by this code location.
- `ods.payments` overrides the group and watermark SQL at the table level.
- `dwd.dim_customer` and `dwd.fact_orders` are consumed as dbt sources but owned by another code location — no asset definitions or observation jobs are generated for them.

---

## Properties for dbt Models (not Sources)

The `meta.luban` namespace is also used in dbt model `schema.yml` files. These properties apply to models, not sources, and are listed here for completeness.

### `meta.luban.partition`

| Attribute | Value |
|-----------|-------|
| **Type** | `string` |
| **Values** | `"daily"`, `"hourly"`, `"monthly"`, `"unpartitioned"` |

Overrides the partition type for a model. Normally set via `dagsterization.yml`, but can be declared inline in the model's YAML.

### `meta.luban.asset_job`

| Attribute | Value |
|-----------|-------|
| **Type** | `boolean` |
| **Default** | `false` |

When `true`, the model is assigned to a dedicated asset job instead of the default group job.

---

## See Also

- [dagsterization.yml Reference](dagsterization-yml.md) — Orchestration configuration
- [dbt Model SQL Files Guide](dbt-models.md) — Model patterns and macros
- [Developer Workflow](../templates/dagster-dbt-starrocks-code-location/developer_workflow.md) — End-to-end workflow
- [dbt sources.yml documentation](https://docs.getdbt.com/docs/build/sources) — Official dbt docs

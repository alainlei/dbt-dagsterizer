from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ...resources.dbt import get_dbt_project_dir
from ..dbt.prepare import prepare_manifest_if_missing
from ..dbt.translator import relation_asset_key_path


def _manifest_path() -> Path:
    return get_dbt_project_dir() / "target" / "manifest.json"


def _extract_group_from_meta(meta_value: Any) -> str | None:
    """Extract a custom Dagster group from a dbt `meta` mapping.

    Reads `meta.luban.group`; falls back to the legacy
    `meta.luban.observe.group` placement for backward compatibility.
    """
    if not isinstance(meta_value, dict):
        return None
    luban_meta = meta_value.get("luban")
    if not isinstance(luban_meta, dict):
        return None
    group = luban_meta.get("group")
    if group is None:
        observe = luban_meta.get("observe")
        if isinstance(observe, dict):
            group = observe.get("group")
    return str(group) if group is not None else None


def load_automation_observable_sources() -> list[dict[str, str | None]]:
    prepare_manifest_if_missing()
    with _manifest_path().open("r", encoding="utf-8") as f:
        manifest = json.load(f)

    sources = manifest.get("sources") or {}
    specs: list[dict[str, str]] = []
    for props in sources.values():
        if not isinstance(props, dict):
            continue
        meta: dict[str, Any] = props.get("meta") or {}
        luban_meta: dict[str, Any] = meta.get("luban") or {}
        observe: dict[str, Any] = luban_meta.get("observe") or {}
        watermark_column = observe.get("watermark_column")
        watermark_sql = observe.get("watermark_sql")
        if not watermark_column and not watermark_sql:
            continue
        source_name = props.get("source_name")
        table_name = props.get("identifier") or props.get("name")
        dbt_name = props.get("name")
        if not source_name or not table_name:
            continue

        # Table-level `meta.luban.group` wins; fall back to the source-level
        # meta (exposed by dbt as `source_meta`) so one group can cover a
        # whole source definition.
        group = _extract_group_from_meta(props.get("meta"))
        if group is None:
            group = _extract_group_from_meta(props.get("source_meta"))

        specs.append(
            {
                "source": str(source_name),
                "table": str(table_name),
                "name": str(dbt_name) if dbt_name is not None else str(table_name),
                "watermark_column": str(watermark_column) if watermark_column is not None else None,
                "watermark_sql": str(watermark_sql) if watermark_sql is not None else None,
                "group": str(group) if group is not None else None,
            }
        )

    return sorted(specs, key=lambda s: (s["source"], s["table"]))


def _is_luban_external_code_location(meta_value: Any) -> bool:
    """Return True when the meta dict contains ``luban.external_code_location: true``."""
    if not isinstance(meta_value, dict):
        return False
    luban = meta_value.get("luban")
    if not isinstance(luban, dict):
        return False
    return bool(luban.get("external_code_location"))


def load_external_source_names() -> set[tuple[str, str]]:
    """Scan the dbt manifest for sources marked ``meta.luban.external_code_location: true``.

    Returns a set of ``(source_name, dbt_name)`` pairs, where *dbt_name* is the
    logical ``name`` field from sources.yml (i.e. the value you would use inside
    ``{{ source('…','…') }}``).  Both table-level ``meta`` and source-level
    ``source_meta`` are checked (table-level wins, source-level is the fallback
    — same cascade used by ``_extract_group_from_meta``).

    Tracking per-table pairs (instead of collapsing to the whole source group)
    means marking one table as external does **not** accidentally exclude its
    sibling tables that share the same ``source_name``.
    """
    prepare_manifest_if_missing()
    with _manifest_path().open("r", encoding="utf-8") as f:
        manifest = json.load(f)

    external_pairs: set[tuple[str, str]] = set()
    for props in (manifest.get("sources") or {}).values():
        if not isinstance(props, dict):
            continue
        # Table-level meta wins; fall back to source-level source_meta
        if _is_luban_external_code_location(props.get("meta")) or _is_luban_external_code_location(
            props.get("source_meta")
        ):
            source_name = props.get("source_name")
            dbt_name = props.get("name")
            if source_name and dbt_name:
                external_pairs.add((str(source_name), str(dbt_name)))
    return external_pairs


def load_filtered_observable_sources() -> list[dict[str, str | None]]:
    """Load observable source specs, excluding sources owned by other code locations.

    A spec is excluded when its ``(source, name)`` pair (the dbt ``source_name``
    / logical ``name`` tuple) appears in ``load_external_source_names()``.
    This prevents duplicate asset definitions when two code locations reference
    the same physical table (one as a model, the other as a source) without
    accidentally dropping sibling tables under the same source group.
    """
    all_specs = load_automation_observable_sources()
    external_pairs = load_external_source_names()
    if not external_pairs:
        return all_specs
    return [
        s
        for s in all_specs
        if (str(s.get("source") or ""), str(s.get("name") or s.get("table") or ""))
        not in external_pairs
    ]


def _materializing_node_key_paths(manifest: dict[str, Any]) -> set[tuple[str, ...]]:
    """Return relation-based key paths for dbt models/seeds/snapshots in the manifest.

    Used to skip lineage-only source specs whose key is already claimed by a real
    (materializable) dbt asset, which would otherwise produce duplicate asset
    definitions at load time.  The key for each node is built with the same
    ``identifier or name`` formula the translator uses for every dbt resource.
    """
    key_paths: set[tuple[str, ...]] = set()
    for props in (manifest.get("nodes") or {}).values():
        if not isinstance(props, dict):
            continue
        if props.get("resource_type") not in ("model", "seed", "snapshot"):
            continue
        if (props.get("config") or {}).get("materialized") == "ephemeral":
            continue
        key_paths.add(
            tuple(
                relation_asset_key_path(
                    database=str(props.get("database") or ""),
                    schema=str(props.get("schema") or ""),
                    identifier=str(props.get("identifier") or props.get("name") or ""),
                )
            )
        )
    return key_paths


def load_unobserved_source_specs() -> list[dict[str, Any]]:
    """Load specs for source tables that can never emit Dagster events.

    Only sources with observe metadata (``meta.luban.observe.watermark_column`` /
    ``watermark_sql``) or an ``meta.luban.external_code_location`` marker get an
    observable/materializable asset definition (here or in another code location).
    Every other source table is returned here so that ``build_unobserved_source_assets``
    can give it a lineage-only AssetSpec: without one the key exists in the asset
    graph only as a bare dependency node, and it can never record a materialization
    or observation event.

    Sources whose relation key is already produced by a dbt model/seed/snapshot are
    skipped to avoid duplicate asset definitions.

    Each returned dict contains ``source``, ``table`` (physical identifier),
    ``name`` (logical dbt name), ``key_path`` (relation-based AssetKey path), and
    ``group`` (from ``meta.luban.group``, table-level first, then source-level).
    """
    prepare_manifest_if_missing()
    with _manifest_path().open("r", encoding="utf-8") as f:
        manifest = json.load(f)

    observable_pairs = {
        (str(spec.get("source") or ""), str(spec.get("name") or spec.get("table") or ""))
        for spec in load_automation_observable_sources()
    }
    external_pairs = load_external_source_names()
    node_key_paths = _materializing_node_key_paths(manifest)

    specs: list[dict[str, Any]] = []
    for props in (manifest.get("sources") or {}).values():
        if not isinstance(props, dict):
            continue
        source_name = props.get("source_name")
        dbt_name = props.get("name")
        if not source_name or not dbt_name:
            continue
        pair = (str(source_name), str(dbt_name))
        if pair in observable_pairs or pair in external_pairs:
            continue
        identifier = str(props.get("identifier") or dbt_name)
        key_path = relation_asset_key_path(
            database=str(props.get("database") or ""),
            schema=str(props.get("schema") or ""),
            identifier=identifier,
        )
        if tuple(key_path) in node_key_paths:
            continue
        group = _extract_group_from_meta(props.get("meta"))
        if group is None:
            group = _extract_group_from_meta(props.get("source_meta"))
        specs.append(
            {
                "source": str(source_name),
                "table": identifier,
                "name": str(dbt_name),
                "key_path": key_path,
                "group": str(group) if group is not None else None,
            }
        )

    return sorted(specs, key=lambda s: (s["source"], s["table"]))


def load_unobserved_source_key_paths() -> list[list[str]]:
    """Return relation-based AssetKey paths for source tables that can never emit events.

    Thin wrapper around ``load_unobserved_source_specs`` for callers that only need
    the keys (e.g. scoping the eager condition's ``any_deps_missing`` gate).
    """
    return sorted(spec["key_path"] for spec in load_unobserved_source_specs())

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ...resources.dbt import get_dbt_project_dir
from ..dbt.prepare import prepare_manifest_if_missing


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
                "watermark_column": str(watermark_column) if watermark_column is not None else None,
                "watermark_sql": str(watermark_sql) if watermark_sql is not None else None,
                "group": str(group) if group is not None else None,
            }
        )

    return sorted(specs, key=lambda s: (s["source"], s["table"]))

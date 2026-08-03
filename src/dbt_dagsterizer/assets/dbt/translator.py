from __future__ import annotations

import os
import re
from typing import Any, Mapping, Optional

import dagster as dg
from dagster_dbt import DagsterDbtTranslator


def _relation_asset_key(dbt_resource_props: Mapping[str, Any]) -> dg.AssetKey:
    """Build a relation-based AssetKey from dbt resource properties.

    Uses the physical relation identifiers (database, schema, identifier)
    instead of logical names (source_name, name) to produce keys that are
    stable across different code locations referencing the same physical table.

    Format: ``dbt/<database>/<schema>/<identifier>`` (empty components are omitted)
    """
    database = dbt_resource_props.get("database") or ""
    schema = dbt_resource_props.get("schema") or ""
    identifier = dbt_resource_props.get("identifier") or dbt_resource_props.get("name") or ""
    return dg.AssetKey(relation_asset_key_path(database=str(database), schema=str(schema), identifier=str(identifier)))


def _sanitize_relation_part(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_]", "_", value)


def relation_asset_key_path(*, database: str, schema: str, identifier: str) -> list[str]:
    """Return the AssetKey path components for a dbt relation.

    Useful for constructing specs that must match the translator's
    relation-based keys at config/auto_config time.
    """
    database = _sanitize_relation_part(database or "")
    schema = _sanitize_relation_part(schema or "")
    identifier = _sanitize_relation_part(identifier or "")

    if not database and schema:
        database, schema = schema, ""

    parts = [p for p in [database, schema, identifier] if p]
    return ["dbt", *parts]


class LubanDagsterDbtTranslator(DagsterDbtTranslator):
    def __init__(
        self,
        daily_partitions_def: Optional[dg.PartitionsDefinition],
        automation_observable_tables: set[str] | None = None,
        partitions_by_model: dict[str, str] | None = None,
    ):
        super().__init__()
        self.daily_partitions_def = daily_partitions_def
        self.automation_observable_tables = automation_observable_tables or set()
        self.partitions_by_model = partitions_by_model or {}
        self.propagator_mode = os.getenv(
            "LUBAN_PARTITION_CHANGE_PROPAGATOR_MODE", "sensor").strip().lower()

    def get_automation_condition(self, dbt_resource_props):
        resource_type = dbt_resource_props.get("resource_type")
        if resource_type != "model":
            return None

        name = dbt_resource_props.get("name")
        tags = set(dbt_resource_props.get("tags", []))
        is_daily = bool(name) and self.partitions_by_model.get(
            str(name)) == "daily"

        automation_tables = self.automation_observable_tables

        if name in automation_tables:
            return dg.AutomationCondition.eager()

        if self._propagator_mode_is_eager() and is_daily:
            return dg.AutomationCondition.eager()

        if "dim" in tags:
            return dg.AutomationCondition.eager()

        if "automation_table" in tags:
            return dg.AutomationCondition.eager()

        if "materialize_at_startup" in tags:
            return dg.AutomationCondition.missing()

        return None

    def _propagator_mode_is_eager(self) -> bool:
        return self.propagator_mode == "eager"

    def get_asset_key(self, dbt_resource_props: Mapping[str, Any]) -> dg.AssetKey:
        return _relation_asset_key(dbt_resource_props)

    def get_group_name(self, dbt_resource_props: Mapping[str, Any]) -> Optional[str]:
        resource_type = dbt_resource_props.get("resource_type")

        if resource_type != "model":
            return resource_type

        original_file_path = (dbt_resource_props.get("original_file_path") or "").replace("\\", "/").lstrip("./")
        if original_file_path:
            parts = [p for p in original_file_path.split("/") if p]
            if "models" in parts:
                models_idx = parts.index("models")
                if len(parts) > models_idx + 2:
                    return parts[models_idx + 1]
                return None
                
        fqn = dbt_resource_props.get("fqn") or []
        if len(fqn) >= 3:
            return str(fqn[1])
        return None

    def get_partitions_def(self, dbt_resource_props: Mapping[str, Any]) -> Optional[dg.PartitionsDefinition]:
        """Return partition definitions for individual models.
        
        This enables partitions to be visible on the Assets page in the Dagster UI.
        
        IMPORTANT: This only works when ALL models in the same @dbt_assets have the SAME
        partition type (e.g., all daily, or all unpartitioned). If you have mixed partition
        types, keep returning None and handle partitions at the job/schedule level instead.
        """
        resource_type = dbt_resource_props.get("resource_type")
        if resource_type != "model":
            return None
        
        name = dbt_resource_props.get("name")
        if not name:
            return None
        
        # Look up partition type for this model
        partition_type = self.partitions_by_model.get(name)
        
        if partition_type == "daily":
            return self.daily_partitions_def
        
        # Model is unpartitioned or partition type not defined
        return None

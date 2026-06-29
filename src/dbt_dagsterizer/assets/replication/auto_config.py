"""Auto-configuration for StarRocks-to-SQL Server replication assets.

Reads ``replication.entries`` from ``dagsterization.yml`` and produces a list
of spec dicts that ``factory.py`` turns into Dagster ``@asset`` definitions.
"""
from __future__ import annotations

from pathlib import Path

from ...assets.dbt.translator import relation_asset_key_path
from ...dbt.manifest import iter_models, load_manifest
from ...orchestration_config import (
    default_orchestration_path,
    resolve_orchestration_path,
)
from ...orchestration_config import (
    index as index_orch,
)
from ...orchestration_config import (
    load_or_create as load_orch,
)
from ...resources.dbt import get_dbt_project_dir


def build_auto_replication_specs() -> list[dict]:
    """Build replication asset specs from dagsterization.yml.

    Returns an empty list when replication is disabled or no entries are enabled.
    """
    dbt_project_dir = get_dbt_project_dir()
    cfg_path = resolve_orchestration_path(
        dbt_project_dir=dbt_project_dir,
        path_=Path(default_orchestration_path(dbt_project_dir=dbt_project_dir).name),
    )
    cfg = load_orch(cfg_path)
    idx = index_orch(cfg)

    if not idx.replication_enabled:
        return []

    manifest = load_manifest()
    existing_models = {m.name for m in iter_models(manifest)}
    model_relation_map: dict[str, list[str]] = {}
    model_db_map: dict[str, tuple[str, str, str]] = {}
    for m in iter_models(manifest):
        model_relation_map[m.name] = relation_asset_key_path(
            database=m.database, schema=m.schema, identifier=m.identifier
        )
        model_db_map[m.name] = (m.database, m.schema, m.identifier)

    specs: list[dict] = []
    for model_name, entry in sorted(idx.replication_entries.items()):
        if not entry.enabled:
            continue
        if model_name not in existing_models:
            raise ValueError(f"Replication entry references missing dbt model '{model_name}'")

        relation = model_relation_map[model_name]
        source_database, source_schema, source_table = model_db_map[model_name]
        partition_type = idx.partitions_by_model.get(model_name, "unpartitioned")

        spec: dict = {
            "name": f"replicate_{model_name}",
            "model": model_name,
            "source_relation": relation,
            "source_database": source_database or source_schema,
            "source_schema": source_schema,
            "source_table": source_table or model_name,
            "destination_table": entry.destination_table,
            "destination_schema": entry.destination_schema,
            "write_disposition": entry.write_disposition,
            "partition_column": entry.partition_column,
            "partition_type": partition_type,
        }
        specs.append(spec)

    return specs

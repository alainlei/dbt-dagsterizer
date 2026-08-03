"""Replication trigger sensors: auto-configuration from dagsterization.yml.

Builds sensor specs for enabled replication entries so that a dedicated
sensor watches each upstream dbt model's materialization events and triggers
the corresponding replication job — per partition for partitioned models, or
once (full table) for unpartitioned models.
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


def build_auto_replication_trigger_specs() -> list[dict]:
    """Build replication trigger sensor specs from dagsterization.yml.

    Every enabled replication entry gets a trigger sensor.  Partitioned
    entries trigger the replication job per materialized partition; and
    unpartitioned entries trigger a single full-table replication run when
    the upstream dbt model materializes.

    Returns an empty list when replication is disabled.
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
    model_relation_map: dict[str, list[str]] = {}
    for m in iter_models(manifest):
        model_relation_map[m.name] = relation_asset_key_path(
            database=m.database, schema=m.schema, identifier=m.identifier
        )

    specs: list[dict] = []
    for model_name, entry in sorted(idx.replication_entries.items()):
        if not entry.enabled:
            continue

        partition_type = idx.partitions_by_model.get(model_name, "unpartitioned")
        if partition_type in (None, ""):
            partition_type = "unpartitioned"

        relation = model_relation_map.get(model_name)
        if relation is None:
            continue

        job_name = f"replicate_{model_name}_job"

        specs.append(
            {
                "name": f"replicate_{model_name}_trigger",
                "job_name": job_name,
                "upstream_model_name": model_name,
                "upstream_model_relation": relation,
                "partition_type": partition_type,
                "enabled": True,
                "minimum_interval_seconds": 30,
            }
        )

    return specs

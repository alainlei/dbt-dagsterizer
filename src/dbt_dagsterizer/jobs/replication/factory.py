"""Build Dagster ``define_asset_job`` definitions for replication entries."""
from __future__ import annotations

from pathlib import Path

from dagster import AssetKey, AssetSelection, define_asset_job

from ...k8s_tags import with_luban_run_k8s_config_tag
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
from ...partitions import get_partitions_def
from ...partitions_registry import get_dynamic_partitions_defs
from ...resources.dbt import get_dbt_project_dir


def build_replication_jobs(job_specs: list[dict]) -> list:
    """Build ``define_asset_job`` for each replication spec."""
    if not job_specs:
        return []

    dbt_project_dir = get_dbt_project_dir()
    cfg_path = resolve_orchestration_path(
        dbt_project_dir=dbt_project_dir,
        path_=Path(default_orchestration_path(dbt_project_dir=dbt_project_dir).name),
    )
    cfg = load_orch(cfg_path)
    idx = index_orch(cfg)
    dynamic_partitions_defs = get_dynamic_partitions_defs(dbt_project_dir)

    jobs: list = []
    for spec in job_specs:
        job_name = spec["name"]
        asset_key_str = spec["asset_key"]
        partition_type = spec.get("partition_type", "unpartitioned")

        partitions_def = get_partitions_def(
            partition_type,
            dynamic_partitions_defs=dynamic_partitions_defs,
            include_current_day_partition=idx.daily_include_current_day_partition,
        )

        # Select the single replication asset by its key
        # AssetKey path is ["replication", model_name] — the asset name is a flat string
        selection = AssetSelection.keys(AssetKey([asset_key_str]))

        jobs.append(
            define_asset_job(
                name=job_name,
                selection=selection,
                partitions_def=partitions_def,
                tags=with_luban_run_k8s_config_tag(None),
            )
        )

    return jobs

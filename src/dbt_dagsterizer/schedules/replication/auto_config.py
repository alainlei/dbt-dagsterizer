"""Auto-configuration for replication schedules.

Creates schedule specs that match each replication job with appropriate timing.
"""
from __future__ import annotations

from pathlib import Path

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


def build_auto_replication_schedule_specs() -> list[dict]:
    """Build schedule specs for replication jobs.

    Note: Replication is primarily triggered via asset dependencies (when dbt
    assets materialize). Schedules are optional and disabled by default.
    Set replication.schedules.enabled=true in dagsterization.yml to enable.

    Returns an empty list when replication is disabled or schedules are not enabled.
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

    # Check if schedules are explicitly enabled (default: False)
    # Schedules are optional - replication can work via asset dependencies alone
    replication_cfg = cfg.get("replication", {})
    if not replication_cfg.get("schedules", {}).get("enabled", False):
        return []

    specs: list[dict] = []
    for model_name, entry in sorted(idx.replication_entries.items()):
        if not entry.enabled:
            continue

        job_name = f"replicate_{model_name}_job"  # Match job name with _job suffix
        partition_type = idx.partitions_by_model.get(model_name, "unpartitioned")

        # Default: run 30 minutes after midnight UTC (adjust as needed)
        specs.append(
            {
                "name": f"replicate_{model_name}_schedule",
                "job_name": job_name,
                "cron_schedule": "30 0 * * *",  # 00:30 UTC daily
                "partition_type": partition_type,
                "enabled": True,
            }
        )

    return specs

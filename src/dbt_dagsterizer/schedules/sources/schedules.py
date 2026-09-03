import os
from pathlib import Path

from dagster import DefaultScheduleStatus, ScheduleDefinition

from ...orchestration_config import (
    default_orchestration_path,
    normalize_timezone,
    resolve_orchestration_path,
)
from ...orchestration_config import (
    load_or_create as load_orch,
)
from ...resources.dbt import get_dbt_project_dir


def _get_global_timezone() -> str:
    """Read the global timezone from dagsterization.yml, defaulting to UTC."""
    try:
        dbt_project_dir = get_dbt_project_dir()
        cfg_path = resolve_orchestration_path(
            dbt_project_dir=dbt_project_dir,
            path_=Path(default_orchestration_path(dbt_project_dir=dbt_project_dir).name),
        )
        cfg = load_orch(cfg_path)
        return normalize_timezone(cfg.get("timezone"), default="UTC")
    except Exception:
        return "UTC"


def get_observe_sources_schedule():
    from ...assets.sources.automation import load_filtered_observable_sources

    if not load_filtered_observable_sources():
        return None
    return ScheduleDefinition(
        name="observe_sources_schedule",
        cron_schedule=os.getenv("LUBAN_OBSERVE_SOURCES_CRON", "*/5 * * * *"),
        job_name="observe_sources_job",
        default_status=DefaultScheduleStatus.RUNNING,
        execution_timezone=_get_global_timezone(),
    )

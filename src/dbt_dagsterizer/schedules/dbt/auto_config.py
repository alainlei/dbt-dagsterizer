from __future__ import annotations

from pathlib import Path

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
from .presets import daily_at


def build_auto_dbt_schedule_specs() -> list[dict]:
    manifest = load_manifest()
    models = {m.name for m in iter_models(manifest)}
    dbt_project_dir = get_dbt_project_dir()
    cfg_path = resolve_orchestration_path(
        dbt_project_dir=dbt_project_dir,
        path_=Path(default_orchestration_path(dbt_project_dir=dbt_project_dir).name),
    )
    cfg = load_orch(cfg_path)
    idx = index_orch(cfg)

    specs: list[dict] = []
    schedules = cfg.get("schedules")
    if isinstance(schedules, dict):
        for name in sorted(schedules.keys()):
            schedule_meta = schedules.get(name)
            if not isinstance(name, str) or not name.strip() or not isinstance(schedule_meta, dict):
                continue

            enabled = bool(schedule_meta.get("enabled", True))
            schedule_type = schedule_meta.get("type")
            job_name = schedule_meta.get("job_name")
            if not isinstance(job_name, str) or not job_name.strip():
                raise ValueError(f"Schedule '{name}' requires job_name")

            if schedule_type == "daily_at":
                hour = int(schedule_meta.get("hour", 0))
                minute = int(schedule_meta.get("minute", 0))
                lookback_days = int(schedule_meta.get("lookback_days", 0))
                offset_days = int(schedule_meta.get("offset_days", 1))
                specs.append(
                    daily_at(
                        name=str(name),
                        job_name=str(job_name),
                        hour=hour,
                        minute=minute,
                        lookback_days=lookback_days,
                        offset_days=offset_days,
                        enabled=enabled,
                    )
                )
                continue

            raise ValueError(f"Unsupported schedule type '{schedule_type}' for schedule '{name}'")

    job_names: set[str] = set()
    jobs = cfg.get("jobs")
    if isinstance(jobs, dict):
        for job in jobs.keys():
            if isinstance(job, str) and job.strip():
                job_names.add(job.strip())
    for m in idx.asset_job_models:
        job_names.add(f"dbt_{m}_asset_job")

    for spec in specs:
        if spec.get("job_name") not in job_names:
            raise ValueError(f"Schedule '{spec.get('name')}' references unknown job_name '{spec.get('job_name')}'")

    for d in cfg.get("partition_change", {}).get("detectors", []) if isinstance(cfg.get("partition_change"), dict) else []:
        if isinstance(d, dict):
            model = d.get("model")
            if isinstance(model, str) and model.strip() and model.strip() not in models:
                raise ValueError(f"Partition-change detector references missing dbt model '{model}'")

    return sorted(specs, key=lambda s: str(s.get("name", "")))

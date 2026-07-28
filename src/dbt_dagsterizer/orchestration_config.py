from __future__ import annotations

from collections.abc import Mapping, MutableMapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from ruamel.yaml import YAML


def _yaml() -> YAML:
    y = YAML()
    y.preserve_quotes = True
    y.indent(mapping=2, sequence=4, offset=2)
    y.width = 4096
    return y


def default_orchestration_path(*, dbt_project_dir: Path) -> Path:
    return dbt_project_dir / "dagsterization.yml"


def normalize_timezone(timezone: object, *, default: str = "UTC") -> str:
    if timezone is None:
        return default
    if not isinstance(timezone, str):
        raise ValueError("timezone must be a non-empty IANA timezone string")

    normalized = timezone.strip()
    if not normalized:
        raise ValueError("timezone must be a non-empty IANA timezone string")

    try:
        ZoneInfo(normalized)
    except ZoneInfoNotFoundError as exc:
        raise ValueError(f"invalid timezone '{normalized}'") from exc

    return normalized


def load_or_create(path: Path) -> MutableMapping[str, Any]:
    y = _yaml()
    if not path.exists():
        return {
            "version": 1,
            "timezone": "UTC",
            "jobs": {},
            "asset_jobs": [],
            "partitions": {},
            "schedules": {},
            "partition_change": {"detectors": [], "propagators": []},
            "ssrs_reports": [],
        }

    with path.open("r", encoding="utf-8") as f:
        data = y.load(f)
    if data is None:
        return {
            "version": 1,
            "timezone": "UTC",
            "jobs": {},
            "asset_jobs": [],
            "partitions": {},
            "schedules": {},
            "partition_change": {"detectors": [], "propagators": []},
            "ssrs_reports": [],
        }
    if not isinstance(data, MutableMapping):
        raise ValueError(f"Orchestration config must be a mapping: {path}")

    if "version" not in data:
        data["version"] = 1
    if "timezone" not in data:
        data["timezone"] = "UTC"
    if "jobs" not in data:
        data["jobs"] = {}
    if "asset_jobs" not in data:
        data["asset_jobs"] = []
    if "partitions" not in data:
        data["partitions"] = {}
    if "schedules" not in data:
        data["schedules"] = {}
    if "partition_change" not in data:
        data["partition_change"] = {"detectors": [], "propagators": []}
    else:
        pc = data.get("partition_change")
        if isinstance(pc, MutableMapping):
            if "propagators" not in pc and "propagations" in pc:
                pc["propagators"] = pc.get("propagations")
                del pc["propagations"]
    if "ssrs_reports" not in data:
        data["ssrs_reports"] = []

    return data


def resolve_orchestration_path(*, dbt_project_dir: Path, path_: Path) -> Path:
    if path_.is_absolute():
        return path_
    candidate = (dbt_project_dir / path_).resolve()
    return candidate


def save(path: Path, data: Any) -> None:
    y = _yaml()
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        y.dump(data, f)


def _ensure_mapping(parent: MutableMapping[str, Any], key: str) -> MutableMapping[str, Any]:
    value = parent.get(key)
    if value is None:
        parent[key] = {}
        value = parent[key]
    if not isinstance(value, MutableMapping):
        raise ValueError(f"Expected mapping at '{key}'")
    return value


def _ensure_list(parent: MutableMapping[str, Any], key: str) -> list[Any]:
    value = parent.get(key)
    if value is None:
        parent[key] = []
        value = parent[key]
    if not isinstance(value, list):
        raise ValueError(f"Expected list at '{key}'")
    return value


@dataclass(frozen=True)
class OrchestrationIndex:
    partitions_by_model: dict[str, str]
    asset_job_models: set[str]
    group_job_by_model: dict[str, str]
    timezone: str = "UTC"  # Global schedule execution timezone


def index(data: Mapping[str, Any]) -> OrchestrationIndex:
    partitions_by_model: dict[str, str] = {}

    partitions = data.get("partitions")
    if isinstance(partitions, Mapping):
        for p_type, models in partitions.items():
            if not isinstance(p_type, str):
                continue
            if p_type not in {"daily", "unpartitioned"}:
                continue
            if not isinstance(models, list):
                continue
            for m in models:
                if isinstance(m, str) and m.strip():
                    partitions_by_model[m.strip()] = p_type

    asset_job_models: set[str] = set()
    asset_jobs = data.get("asset_jobs")
    if isinstance(asset_jobs, list):
        for m in asset_jobs:
            if isinstance(m, str) and m.strip():
                asset_job_models.add(m.strip())

    group_job_by_model: dict[str, str] = {}
    jobs = data.get("jobs")
    if isinstance(jobs, Mapping):
        for job_name, job_cfg in jobs.items():
            if not isinstance(job_name, str) or not job_name.strip():
                continue
            if not isinstance(job_cfg, Mapping):
                continue
            models = job_cfg.get("models")
            if not isinstance(models, list):
                continue
            for m in models:
                if not isinstance(m, str) or not m.strip():
                    continue
                name = m.strip()
                if name in group_job_by_model and group_job_by_model[name] != job_name:
                    raise ValueError(f"Model '{name}' appears in multiple jobs: '{group_job_by_model[name]}' and '{job_name}'")
                group_job_by_model[name] = job_name

    # Parse global timezone
    timezone = normalize_timezone(data.get("timezone"), default="UTC")

    return OrchestrationIndex(
        partitions_by_model=partitions_by_model,
        asset_job_models=asset_job_models,
        group_job_by_model=group_job_by_model,
        timezone=timezone,
    )


def set_timezone(*, data: MutableMapping[str, Any], timezone: str) -> None:
    """Set the global schedule execution timezone.

    Args:
        data: Orchestration config dict
        timezone: IANA timezone name (e.g. 'UTC', 'Asia/Shanghai')
    """
    data["timezone"] = normalize_timezone(timezone, default="UTC")


def set_partition(*, data: MutableMapping[str, Any], model: str, partition: str | None) -> None:
    model = model.strip()
    if not model:
        return

    partitions = _ensure_mapping(data, "partitions")
    for p_type in ["daily", "unpartitioned"]:
        models = partitions.get(p_type)
        if isinstance(models, list):
            partitions[p_type] = [m for m in models if not (isinstance(m, str) and m.strip() == model)]

    if partition is None:
        return
    if partition not in {"daily", "unpartitioned"}:
        raise ValueError("partition must be one of daily|unpartitioned")

    models = partitions.get(partition)
    if not isinstance(models, list):
        models = []
        partitions[partition] = models
    if model not in [m for m in models if isinstance(m, str)]:
        models.append(model)


def set_asset_job(*, data: MutableMapping[str, Any], model: str, enabled: bool) -> None:
    model = model.strip()
    if not model:
        return
    asset_jobs = _ensure_list(data, "asset_jobs")
    asset_jobs_filtered = [m for m in asset_jobs if not (isinstance(m, str) and m.strip() == model)]
    if enabled:
        asset_jobs_filtered.append(model)
    data["asset_jobs"] = asset_jobs_filtered


def set_group_job(
    *,
    data: MutableMapping[str, Any],
    job_name: str,
    models: list[str],
    include_upstream: bool,
    partitions: str | None,
) -> None:
    job_name = job_name.strip()
    if not job_name:
        raise ValueError("job_name must be non-empty")
    jobs = _ensure_mapping(data, "jobs")
    job = jobs.get(job_name)
    if job is None:
        jobs[job_name] = {}
        job = jobs[job_name]
    if not isinstance(job, MutableMapping):
        raise ValueError(f"jobs.{job_name} must be a mapping")

    job["models"] = sorted(set([m.strip() for m in models if m.strip()]))
    job["include_upstream"] = bool(include_upstream)
    if partitions is None:
        job.pop("partitions", None)
    else:
        if partitions not in {"daily", "unpartitioned"}:
            raise ValueError("partitions must be one of daily|unpartitioned")
        job["partitions"] = partitions


def delete_group_job(*, data: MutableMapping[str, Any], job_name: str) -> bool:
    job_name = job_name.strip()
    if not job_name:
        return False
    jobs = data.get("jobs")
    if not isinstance(jobs, MutableMapping):
        return False
    if job_name not in jobs:
        return False
    del jobs[job_name]
    return True


def set_schedule(
    *,
    data: MutableMapping[str, Any],
    name: str,
    job_name: str,
    schedule_type: str,
    hour: int,
    minute: int,
    lookback_days: int,
    offset_days: int = 1,
    enabled: bool,
) -> None:
    name = name.strip()
    if not name:
        raise ValueError("schedule name must be non-empty")
    job_name = job_name.strip()
    if not job_name:
        raise ValueError("schedule job_name must be non-empty")
    if schedule_type != "daily_at":
        raise ValueError("schedule type must be 'daily_at'")

    schedules = _ensure_mapping(data, "schedules")
    schedules[name] = {
        "type": "daily_at",
        "job_name": job_name,
        "hour": int(hour),
        "minute": int(minute),
        "lookback_days": int(lookback_days),
        "offset_days": int(offset_days),
        "enabled": bool(enabled),
    }


def set_partition_change_detector(
    *,
    data: MutableMapping[str, Any],
    model: str,
    enabled: bool,
    name: str | None,
    job_name: str | None,
    detect_relation: str | None,
    detect_source: dict[str, str] | None,
    partition_date_expr: str,
    updated_at_expr: str,
    lookback_days: int,
    offset_days: int,
    minimum_interval_seconds: int,
) -> None:
    model = model.strip()
    if not model:
        raise ValueError("model must be non-empty")

    pc = _ensure_mapping(data, "partition_change")
    detectors = _ensure_list(pc, "detectors")

    detectors_filtered: list[dict[str, Any]] = []
    for d in detectors:
        if isinstance(d, Mapping) and d.get("model") == model:
            continue
        if isinstance(d, dict):
            detectors_filtered.append(d)

    entry: dict[str, Any] = {
        "model": model,
        "enabled": bool(enabled),
        "partition_date_expr": partition_date_expr,
        "updated_at_expr": updated_at_expr,
        "lookback_days": int(lookback_days),
        "offset_days": int(offset_days),
        "minimum_interval_seconds": int(minimum_interval_seconds),
    }
    if name:
        entry["name"] = name
    if job_name:
        entry["job_name"] = job_name
    if detect_relation:
        entry["detect_relation"] = detect_relation
    if detect_source:
        entry["detect_source"] = detect_source

    detectors_filtered.append(entry)
    pc["detectors"] = detectors_filtered


def set_partition_change_propagation(
    *,
    data: MutableMapping[str, Any],
    upstream_model: str,
    enabled: bool,
    name: str | None,
    minimum_interval_seconds: int,
    targets: list[str],
) -> None:
    upstream_model = upstream_model.strip()
    if not upstream_model:
        raise ValueError("upstream_model must be non-empty")
    pc = _ensure_mapping(data, "partition_change")
    if "propagators" not in pc and "propagations" in pc and isinstance(pc.get("propagations"), list):
        pc["propagators"] = pc.get("propagations")
        del pc["propagations"]
    propagations = _ensure_list(pc, "propagators")

    propagations_filtered: list[dict[str, Any]] = []
    for p in propagations:
        if isinstance(p, Mapping) and p.get("upstream_model") == upstream_model:
            continue
        if isinstance(p, dict):
            propagations_filtered.append(p)

    entry: dict[str, Any] = {
        "upstream_model": upstream_model,
        "enabled": bool(enabled),
        "minimum_interval_seconds": int(minimum_interval_seconds),
        "targets": [{"job_name": t} for t in targets],
    }
    if name:
        entry["name"] = name

    propagations_filtered.append(entry)
    pc["propagators"] = propagations_filtered


def set_ssrs_report(
    *,
    data: MutableMapping[str, Any],
    name: str,
    model: str,
    subscription_description: str,
    enabled: bool,
) -> None:
    name = name.strip()
    if not name:
        raise ValueError("report name must be non-empty")
    model = model.strip()
    if not model:
        raise ValueError("report model must be non-empty")
    subscription_description = subscription_description.strip()
    if not subscription_description:
        raise ValueError("subscription_description must be non-empty")

    entry: dict[str, Any] = {
        "name": name,
        "model": model,
        "subscription_description": subscription_description,
        "enabled": bool(enabled),
    }

    reports = _ensure_list(data, "ssrs_reports")

    reports_filtered: list[dict[str, Any]] = []
    for r in reports:
        if isinstance(r, Mapping) and r.get("name") == name:
            continue
        if isinstance(r, dict):
            reports_filtered.append(r)

    reports_filtered.append(entry)
    data["ssrs_reports"] = reports_filtered


def delete_ssrs_report(*, data: MutableMapping[str, Any], name: str) -> bool:
    name = name.strip()
    if not name:
        return False
    reports = data.get("ssrs_reports")
    if not isinstance(reports, list):
        return False
    reports_filtered = [r for r in reports if not (isinstance(r, Mapping) and r.get("name") == name)]
    if len(reports_filtered) == len(reports):
        return False
    data["ssrs_reports"] = reports_filtered
    return True


def derive_job_name_for_model(index: OrchestrationIndex, *, model: str) -> str | None:
    model = model.strip()
    if not model:
        return None
    if model in index.asset_job_models:
        return f"dbt_{model}_asset_job"
    job = index.group_job_by_model.get(model)
    if job:
        return str(job)
    return None

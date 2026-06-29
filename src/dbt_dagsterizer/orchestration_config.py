from __future__ import annotations

from collections.abc import Mapping, MutableMapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from ruamel.yaml import YAML


@dataclass(frozen=True)
class DynamicPartitionConfig:
    """Configuration for a dynamic partition definition."""
    name: str
    initial_partition_keys: list[str]


@dataclass(frozen=True)
class ReplicationEntry:
    """Configuration for a single replication entry (StarRocks -> SQL Server)."""
    model: str
    enabled: bool
    destination_table: str
    destination_schema: str
    write_disposition: str
    partition_column: str | None


def _yaml() -> YAML:
    y = YAML()
    y.preserve_quotes = True
    y.indent(mapping=2, sequence=4, offset=2)
    y.width = 4096
    return y


def default_orchestration_path(*, dbt_project_dir: Path) -> Path:
    return dbt_project_dir / "dagsterization.yml"


def load_or_create(path: Path) -> MutableMapping[str, Any]:
    y = _yaml()
    if not path.exists():
        return {
            "version": 1,
            "jobs": {},
            "asset_jobs": [],
            "partitions": {},
            "schedules": {},
            "partition_change": {"detectors": [], "propagators": []},
            "replication": {"enabled": False, "entries": []},
        }

    with path.open("r", encoding="utf-8") as f:
        data = y.load(f)
    if data is None:
        return {
            "version": 1,
            "jobs": {},
            "asset_jobs": [],
            "partitions": {},
            "schedules": {},
            "partition_change": {"detectors": [], "propagators": []},
            "replication": {"enabled": False, "entries": []},
        }
    if not isinstance(data, MutableMapping):
        raise ValueError(f"Orchestration config must be a mapping: {path}")

    if "version" not in data:
        data["version"] = 1
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

    if "replication" not in data:
        data["replication"] = {"enabled": False, "entries": []}
    else:
        repl = data.get("replication")
        if isinstance(repl, MutableMapping):
            if "enabled" not in repl:
                repl["enabled"] = False
            if "entries" not in repl:
                repl["entries"] = []
        else:
            data["replication"] = {"enabled": False, "entries": []}

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
    partitions_by_model: dict[str, str]  # model -> "daily"|"dynamic:name"|"unpartitioned"
    dynamic_partitions: dict[str, DynamicPartitionConfig]  # partition_name -> config
    asset_job_models: set[str]
    group_job_by_model: dict[str, str]
    daily_include_current_day_partition: bool = False  # DailyPartitionsDefinition end_offset from daily_config (true -> end_offset=1)
    replication_enabled: bool = False
    replication_entries: dict[str, ReplicationEntry] = field(default_factory=dict)  # model_name -> config


def index(data: Mapping[str, Any]) -> OrchestrationIndex:
    partitions_by_model: dict[str, str] = {}
    dynamic_partitions: dict[str, DynamicPartitionConfig] = {}

    partitions = data.get("partitions")
    if isinstance(partitions, Mapping):
        # Process daily and unpartitioned partitions
        for p_type, models in partitions.items():
            if not isinstance(p_type, str):
                continue
            if p_type in {"daily", "unpartitioned"}:
                if not isinstance(models, list):
                    continue
                for m in models:
                    if isinstance(m, str) and m.strip():
                        partitions_by_model[m.strip()] = p_type
        
        # Process dynamic partitions
        if "dynamic" in partitions:
            dynamic_defs = partitions["dynamic"]
            if isinstance(dynamic_defs, list):
                for d in dynamic_defs:
                    if not isinstance(d, Mapping):
                        continue
                    name = d.get("name")
                    initial_keys = d.get("initial_partition_keys")
                    if isinstance(name, str) and name.strip() and isinstance(initial_keys, list):
                        name_str = name.strip()
                        partition_spec = f"dynamic:{name_str}"
                        dynamic_partitions[name_str] = DynamicPartitionConfig(
                            name=name_str,
                            initial_partition_keys=initial_keys,
                        )
                        # Add models assigned to this dynamic partition
                        models = d.get("models")
                        if isinstance(models, list):
                            for m in models:
                                if isinstance(m, str) and m.strip():
                                    partitions_by_model[m.strip()] = partition_spec

    # Parse daily partition config
    daily_include_current_day_partition = False
    if isinstance(partitions, Mapping):
        daily_config = partitions.get("daily_config")
        if isinstance(daily_config, Mapping):
            raw_include_current_day_partition = daily_config.get("include_current_day_partition")
            if raw_include_current_day_partition is not None:
                if not isinstance(raw_include_current_day_partition, bool):
                    raise ValueError("partitions.daily_config.include_current_day_partition must be a boolean")
                daily_include_current_day_partition = raw_include_current_day_partition

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

    replication_enabled = False
    replication_entries: dict[str, ReplicationEntry] = {}
    repl = data.get("replication")
    if isinstance(repl, Mapping):
        raw_enabled = repl.get("enabled")
        if isinstance(raw_enabled, bool):
            replication_enabled = raw_enabled
        entries = repl.get("entries")
        if isinstance(entries, list):
            for entry in entries:
                if not isinstance(entry, Mapping):
                    continue
                model = entry.get("model")
                if not isinstance(model, str) or not model.strip():
                    continue
                model = model.strip()
                replication_entries[model] = ReplicationEntry(
                    model=model,
                    enabled=bool(entry.get("enabled", True)),
                    destination_table=str(entry.get("destination_table") or model),
                    destination_schema=str(entry.get("destination_schema") or "dbo"),
                    write_disposition=str(entry.get("write_disposition") or "replace"),
                    partition_column=(
                        str(entry.get("partition_column")).strip()
                        if isinstance(entry.get("partition_column"), str) and entry.get("partition_column").strip()
                        else None
                    ),
                )

    return OrchestrationIndex(
        partitions_by_model=partitions_by_model,
        dynamic_partitions=dynamic_partitions,
        asset_job_models=asset_job_models,
        group_job_by_model=group_job_by_model,
        daily_include_current_day_partition=daily_include_current_day_partition,
        replication_enabled=replication_enabled,
        replication_entries=replication_entries,
    )


def set_daily_config(
    *,
    data: MutableMapping[str, Any],
    include_current_day_partition: bool | None = None,
) -> None:
    """Set daily partition configuration.

    Args:
        data: Orchestration config dict
        include_current_day_partition: Whether today's partition should be available
    """
    partitions = _ensure_mapping(data, "partitions")
    if include_current_day_partition is not None:
        daily_config = partitions.get("daily_config")
        if not isinstance(daily_config, MutableMapping):
            partitions["daily_config"] = {}
            daily_config = partitions["daily_config"]
        daily_config["include_current_day_partition"] = bool(include_current_day_partition)


def set_partition(*, data: MutableMapping[str, Any], model: str, partition: str | None) -> None:
    """Set partition for a model.
    
    Args:
        data: Orchestration config dict
        model: Model name
        partition: Partition spec ("daily", "unpartitioned", "dynamic:name", or None)
    """
    model = model.strip()
    if not model:
        return

    partitions = _ensure_mapping(data, "partitions")
    
    # Remove model from all existing partition assignments (daily, unpartitioned, and dynamic)
    for p_type in ["daily", "unpartitioned"]:
        models = partitions.get(p_type)
        if isinstance(models, list):
            partitions[p_type] = [m for m in models if not (isinstance(m, str) and m.strip() == model)]
    
    # Remove from any dynamic partitions
    if "dynamic" in partitions and isinstance(partitions["dynamic"], list):
        for d in partitions["dynamic"]:
            if isinstance(d, Mapping) and "models" in d and isinstance(d["models"], list):
                d["models"] = [m for m in d["models"] if not (isinstance(m, str) and m.strip() == model)]

    if partition is None:
        return
    
    # Validate partition spec
    if partition not in {"daily", "unpartitioned"}:
        if not partition.startswith("dynamic:"):
            raise ValueError("partition must be one of daily|unpartitioned|dynamic:name")
        # Validate that the dynamic partition exists
        dynamic_name = partition.split(":", 1)[1]
        dynamic_list = partitions.get("dynamic", [])
        if isinstance(dynamic_list, list):
            found = False
            for d in dynamic_list:
                if isinstance(d, Mapping) and d.get("name") == dynamic_name:
                    found = True
                    break
            if not found:
                raise ValueError(f"Unknown dynamic partition: {dynamic_name}")

    # For daily/unpartitioned, add to the list
    if partition in {"daily", "unpartitioned"}:
        models = partitions.get(partition)
        if not isinstance(models, list):
            models = []
            partitions[partition] = models
        if model not in [m for m in models if isinstance(m, str)]:
            models.append(model)
    # For dynamic partitions, add to the specific dynamic partition's models list
    elif partition.startswith("dynamic:"):
        dynamic_name = partition.split(":", 1)[1]
        dynamic_list = partitions.get("dynamic", [])
        if isinstance(dynamic_list, list):
            for d in dynamic_list:
                if isinstance(d, Mapping) and d.get("name") == dynamic_name:
                    if "models" not in d:
                        d["models"] = []
                    if not isinstance(d["models"], list):
                        d["models"] = []
                    if model not in [m for m in d["models"] if isinstance(m, str)]:
                        d["models"].append(model)
                    break


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
            if not partitions.startswith("dynamic:"):
                raise ValueError("partitions must be one of daily|unpartitioned|dynamic:name")
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


def set_replication_entry(
    *,
    data: MutableMapping[str, Any],
    model: str,
    enabled: bool,
    destination_table: str | None,
    destination_schema: str | None,
    write_disposition: str | None,
    partition_column: str | None,
) -> None:
    """Add or update a replication entry for a dbt model.

    Args:
        data: Orchestration config dict
        model: dbt model name to replicate
        enabled: Whether this replication entry is active
        destination_table: Target table name in SQL Server (default: model name)
        destination_schema: Target schema in SQL Server (default: dbo)
        write_disposition: dlt write disposition - "append", "replace", or "merge" (default: replace)
        partition_column: Column to filter by for partition-aware replication (optional)
    """
    model = model.strip()
    if not model:
        raise ValueError("replication entry model must be non-empty")
    if write_disposition is not None and write_disposition not in {"append", "replace", "merge"}:
        raise ValueError('replication entry write_disposition must be "append", "replace", or "merge"')

    repl = _ensure_mapping(data, "replication")
    entries = _ensure_list(repl, "entries")

    entries_filtered: list[dict[str, Any]] = []
    for e in entries:
        if isinstance(e, Mapping) and e.get("model") == model:
            continue
        if isinstance(e, dict):
            entries_filtered.append(e)

    entry: dict[str, Any] = {
        "model": model,
        "enabled": bool(enabled),
    }
    entry["destination_table"] = destination_table.strip() if destination_table and destination_table.strip() else model
    entry["destination_schema"] = destination_schema.strip() if destination_schema and destination_schema.strip() else "dbo"
    entry["write_disposition"] = write_disposition.strip() if write_disposition and write_disposition.strip() else "replace"
    if partition_column and partition_column.strip():
        entry["partition_column"] = partition_column.strip()

    entries_filtered.append(entry)
    repl["entries"] = entries_filtered


def set_dynamic_partition(
    *,
    data: MutableMapping[str, Any],
    name: str,
    initial_partition_keys: list[str],
) -> None:
    """Add or update a dynamic partition definition.
    
    Args:
        data: Orchestration config dict
        name: Name of the dynamic partition
        initial_partition_keys: List of initial partition keys
    """
    name = name.strip()
    if not name:
        raise ValueError("Dynamic partition name must be non-empty")
    if not initial_partition_keys:
        raise ValueError("initial_partition_keys must be non-empty")
    
    partitions = _ensure_mapping(data, "partitions")
    dynamic_list = _ensure_list(partitions, "dynamic")
    
    # Remove existing entry with same name
    dynamic_list[:] = [d for d in dynamic_list if not (isinstance(d, Mapping) and d.get("name") == name)]
    
    # Add new entry
    dynamic_list.append({
        "name": name,
        "initial_partition_keys": list(initial_partition_keys),
    })


def remove_dynamic_partition(
    *,
    data: MutableMapping[str, Any],
    name: str,
) -> bool:
    """Remove a dynamic partition definition.
    
    Args:
        data: Orchestration config dict
        name: Name of the dynamic partition to remove
    
    Returns:
        True if removed, False if not found
    """
    name = name.strip()
    if not name:
        return False
    
    partitions = data.get("partitions")
    if not isinstance(partitions, MutableMapping):
        return False
    
    dynamic_list = partitions.get("dynamic")
    if not isinstance(dynamic_list, list):
        return False
    
    original_len = len(dynamic_list)
    partitions["dynamic"] = [d for d in dynamic_list if not (isinstance(d, Mapping) and d.get("name") == name)]
    
    return len(partitions["dynamic"]) < original_len


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

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import click

from ..dbt.manifest_prepare import load_manifest, manifest_path
from ..orchestration_config import index as index_orch
from ..orchestration_config import save as save_orch
from .common import existing_model_names


@dataclass(frozen=True)
class ValidationIssue:
    level: str
    message: str


def validate_orchestration(
    *,
    manifest: dict[str, Any],
    orchestration: dict[str, Any],
    require_file_exists: bool,
    orchestration_path: Path,
) -> list[ValidationIssue]:
    issues: list[ValidationIssue] = []
    existing_models = existing_model_names(manifest)
    idx = index_orch(orchestration)

    for model in sorted(idx.asset_job_models):
        if model not in existing_models:
            issues.append(ValidationIssue("error", f"asset_jobs references missing model '{model}'"))

    for model, p_type in sorted(idx.partitions_by_model.items()):
        if model not in existing_models:
            issues.append(ValidationIssue("error", f"partitions references missing model '{model}'"))
        if p_type not in {"daily", "unpartitioned"}:
            issues.append(ValidationIssue("error", f"partitions for model '{model}' must be daily|unpartitioned"))

    jobs = orchestration.get("jobs")
    if jobs is not None and not isinstance(jobs, dict):
        issues.append(ValidationIssue("error", "jobs must be a mapping"))
    job_names: set[str] = set()
    if isinstance(jobs, dict):
        for job_name, job_cfg in jobs.items():
            if not isinstance(job_name, str) or not job_name.strip():
                issues.append(ValidationIssue("error", "jobs contains an empty/non-string key"))
                continue
            job_names.add(job_name.strip())
            if not isinstance(job_cfg, dict):
                issues.append(ValidationIssue("error", f"jobs.{job_name} must be a mapping"))
                continue
            models = job_cfg.get("models")
            if not isinstance(models, list) or not models:
                issues.append(ValidationIssue("error", f"jobs.{job_name}.models must be a non-empty list"))
                continue
            for m in models:
                if not isinstance(m, str) or not m.strip():
                    issues.append(ValidationIssue("error", f"jobs.{job_name}.models contains empty model"))
                    continue
                if m.strip() not in existing_models:
                    issues.append(ValidationIssue("error", f"jobs.{job_name} references missing model '{m.strip()}'"))
            partitions = job_cfg.get("partitions")
            if partitions is not None and partitions not in {"daily", "unpartitioned"}:
                issues.append(ValidationIssue("error", f"jobs.{job_name}.partitions must be daily|unpartitioned when set"))
            include_upstream = job_cfg.get("include_upstream")
            if include_upstream is not None and not isinstance(include_upstream, bool):
                issues.append(ValidationIssue("error", f"jobs.{job_name}.include_upstream must be boolean when set"))

    derived_job_names = set(job_names)
    for m in idx.asset_job_models:
        derived_job_names.add(f"dbt_{m}_asset_job")

    schedules = orchestration.get("schedules")
    if schedules is not None and not isinstance(schedules, dict):
        issues.append(ValidationIssue("error", "schedules must be a mapping"))
    if isinstance(schedules, dict):
        for name, schedule_cfg in schedules.items():
            if not isinstance(name, str) or not name.strip():
                issues.append(ValidationIssue("error", "schedules contains an empty/non-string key"))
                continue
            if not isinstance(schedule_cfg, dict):
                issues.append(ValidationIssue("error", f"schedules.{name} must be a mapping"))
                continue
            if schedule_cfg.get("type") != "daily_at":
                issues.append(ValidationIssue("error", f"schedules.{name}.type must be 'daily_at'"))
            job_name = schedule_cfg.get("job_name")
            if not isinstance(job_name, str) or not job_name.strip():
                issues.append(ValidationIssue("error", f"schedules.{name}.job_name must be non-empty"))
            elif job_name.strip() not in derived_job_names:
                issues.append(ValidationIssue("error", f"schedules.{name}.job_name '{job_name.strip()}' not found"))

            hour = schedule_cfg.get("hour")
            minute = schedule_cfg.get("minute")
            if not isinstance(hour, int) or hour < 0 or hour > 23:
                issues.append(ValidationIssue("error", f"schedules.{name}.hour must be 0..23"))
            if not isinstance(minute, int) or minute < 0 or minute > 59:
                issues.append(ValidationIssue("error", f"schedules.{name}.minute must be 0..59"))
            lookback_days = schedule_cfg.get("lookback_days", 0)
            if not isinstance(lookback_days, int) or lookback_days < 0:
                issues.append(ValidationIssue("error", f"schedules.{name}.lookback_days must be >= 0"))
            offset_days = schedule_cfg.get("offset_days", 1)
            if not isinstance(offset_days, int) or offset_days < 0:
                issues.append(ValidationIssue("error", f"schedules.{name}.offset_days must be >= 0"))

    pc = orchestration.get("partition_change")
    if pc is not None and not isinstance(pc, dict):
        issues.append(ValidationIssue("error", "partition_change must be a mapping"))
        return issues
    detectors = pc.get("detectors") if isinstance(pc, dict) else None
    if detectors is not None and not isinstance(detectors, list):
        issues.append(ValidationIssue("error", "partition_change.detectors must be a list"))
    if isinstance(detectors, list):
        for i, d in enumerate(detectors):
            if not isinstance(d, dict):
                issues.append(ValidationIssue("error", f"partition_change.detectors[{i}] must be a mapping"))
                continue
            model = d.get("model")
            if not isinstance(model, str) or not model.strip():
                issues.append(ValidationIssue("error", f"partition_change.detectors[{i}].model must be non-empty"))
                continue
            if model.strip() not in existing_models:
                issues.append(
                    ValidationIssue("error", f"partition_change.detectors[{i}] references missing model '{model.strip()}'")
                )

            has_detect_relation = isinstance(d.get("detect_relation"), str) and d.get("detect_relation").strip()
            has_detect_source = isinstance(d.get("detect_source"), dict)
            if bool(has_detect_relation) == bool(has_detect_source):
                issues.append(
                    ValidationIssue(
                        "error",
                        f"partition_change.detectors[{i}] must set exactly one of detect_relation/detect_source",
                    )
                )
            for key in ["partition_date_expr", "updated_at_expr"]:
                v = d.get(key)
                if not isinstance(v, str) or not v.strip():
                    issues.append(ValidationIssue("error", f"partition_change.detectors[{i}].{key} must be non-empty"))

            job_name = d.get("job_name")
            if job_name is not None:
                if not isinstance(job_name, str) or not job_name.strip():
                    issues.append(ValidationIssue("error", f"partition_change.detectors[{i}].job_name must be non-empty"))
                elif job_name.strip() not in derived_job_names:
                    issues.append(
                        ValidationIssue(
                            "error",
                            f"partition_change.detectors[{i}].job_name '{job_name.strip()}' not found",
                        )
                    )

    propagations = pc.get("propagators") if isinstance(pc, dict) else None
    if propagations is not None and not isinstance(propagations, list):
        issues.append(ValidationIssue("error", "partition_change.propagators must be a list"))
    if isinstance(propagations, list):
        for i, p in enumerate(propagations):
            if not isinstance(p, dict):
                issues.append(ValidationIssue("error", f"partition_change.propagators[{i}] must be a mapping"))
                continue
            upstream_model = p.get("upstream_model")
            if not isinstance(upstream_model, str) or not upstream_model.strip():
                issues.append(ValidationIssue("error", f"partition_change.propagators[{i}].upstream_model must be non-empty"))
                continue
            if upstream_model.strip() not in existing_models:
                issues.append(
                    ValidationIssue(
                        "error",
                        f"partition_change.propagators[{i}] references missing model '{upstream_model.strip()}'",
                    )
                )
            targets = p.get("targets")
            if not isinstance(targets, list) or not targets:
                issues.append(ValidationIssue("error", f"partition_change.propagators[{i}].targets must be non-empty list"))
                continue
            for j, t in enumerate(targets):
                if not isinstance(t, dict):
                    issues.append(ValidationIssue("error", f"partition_change.propagators[{i}].targets[{j}] must be dict"))
                    continue
                job_name = t.get("job_name")
                if not isinstance(job_name, str) or not job_name.strip():
                    issues.append(
                        ValidationIssue("error", f"partition_change.propagators[{i}].targets[{j}].job_name must be non-empty")
                    )
                    continue
                if job_name.strip() not in derived_job_names:
                    issues.append(
                        ValidationIssue(
                            "error",
                            f"partition_change.propagators[{i}].targets[{j}].job_name '{job_name.strip()}' not found",
                        )
                    )

    if require_file_exists and not orchestration_path.exists():
        issues.append(ValidationIssue("error", f"orchestration file not found: {orchestration_path}"))
    return issues


def validate_orchestration_structure(*, orchestration: dict[str, Any]) -> list[ValidationIssue]:
    issues: list[ValidationIssue] = []
    try:
        idx = index_orch(orchestration)
    except ValueError as e:
        issues.append(ValidationIssue("error", str(e)))
        return issues

    partitions = orchestration.get("partitions")
    if partitions is not None and not isinstance(partitions, dict):
        issues.append(ValidationIssue("error", "partitions must be a mapping"))
    if isinstance(partitions, dict):
        for p_type, models in partitions.items():
            if p_type not in {"daily", "unpartitioned"}:
                issues.append(ValidationIssue("error", f"Unsupported partition type '{p_type}'"))
                continue
            if not isinstance(models, list):
                issues.append(ValidationIssue("error", f"partitions.{p_type} must be a list"))
                continue
            for m in models:
                if not isinstance(m, str) or not m.strip():
                    issues.append(ValidationIssue("error", f"partitions.{p_type} contains empty model"))

    jobs = orchestration.get("jobs")
    if jobs is not None and not isinstance(jobs, dict):
        issues.append(ValidationIssue("error", "jobs must be a mapping"))
    if isinstance(jobs, dict):
        for job_name, cfg in jobs.items():
            if not isinstance(job_name, str) or not job_name.strip():
                issues.append(ValidationIssue("error", "jobs contains an empty/non-string key"))
                continue
            if not isinstance(cfg, dict):
                issues.append(ValidationIssue("error", f"jobs.{job_name} must be a mapping"))
                continue
            models = cfg.get("models")
            if not isinstance(models, list) or not models:
                issues.append(ValidationIssue("error", f"jobs.{job_name}.models must be a non-empty list"))
            partitions_value = cfg.get("partitions")
            if partitions_value is not None and partitions_value not in {"daily", "unpartitioned"}:
                issues.append(ValidationIssue("error", f"jobs.{job_name}.partitions must be daily|unpartitioned when set"))

    schedules = orchestration.get("schedules")
    if schedules is not None and not isinstance(schedules, dict):
        issues.append(ValidationIssue("error", "schedules must be a mapping"))
    if isinstance(schedules, dict):
        for name, cfg in schedules.items():
            if not isinstance(name, str) or not name.strip():
                issues.append(ValidationIssue("error", "schedules contains an empty/non-string key"))
                continue
            if not isinstance(cfg, dict):
                issues.append(ValidationIssue("error", f"schedules.{name} must be a mapping"))
                continue
            if cfg.get("type") != "daily_at":
                issues.append(ValidationIssue("error", f"schedules.{name}.type must be 'daily_at'"))
            job_name = cfg.get("job_name")
            if not isinstance(job_name, str) or not job_name.strip():
                issues.append(ValidationIssue("error", f"schedules.{name}.job_name must be non-empty"))

    pc = orchestration.get("partition_change")
    if pc is not None and not isinstance(pc, dict):
        issues.append(ValidationIssue("error", "partition_change must be a mapping"))
        return issues
    detectors = pc.get("detectors") if isinstance(pc, dict) else None
    if detectors is not None and not isinstance(detectors, list):
        issues.append(ValidationIssue("error", "partition_change.detectors must be a list"))
    propagators = pc.get("propagators") if isinstance(pc, dict) else None
    if propagators is not None and not isinstance(propagators, list):
        issues.append(ValidationIssue("error", "partition_change.propagators must be a list"))

    _ = idx
    return issues


def save_orchestration_with_validation(
    *,
    target: Path,
    data: dict[str, Any],
    dbt_project_dir: Path,
    prepare: bool,
) -> None:
    issues = validate_orchestration_structure(orchestration=data)

    mp = manifest_path(dbt_project_dir)
    if prepare or mp.exists():
        dbt_target = os.getenv("DBT_TARGET") or os.getenv("LUBAN_DEFAULT_DBT_TARGET") or "development"
        manifest = load_manifest(
            dbt_project_dir=dbt_project_dir,
            dbt_profiles_dir=dbt_project_dir,
            dbt_target=dbt_target,
            prepare=prepare,
        )
        issues.extend(
            validate_orchestration(
                manifest=manifest,
                orchestration=data,
                require_file_exists=False,
                orchestration_path=target,
            )
        )

    errors = [i for i in issues if i.level == "error"]
    if errors:
        for issue in issues:
            prefix = "ERROR" if issue.level == "error" else "WARN"
            click.echo(f"{prefix}: {issue.message}")
        raise click.ClickException(f"Validation failed with {len(errors)} error(s)")

    save_orch(target, data)

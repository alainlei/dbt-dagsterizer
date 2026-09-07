from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import click

from ..dbt.manifest_prepare import load_manifest, manifest_path
from ..orchestration_config import index as index_orch
from ..orchestration_config import normalize_timezone
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

    daily_models = sorted(
        [
            model
            for model, p_type in idx.partitions_by_model.items()
            if model in existing_models and p_type == "daily"
        ]
    )
    tags_by_model: dict[str, set[str]] = {}
    if daily_models:
        nodes = manifest.get("nodes")
        if isinstance(nodes, dict):
            for props in nodes.values():
                if not isinstance(props, dict) or props.get("resource_type") != "model":
                    continue
                name = props.get("name")
                if not isinstance(name, str) or not name.strip():
                    continue
                tags = props.get("tags") or []
                if isinstance(tags, list):
                    tags_by_model[name.strip()] = {
                        str(t) for t in tags if isinstance(t, str) and t.strip()
                    }

    for model, p_type in sorted(idx.partitions_by_model.items()):
        if model not in existing_models:
            issues.append(ValidationIssue("error", f"partitions references missing model '{model}'"))
        # Allow daily, hourly, monthly, or unpartitioned
        if p_type not in {"daily", "hourly", "monthly", "unpartitioned"}:
            issues.append(ValidationIssue("error", f"partitions for model '{model}' must be daily|hourly|monthly|unpartitioned"))
        if model in existing_models and p_type in {"daily", "hourly", "monthly"} and "materialize_at_startup" in tags_by_model.get(
            model, set()
        ):
            issues.append(
                ValidationIssue(
                    "warn",
                    f"model '{model}' is {p_type}-partitioned and tagged materialize_at_startup; AutomationCondition.missing() may trigger multiple missing partitions",
                )
            )

    # Validate daily_config.include_current_day_partition
    partitions_data = orchestration.get("partitions")
    if isinstance(partitions_data, dict):
        daily_config = partitions_data.get("daily_config")
        if daily_config is not None:
            if not isinstance(daily_config, dict):
                issues.append(ValidationIssue("error", "partitions.daily_config must be a mapping"))
            else:
                include_current_day_partition = daily_config.get("include_current_day_partition")
                if include_current_day_partition is not None and not isinstance(include_current_day_partition, bool):
                    issues.append(ValidationIssue("error", "partitions.daily_config.include_current_day_partition must be a boolean"))

        # Validate hourly_config.include_current_hour_partition
        hourly_config = partitions_data.get("hourly_config")
        if hourly_config is not None:
            if not isinstance(hourly_config, dict):
                issues.append(ValidationIssue("error", "partitions.hourly_config must be a mapping"))
            else:
                include_current_hour_partition = hourly_config.get("include_current_hour_partition")
                if include_current_hour_partition is not None and not isinstance(include_current_hour_partition, bool):
                    issues.append(ValidationIssue("error", "partitions.hourly_config.include_current_hour_partition must be a boolean"))

        # Validate monthly_config.include_current_month_partition
        monthly_config = partitions_data.get("monthly_config")
        if monthly_config is not None:
            if not isinstance(monthly_config, dict):
                issues.append(ValidationIssue("error", "partitions.monthly_config must be a mapping"))
            else:
                include_current_month_partition = monthly_config.get("include_current_month_partition")
                if include_current_month_partition is not None and not isinstance(include_current_month_partition, bool):
                    issues.append(ValidationIssue("error", "partitions.monthly_config.include_current_month_partition must be a boolean"))

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
            if partitions is not None and partitions not in {"daily", "hourly", "monthly", "unpartitioned"}:
                issues.append(ValidationIssue("error", f"jobs.{job_name}.partitions must be daily|hourly|monthly|unpartitioned when set"))
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
            if schedule_cfg.get("type") not in {"daily_at", "hourly_at", "monthly_at"}:
                issues.append(ValidationIssue("error", f"schedules.{name}.type must be 'daily_at', 'hourly_at' or 'monthly_at'"))
            job_name = schedule_cfg.get("job_name")
            if not isinstance(job_name, str) or not job_name.strip():
                issues.append(ValidationIssue("error", f"schedules.{name}.job_name must be non-empty"))
            elif job_name.strip() not in derived_job_names:
                issues.append(ValidationIssue("error", f"schedules.{name}.job_name '{job_name.strip()}' not found"))

            schedule_type = schedule_cfg.get("type")
            hour = schedule_cfg.get("hour")
            minute = schedule_cfg.get("minute")
            if not isinstance(minute, int) or minute < 0 or minute > 59:
                issues.append(ValidationIssue("error", f"schedules.{name}.minute must be 0..59"))

            if schedule_type == "daily_at":
                if not isinstance(hour, int) or hour < 0 or hour > 23:
                    issues.append(ValidationIssue("error", f"schedules.{name}.hour must be 0..23 (required for daily_at)"))
                lookback_hours = schedule_cfg.get("lookback_hours")
                offset_hours = schedule_cfg.get("offset_hours")
                if lookback_hours not in (None, 0):
                    issues.append(ValidationIssue(
                        "error",
                        f"schedules.{name}: daily_at schedule cannot set lookback_hours (use lookback_days)"
                    ))
                if offset_hours not in (None, 0):
                    issues.append(ValidationIssue(
                        "error",
                        f"schedules.{name}: daily_at schedule cannot set offset_hours (use offset_days)"
                    ))
                lookback_months = schedule_cfg.get("lookback_months")
                offset_months = schedule_cfg.get("offset_months")
                if lookback_months not in (None, 0):
                    issues.append(ValidationIssue(
                        "error",
                        f"schedules.{name}: daily_at schedule cannot set lookback_months (use lookback_days)"
                    ))
                if offset_months not in (None, 0):
                    issues.append(ValidationIssue(
                        "error",
                        f"schedules.{name}: daily_at schedule cannot set offset_months (use offset_days)"
                    ))
                lookback_days = schedule_cfg.get("lookback_days", 0)
                if not isinstance(lookback_days, int) or lookback_days < 0:
                    issues.append(ValidationIssue("error", f"schedules.{name}.lookback_days must be >= 0"))
                offset_days = schedule_cfg.get("offset_days", 1)
                if not isinstance(offset_days, int) or offset_days < 0:
                    issues.append(ValidationIssue("error", f"schedules.{name}.offset_days must be >= 0"))
            elif schedule_type == "hourly_at":
                if hour not in (None, 0):
                    issues.append(ValidationIssue(
                        "error",
                        f"schedules.{name}: hourly_at schedule cannot set hour (run at the same minute every hour)"
                    ))
                lookback_days = schedule_cfg.get("lookback_days")
                offset_days = schedule_cfg.get("offset_days")
                if lookback_days not in (None, 0):
                    issues.append(ValidationIssue(
                        "error",
                        f"schedules.{name}: hourly_at schedule cannot set lookback_days (use lookback_hours)"
                    ))
                if offset_days not in (None, 0):
                    issues.append(ValidationIssue(
                        "error",
                        f"schedules.{name}: hourly_at schedule cannot set offset_days (use offset_hours)"
                    ))
                lookback_months = schedule_cfg.get("lookback_months")
                offset_months = schedule_cfg.get("offset_months")
                if lookback_months not in (None, 0):
                    issues.append(ValidationIssue(
                        "error",
                        f"schedules.{name}: hourly_at schedule cannot set lookback_months (use lookback_hours)"
                    ))
                if offset_months not in (None, 0):
                    issues.append(ValidationIssue(
                        "error",
                        f"schedules.{name}: hourly_at schedule cannot set offset_months (use offset_hours)"
                    ))
                lookback_hours = schedule_cfg.get("lookback_hours", 0)
                if not isinstance(lookback_hours, int) or lookback_hours < 0:
                    issues.append(ValidationIssue("error", f"schedules.{name}.lookback_hours must be >= 0"))
                offset_hours = schedule_cfg.get("offset_hours", 1)
                if not isinstance(offset_hours, int) or offset_hours < 0:
                    issues.append(ValidationIssue("error", f"schedules.{name}.offset_hours must be >= 0"))
            elif schedule_type == "monthly_at":
                if not isinstance(hour, int) or hour < 0 or hour > 23:
                    issues.append(ValidationIssue("error", f"schedules.{name}.hour must be 0..23 (required for monthly_at)"))
                # 29-31 would silently skip shorter months, stalling the schedule.
                day_of_month = schedule_cfg.get("day_of_month", 1)
                if not isinstance(day_of_month, int) or day_of_month < 1 or day_of_month > 28:
                    issues.append(ValidationIssue("error", f"schedules.{name}.day_of_month must be 1..28"))
                lookback_days = schedule_cfg.get("lookback_days")
                offset_days = schedule_cfg.get("offset_days")
                if lookback_days not in (None, 0):
                    issues.append(ValidationIssue(
                        "error",
                        f"schedules.{name}: monthly_at schedule cannot set lookback_days (use lookback_months)"
                    ))
                if offset_days not in (None, 0):
                    issues.append(ValidationIssue(
                        "error",
                        f"schedules.{name}: monthly_at schedule cannot set offset_days (use offset_months)"
                    ))
                lookback_hours = schedule_cfg.get("lookback_hours")
                offset_hours = schedule_cfg.get("offset_hours")
                if lookback_hours not in (None, 0):
                    issues.append(ValidationIssue(
                        "error",
                        f"schedules.{name}: monthly_at schedule cannot set lookback_hours (use lookback_months)"
                    ))
                if offset_hours not in (None, 0):
                    issues.append(ValidationIssue(
                        "error",
                        f"schedules.{name}: monthly_at schedule cannot set offset_hours (use offset_months)"
                    ))
                lookback_months = schedule_cfg.get("lookback_months", 0)
                if not isinstance(lookback_months, int) or lookback_months < 0:
                    issues.append(ValidationIssue("error", f"schedules.{name}.lookback_months must be >= 0"))
                offset_months = schedule_cfg.get("offset_months", 1)
                if not isinstance(offset_months, int) or offset_months < 0:
                    issues.append(ValidationIssue("error", f"schedules.{name}.offset_months must be >= 0"))

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

    repl = orchestration.get("replication")
    if repl is not None and not isinstance(repl, dict):
        issues.append(ValidationIssue("error", "replication must be a mapping"))
        return issues
    entries = repl.get("entries") if isinstance(repl, dict) else None
    if entries is not None and not isinstance(entries, list):
        issues.append(ValidationIssue("error", "replication.entries must be a list"))
    if isinstance(entries, list):
        for i, e in enumerate(entries):
            if not isinstance(e, dict):
                issues.append(ValidationIssue("error", f"replication.entries[{i}] must be a mapping"))
                continue
            model = e.get("model")
            if not isinstance(model, str) or not model.strip():
                issues.append(ValidationIssue("error", f"replication.entries[{i}].model must be non-empty"))
                continue
            if model.strip() not in existing_models:
                issues.append(
                    ValidationIssue("error", f"replication.entries[{i}] references missing model '{model.strip()}'")
                )
            write_disposition = e.get("write_disposition")
            if write_disposition is not None and write_disposition not in {"append", "replace", "merge"}:
                issues.append(
                    ValidationIssue(
                        "error",
                        f"replication.entries[{i}].write_disposition must be 'append', 'replace', or 'merge'",
                    )
                )
            partition_column = e.get("partition_column")
            if partition_column is not None:
                if not isinstance(partition_column, str) or not partition_column.strip():
                    issues.append(
                        ValidationIssue("error", f"replication.entries[{i}].partition_column must be non-empty when set")
                    )
            p_type = idx.partitions_by_model.get(model.strip())
            if p_type and p_type != "unpartitioned" and not partition_column:
                issues.append(
                    ValidationIssue(
                        "warn",
                        f"replication.entries[{i}] on partitioned model '{model.strip()}' has no partition_column; full table copy will be used",
                    )
                )

    reports = orchestration.get("ssrs_reports")
    if reports is not None and not isinstance(reports, list):
        issues.append(ValidationIssue("error", "ssrs_reports must be a list"))
    if isinstance(reports, list):
        seen_report_names: set[str] = set()
        for i, r in enumerate(reports):
            if not isinstance(r, dict):
                issues.append(ValidationIssue("error", f"ssrs_reports[{i}] must be a mapping"))
                continue
            name = r.get("name")
            if not isinstance(name, str) or not name.strip():
                issues.append(ValidationIssue("error", f"ssrs_reports[{i}].name must be non-empty"))
            else:
                if name.strip() in seen_report_names:
                    issues.append(ValidationIssue("error", f"ssrs_reports[{i}] duplicates name '{name.strip()}'"))
                seen_report_names.add(name.strip())
            model = r.get("model")
            if not isinstance(model, str) or not model.strip():
                issues.append(ValidationIssue("error", f"ssrs_reports[{i}].model must be non-empty"))
            elif model.strip() not in existing_models:
                issues.append(
                    ValidationIssue("error", f"ssrs_reports[{i}] references missing model '{model.strip()}'")
                )
            subscription_description = r.get("subscription_description")
            if not isinstance(subscription_description, str) or not subscription_description.strip():
                issues.append(
                    ValidationIssue("error", f"ssrs_reports[{i}].subscription_description must be non-empty")
                )
            enabled = r.get("enabled")
            if enabled is not None and not isinstance(enabled, bool):
                issues.append(ValidationIssue("error", f"ssrs_reports[{i}].enabled must be boolean when set"))

    if require_file_exists and not orchestration_path.exists():
        issues.append(ValidationIssue("error", f"orchestration file not found: {orchestration_path}"))
    return issues


def validate_orchestration_structure(*, orchestration: dict[str, Any]) -> list[ValidationIssue]:
    issues: list[ValidationIssue] = []

    # Validate timezone
    raw_tz = orchestration.get("timezone")
    if raw_tz is not None:
        try:
            normalize_timezone(raw_tz, default="UTC")
        except ValueError as exc:
            issues.append(ValidationIssue("error", str(exc)))

    try:
        idx = index_orch(orchestration)
    except ValueError as e:
        issues.append(ValidationIssue("error", str(e)))
        return issues

    partitions = orchestration.get("partitions")
    if partitions is not None and not isinstance(partitions, dict):
        issues.append(ValidationIssue("error", "partitions must be a mapping"))
    if isinstance(partitions, dict):
        # Validate daily/hourly/monthly/unpartitioned partitions
        for p_type in {"daily", "hourly", "monthly", "unpartitioned"}:
            models = partitions.get(p_type)
            if isinstance(models, list):
                for m in models:
                    if not isinstance(m, str) or not m.strip():
                        issues.append(ValidationIssue("error", f"partitions.{p_type} contains empty model"))

        # Validate daily_config
        daily_config = partitions.get("daily_config")
        if daily_config is not None:
            if not isinstance(daily_config, dict):
                issues.append(ValidationIssue("error", "partitions.daily_config must be a mapping"))
            else:
                include_current_day_partition = daily_config.get("include_current_day_partition")
                if include_current_day_partition is not None and not isinstance(include_current_day_partition, bool):
                    issues.append(ValidationIssue("error", "partitions.daily_config.include_current_day_partition must be a boolean"))

        # Validate hourly_config
        hourly_config = partitions.get("hourly_config")
        if hourly_config is not None:
            if not isinstance(hourly_config, dict):
                issues.append(ValidationIssue("error", "partitions.hourly_config must be a mapping"))
            else:
                include_current_hour_partition = hourly_config.get("include_current_hour_partition")
                if include_current_hour_partition is not None and not isinstance(include_current_hour_partition, bool):
                    issues.append(ValidationIssue("error", "partitions.hourly_config.include_current_hour_partition must be a boolean"))

        # Validate monthly_config
        monthly_config = partitions.get("monthly_config")
        if monthly_config is not None:
            if not isinstance(monthly_config, dict):
                issues.append(ValidationIssue("error", "partitions.monthly_config must be a mapping"))
            else:
                include_current_month_partition = monthly_config.get("include_current_month_partition")
                if include_current_month_partition is not None and not isinstance(include_current_month_partition, bool):
                    issues.append(ValidationIssue("error", "partitions.monthly_config.include_current_month_partition must be a boolean"))

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
            if partitions_value is not None and partitions_value not in {"daily", "hourly", "monthly", "unpartitioned"}:
                issues.append(ValidationIssue("error", f"jobs.{job_name}.partitions must be daily|hourly|monthly|unpartitioned when set"))

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
            if cfg.get("type") not in {"daily_at", "hourly_at", "monthly_at"}:
                issues.append(ValidationIssue("error", f"schedules.{name}.type must be 'daily_at', 'hourly_at' or 'monthly_at'"))
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

    repl = orchestration.get("replication")
    if repl is not None and not isinstance(repl, dict):
        issues.append(ValidationIssue("error", "replication must be a mapping"))
    else:
        if isinstance(repl, dict):
            enabled = repl.get("enabled")
            if enabled is not None and not isinstance(enabled, bool):
                issues.append(ValidationIssue("error", "replication.enabled must be a boolean"))
            entries = repl.get("entries")
            if entries is not None and not isinstance(entries, list):
                issues.append(ValidationIssue("error", "replication.entries must be a list"))
            if isinstance(entries, list):
                for i, e in enumerate(entries):
                    if not isinstance(e, dict):
                        issues.append(ValidationIssue("error", f"replication.entries[{i}] must be a mapping"))
                        continue
                    model = e.get("model")
                    if not isinstance(model, str) or not model.strip():
                        issues.append(ValidationIssue("error", f"replication.entries[{i}].model must be non-empty"))
                    write_disposition = e.get("write_disposition")
                    if write_disposition is not None and write_disposition not in {"append", "replace", "merge"}:
                        issues.append(
                            ValidationIssue(
                                "error",
                                f"replication.entries[{i}].write_disposition must be 'append', 'replace', or 'merge'",
                            )
                        )

    reports = orchestration.get("ssrs_reports")
    if reports is not None and not isinstance(reports, list):
        issues.append(ValidationIssue("error", "ssrs_reports must be a list"))
    if isinstance(reports, list):
        for i, r in enumerate(reports):
            if not isinstance(r, dict):
                issues.append(ValidationIssue("error", f"ssrs_reports[{i}] must be a mapping"))
                continue
            name = r.get("name")
            if not isinstance(name, str) or not name.strip():
                issues.append(ValidationIssue("error", f"ssrs_reports[{i}].name must be non-empty"))
            model = r.get("model")
            if not isinstance(model, str) or not model.strip():
                issues.append(ValidationIssue("error", f"ssrs_reports[{i}].model must be non-empty"))
            subscription_description = r.get("subscription_description")
            if not isinstance(subscription_description, str) or not subscription_description.strip():
                issues.append(
                    ValidationIssue("error", f"ssrs_reports[{i}].subscription_description must be non-empty")
                )

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

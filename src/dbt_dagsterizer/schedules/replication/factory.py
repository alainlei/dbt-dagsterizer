"""Build Dagster schedule definitions for replication jobs."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import dagster as dg

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


def build_replication_schedules(schedule_specs: list[dict]) -> list:
    """Build Dagster schedules for replication jobs."""
    if not schedule_specs:
        return []

    dbt_project_dir = get_dbt_project_dir()
    cfg_path = resolve_orchestration_path(
        dbt_project_dir=dbt_project_dir,
        path_=Path(default_orchestration_path(dbt_project_dir=dbt_project_dir).name),
    )
    cfg = load_orch(cfg_path)
    idx = index_orch(cfg)
    dynamic_partitions_defs = get_dynamic_partitions_defs(dbt_project_dir)

    # Build a map of job names to job definitions
    from ...jobs.replication import get_replication_jobs_by_name
    jobs_by_name = get_replication_jobs_by_name()

    schedules: list = []
    for spec in schedule_specs:
        job_name = spec["job_name"]
        schedule_name = spec["name"]
        cron_schedule = spec["cron_schedule"]
        partition_type = spec.get("partition_type", "unpartitioned")
        enabled = spec.get("enabled", True)

        if job_name not in jobs_by_name:
            raise ValueError(f"Replication schedule '{schedule_name}' references unknown job '{job_name}'")

        job = jobs_by_name[job_name]
        partitions_def = get_partitions_def(
            partition_type,
            dynamic_partitions_defs=dynamic_partitions_defs,
            include_current_day_partition=idx.daily_include_current_day_partition,
        )

        default_status = dg.DefaultScheduleStatus.RUNNING if enabled else dg.DefaultScheduleStatus.STOPPED

        # Build schedule based on partition type
        if partition_type == "unpartitioned":
            @dg.schedule(
                name=schedule_name,
                cron_schedule=cron_schedule,
                job=job,
                default_status=default_status,
            )
            def _unpartitioned_schedule(context):
                return dg.RunRequest()

            schedules.append(_unpartitioned_schedule)

        elif partition_type == "daily":
            @dg.schedule(
                name=schedule_name,
                cron_schedule=cron_schedule,
                job=job,
                partitions_def=partitions_def,
                default_status=default_status,
            )
            def _daily_schedule(context):
                scheduled_time = context.scheduled_execution_time or datetime.now(timezone.utc)
                partition_date = (scheduled_time - timedelta(days=1)).date().isoformat()
                return dg.RunRequest(partition_key=partition_date)

            schedules.append(_daily_schedule)

        else:
            # Dynamic partitions
            @dg.schedule(
                name=schedule_name,
                cron_schedule=cron_schedule,
                job=job,
                partitions_def=partitions_def,
                default_status=default_status,
            )
            def _dynamic_schedule(context):
                scheduled_time = context.scheduled_execution_time or datetime.now(timezone.utc)
                # Run for latest partition
                return dg.RunRequest(partition_key=scheduled_time.date().isoformat())

            schedules.append(_dynamic_schedule)

    return schedules

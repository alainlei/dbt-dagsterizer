"""Build Dagster schedule definitions for replication jobs."""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

import dagster as dg

logger = logging.getLogger(__name__)


def build_replication_schedules(schedule_specs: list[dict]) -> list:
    """Build Dagster schedules for replication jobs."""
    if not schedule_specs:
        return []

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
                default_status=default_status,
            )
            def _daily_schedule(context):
                scheduled_time = context.scheduled_execution_time or datetime.now(timezone.utc)
                partition_date = (scheduled_time - timedelta(days=1)).date().isoformat()
                return dg.RunRequest(partition_key=partition_date)

            schedules.append(_daily_schedule)

        else:
            raise ValueError(f"Unsupported partition_type: {partition_type}")

    return schedules

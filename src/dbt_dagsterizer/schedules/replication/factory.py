"""Build Dagster schedule definitions for replication jobs."""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

import dagster as dg

from ...orchestration_config import normalize_timezone
from ...partitions import add_months, month_floor

logger = logging.getLogger(__name__)


def build_replication_schedules(schedule_specs: list[dict]) -> list:
    """Build Dagster schedules for replication jobs."""
    if not schedule_specs:
        return []

    from ...jobs.replication import get_replication_jobs_by_name
    jobs_by_name = get_replication_jobs_by_name()

    schedules: list = []
    for spec in schedule_specs:
        job_name = spec["job_name"]
        schedule_name = spec["name"]
        cron_schedule = spec["cron_schedule"]
        partition_type = spec.get("partition_type", "unpartitioned")
        enabled = spec.get("enabled", True)
        execution_timezone = normalize_timezone(spec.get("timezone"), default="UTC")
        offset_hours = int(spec.get("partition_offset_hours", spec.get("offset_hours", 1)))
        lookback_hours = int(spec.get("partition_lookback_hours", spec.get("lookback_hours", 0)))
        offset_days = int(spec.get("partition_offset_days", spec.get("offset_days", 1)))
        lookback_days = int(spec.get("partition_lookback_days", spec.get("lookback_days", 0)))
        offset_months = int(spec.get("partition_offset_months", spec.get("offset_months", 1)))
        lookback_months = int(spec.get("partition_lookback_months", spec.get("lookback_months", 0)))

        if job_name not in jobs_by_name:
            raise ValueError(f"Replication schedule '{schedule_name}' references unknown job '{job_name}'")

        job = jobs_by_name[job_name]

        default_status = dg.DefaultScheduleStatus.RUNNING if enabled else dg.DefaultScheduleStatus.STOPPED

        if partition_type == "unpartitioned":
            @dg.schedule(
                name=schedule_name,
                cron_schedule=cron_schedule,
                job=job,
                default_status=default_status,
                execution_timezone=execution_timezone,
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
                execution_timezone=execution_timezone,
            )
            def _daily_schedule(context):
                scheduled_time = context.scheduled_execution_time or datetime.now(timezone.utc)
                anchor_day = (scheduled_time - timedelta(days=offset_days)).date()
                run_requests = []
                for i in range(lookback_days + 1):
                    partition_day = (anchor_day - timedelta(days=i)).isoformat()
                    run_requests.append(dg.RunRequest(partition_key=partition_day))
                return run_requests

            schedules.append(_daily_schedule)

        elif partition_type == "hourly":
            @dg.schedule(
                name=schedule_name,
                cron_schedule=cron_schedule,
                job=job,
                default_status=default_status,
                execution_timezone=execution_timezone,
            )
            def _hourly_schedule(context):
                scheduled_time = context.scheduled_execution_time or datetime.now(timezone.utc)
                anchor_hour = (scheduled_time - timedelta(hours=offset_hours)).replace(minute=0, second=0, microsecond=0)
                run_requests = []
                for i in range(lookback_hours + 1):
                    partition_time = anchor_hour - timedelta(hours=i)
                    partition_key = partition_time.strftime("%Y-%m-%d-%H:00")
                    run_requests.append(dg.RunRequest(partition_key=partition_key))
                return run_requests

            schedules.append(_hourly_schedule)

        elif partition_type == "monthly":
            @dg.schedule(
                name=schedule_name,
                cron_schedule=cron_schedule,
                job=job,
                default_status=default_status,
                execution_timezone=execution_timezone,
            )
            def _monthly_schedule(context):
                scheduled_time = context.scheduled_execution_time or datetime.now(timezone.utc)
                # Dagster MonthlyPartitionsDefinition keys are "YYYY-MM-01"
                anchor_month = add_months(month_floor(scheduled_time.date()), -offset_months)
                run_requests = []
                for i in range(lookback_months + 1):
                    partition_key = add_months(anchor_month, -i).isoformat()
                    run_requests.append(dg.RunRequest(partition_key=partition_key))
                return run_requests

            schedules.append(_monthly_schedule)

        else:
            raise ValueError(f"Unsupported partition_type: {partition_type}")

    return schedules

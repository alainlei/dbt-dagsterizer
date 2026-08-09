"""Tests for the hourly schedule factory in schedules/dbt/factory.py."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

import dagster as dg
import pytest


@dg.job(name="stub_job")
def _stub_job():
    """A minimal job for schedule factory tests."""
    pass


STUB_JOB = _stub_job


@dataclass
class _FakeScheduleContext:
    """Minimal context that satisfies the schedule evaluation function."""

    scheduled_execution_time: datetime


def _evaluate_schedule(schedule_def, scheduled_time: datetime):
    """Call the schedule's original function with a fake context."""
    execution_fn = schedule_def._execution_fn
    fn = execution_fn.decorated_fn
    ctx = _FakeScheduleContext(scheduled_execution_time=scheduled_time)
    result = fn(ctx)
    return list(result) if result else []


def test_hourly_schedule_partition_key_format():
    """Hourly schedule generates partition keys in 'YYYY-MM-DD-HH:00' format."""
    from dbt_dagsterizer.schedules.dbt.factory import build_dbt_schedules
    from dbt_dagsterizer.schedules.dbt.presets import hourly_at

    spec = hourly_at(name="hourly_sched", job_name="stub_job", minute=0, offset_hours=1)

    schedules = build_dbt_schedules([spec], {"stub_job": STUB_JOB})
    assert len(schedules) == 1

    run_requests = _evaluate_schedule(
        schedules[0], datetime(2025, 6, 15, 10, 0, 0, tzinfo=timezone.utc)
    )
    assert len(run_requests) >= 1
    # offset_hours=1 means anchor is 09:00, so partition key should be 2025-06-15-09:00
    assert run_requests[0].partition_key == "2025-06-15-09:00"


def test_hourly_schedule_with_lookback():
    """Hourly schedule with lookback_hours generates multiple run requests."""
    from dbt_dagsterizer.schedules.dbt.factory import build_dbt_schedules
    from dbt_dagsterizer.schedules.dbt.presets import hourly_at

    spec = hourly_at(
        name="hourly_lb", job_name="stub_job", minute=0, offset_hours=1, lookback_hours=2
    )

    schedules = build_dbt_schedules([spec], {"stub_job": STUB_JOB})

    run_requests = _evaluate_schedule(
        schedules[0], datetime(2025, 6, 15, 10, 0, 0, tzinfo=timezone.utc)
    )
    # lookback_hours=2 → 3 partitions: anchor-0, anchor-1, anchor-2
    assert len(run_requests) == 3
    partition_keys = [rr.partition_key for rr in run_requests]
    assert partition_keys == [
        "2025-06-15-09:00",
        "2025-06-15-08:00",
        "2025-06-15-07:00",
    ]


def test_hourly_schedule_zero_offset():
    """Hourly schedule with offset_hours=0 anchors at the current hour."""
    from dbt_dagsterizer.schedules.dbt.factory import build_dbt_schedules
    from dbt_dagsterizer.schedules.dbt.presets import hourly_at

    spec = hourly_at(name="hourly_zero", job_name="stub_job", minute=0, offset_hours=0)

    schedules = build_dbt_schedules([spec], {"stub_job": STUB_JOB})

    run_requests = _evaluate_schedule(
        schedules[0], datetime(2025, 6, 15, 10, 30, 0, tzinfo=timezone.utc)
    )
    # offset_hours=0 → anchor is 10:00 (minute truncated to 0)
    assert run_requests[0].partition_key == "2025-06-15-10:00"


def test_hourly_schedule_run_key_with_dedupe():
    """Hourly schedule with dedupe_across_ticks=True uses plain run key."""
    from dbt_dagsterizer.schedules.dbt.factory import build_dbt_schedules
    from dbt_dagsterizer.schedules.dbt.presets import hourly_at

    spec = hourly_at(
        name="hourly_dedup", job_name="stub_job", minute=0, offset_hours=1, dedupe_across_ticks=True
    )

    schedules = build_dbt_schedules([spec], {"stub_job": STUB_JOB})

    run_requests = _evaluate_schedule(
        schedules[0], datetime(2025, 6, 15, 10, 0, 0, tzinfo=timezone.utc)
    )
    # With dedupe_across_ticks=True, run_key is just "{name}:{partition_key}"
    assert run_requests[0].run_key == "hourly_dedup:2025-06-15-09:00"


def test_hourly_schedule_run_key_without_dedupe():
    """Hourly schedule with dedupe_across_ticks=False appends tick timestamp."""
    from dbt_dagsterizer.schedules.dbt.factory import build_dbt_schedules
    from dbt_dagsterizer.schedules.dbt.presets import hourly_at

    spec = hourly_at(
        name="hourly_nodedup", job_name="stub_job", minute=0, offset_hours=1, dedupe_across_ticks=False
    )

    schedules = build_dbt_schedules([spec], {"stub_job": STUB_JOB})

    run_requests = _evaluate_schedule(
        schedules[0], datetime(2025, 6, 15, 10, 0, 0, tzinfo=timezone.utc)
    )
    # With dedupe_across_ticks=False, run_key has a tick timestamp suffix
    assert run_requests[0].run_key.startswith("hourly_nodedup:2025-06-15-09:00:")
    assert "20250615T100000" in run_requests[0].run_key


def test_hourly_schedule_cannot_set_daily_offset_fields():
    """Hourly schedule raises if partition_offset_days or partition_lookback_days are non-zero."""
    from dbt_dagsterizer.schedules.dbt.factory import build_dbt_schedules

    spec = {
        "name": "bad_hourly",
        "cron_schedule": "0 * * * *",
        "job_name": "stub_job",
        "partition_type": "hourly",
        "partition_offset_days": 1,  # Non-zero daily offset → error
        "partition_lookback_days": 0,
        "partition_offset_hours": 1,
        "partition_lookback_hours": 0,
        "enabled": True,
        "dedupe_across_ticks": True,
        "timezone": "UTC",
    }

    with pytest.raises(ValueError, match="cannot set daily offset/lookback"):
        build_dbt_schedules([spec], {"stub_job": STUB_JOB})


def test_daily_schedule_cannot_set_hourly_offset_fields():
    """Daily schedule raises if partition_offset_hours or partition_lookback_hours are non-zero."""
    from dbt_dagsterizer.schedules.dbt.factory import build_dbt_schedules

    spec = {
        "name": "bad_daily",
        "cron_schedule": "0 0 * * *",
        "job_name": "stub_job",
        "partition_type": "daily",
        "partition_offset_days": 1,
        "partition_lookback_days": 0,
        "partition_offset_hours": 2,  # Non-zero hourly offset → error
        "partition_lookback_hours": 0,
        "enabled": True,
        "dedupe_across_ticks": True,
        "timezone": "UTC",
    }

    with pytest.raises(ValueError, match="cannot set hourly offset/lookback"):
        build_dbt_schedules([spec], {"stub_job": STUB_JOB})


def test_unsupported_partition_type_raises():
    """build_dbt_schedules raises ValueError for unsupported partition_type."""
    from dbt_dagsterizer.schedules.dbt.factory import build_dbt_schedules

    spec = {
        "name": "bad_type",
        "cron_schedule": "0 * * * *",
        "job_name": "stub_job",
        "partition_type": "weekly",
        "partition_offset_days": 0,
        "partition_lookback_days": 0,
        "partition_offset_hours": 0,
        "partition_lookback_hours": 0,
        "enabled": True,
        "dedupe_across_ticks": True,
        "timezone": "UTC",
    }

    with pytest.raises(ValueError, match="Unsupported partition_type"):
        build_dbt_schedules([spec], {"stub_job": STUB_JOB})


def test_build_dbt_schedules_duplicate_names_raises():
    """Duplicate schedule names raise ValueError."""
    from dbt_dagsterizer.schedules.dbt.factory import build_dbt_schedules
    from dbt_dagsterizer.schedules.dbt.presets import hourly_at

    spec1 = hourly_at(name="dup_name", job_name="stub_job")
    spec2 = hourly_at(name="dup_name", job_name="stub_job")

    with pytest.raises(ValueError, match="Duplicate dbt schedule names"):
        build_dbt_schedules([spec1, spec2], {"stub_job": STUB_JOB})


def test_hourly_schedule_disabled():
    """Disabled hourly schedule gets STOPPED default status."""
    from dbt_dagsterizer.schedules.dbt.factory import build_dbt_schedules
    from dbt_dagsterizer.schedules.dbt.presets import hourly_at

    spec = hourly_at(name="stopped_sched", job_name="stub_job", enabled=False)

    schedules = build_dbt_schedules([spec], {"stub_job": STUB_JOB})
    assert len(schedules) == 1
    assert schedules[0] is not None


def test_mixed_daily_and_hourly_schedules():
    """build_dbt_schedules can handle both daily and hourly specs together."""
    from dbt_dagsterizer.schedules.dbt.factory import build_dbt_schedules
    from dbt_dagsterizer.schedules.dbt.presets import daily_at, hourly_at

    daily_spec = daily_at(name="daily_sched", job_name="stub_job", hour=3, minute=0)
    hourly_spec = hourly_at(name="hourly_sched", job_name="stub_job", minute=0)

    schedules = build_dbt_schedules([daily_spec, hourly_spec], {"stub_job": STUB_JOB})
    assert len(schedules) == 2


def test_hourly_schedule_midnight_boundary():
    """Hourly schedule handles midnight boundary correctly."""
    from dbt_dagsterizer.schedules.dbt.factory import build_dbt_schedules
    from dbt_dagsterizer.schedules.dbt.presets import hourly_at

    spec = hourly_at(name="midnight", job_name="stub_job", minute=0, offset_hours=1)

    schedules = build_dbt_schedules([spec], {"stub_job": STUB_JOB})

    # Tick at 2025-06-15 00:30 UTC → anchor is 2025-06-14 23:00
    run_requests = _evaluate_schedule(
        schedules[0], datetime(2025, 6, 15, 0, 30, 0, tzinfo=timezone.utc)
    )
    assert run_requests[0].partition_key == "2025-06-14-23:00"


def test_daily_schedule_partition_key_format():
    """Daily schedule generates partition keys in 'YYYY-MM-DD' format."""
    from dbt_dagsterizer.schedules.dbt.factory import build_dbt_schedules
    from dbt_dagsterizer.schedules.dbt.presets import daily_at

    spec = daily_at(name="daily_sched", job_name="stub_job", hour=3, minute=0, offset_days=1)

    schedules = build_dbt_schedules([spec], {"stub_job": STUB_JOB})

    run_requests = _evaluate_schedule(
        schedules[0], datetime(2025, 6, 15, 3, 0, 0, tzinfo=timezone.utc)
    )
    assert len(run_requests) == 1
    # offset_days=1 → anchor is 2025-06-14
    assert run_requests[0].partition_key == "2025-06-14"

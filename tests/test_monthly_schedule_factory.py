"""Tests for the monthly schedule factory in schedules/dbt/factory.py."""

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


def test_monthly_schedule_partition_key_format():
    """Monthly schedule generates partition keys in 'YYYY-MM-01' format."""
    from dbt_dagsterizer.schedules.dbt.factory import build_dbt_schedules
    from dbt_dagsterizer.schedules.dbt.presets import monthly_at

    spec = monthly_at(name="monthly_sched", job_name="stub_job", hour=2, minute=30, offset_months=1)

    schedules = build_dbt_schedules([spec], {"stub_job": STUB_JOB})
    assert len(schedules) == 1

    run_requests = _evaluate_schedule(
        schedules[0], datetime(2026, 9, 1, 2, 30, 0, tzinfo=timezone.utc)
    )
    assert len(run_requests) == 1
    # offset_months=1 means anchor is August, so partition key should be 2026-08-01
    assert run_requests[0].partition_key == "2026-08-01"


def test_monthly_schedule_emits_keys_valid_for_monthly_partitions_def():
    """Every emitted key must exist in the MonthlyPartitionsDefinition the job uses."""
    from dbt_dagsterizer.schedules.dbt.factory import build_dbt_schedules
    from dbt_dagsterizer.schedules.dbt.presets import monthly_at

    partitions_def = dg.MonthlyPartitionsDefinition(start_date="2025-01-01", end_offset=1)

    @dg.job(name="monthly_job", partitions_def=partitions_def)
    def _monthly_job():
        pass

    spec = monthly_at(
        name="monthly_valid", job_name="monthly_job", hour=0, minute=0, offset_months=1,
        lookback_months=4,
    )
    schedules = build_dbt_schedules([spec], {"monthly_job": _monthly_job})

    valid_keys = set(partitions_def.get_partition_keys())
    run_requests = _evaluate_schedule(
        schedules[0], datetime(2026, 9, 1, 0, 0, 0, tzinfo=timezone.utc)
    )
    assert len(run_requests) == 5
    for rr in run_requests:
        assert rr.partition_key in valid_keys


def test_monthly_schedule_with_lookback():
    """Monthly schedule with lookback_months generates multiple run requests."""
    from dbt_dagsterizer.schedules.dbt.factory import build_dbt_schedules
    from dbt_dagsterizer.schedules.dbt.presets import monthly_at

    spec = monthly_at(
        name="monthly_lb", job_name="stub_job", hour=2, minute=30, offset_months=1, lookback_months=2
    )

    schedules = build_dbt_schedules([spec], {"stub_job": STUB_JOB})

    run_requests = _evaluate_schedule(
        schedules[0], datetime(2026, 9, 1, 2, 30, 0, tzinfo=timezone.utc)
    )
    # lookback_months=2 → 3 partitions: anchor-0, anchor-1, anchor-2
    assert len(run_requests) == 3
    partition_keys = [rr.partition_key for rr in run_requests]
    assert partition_keys == [
        "2026-08-01",
        "2026-07-01",
        "2026-06-01",
    ]


def test_monthly_schedule_year_rollover():
    """Monthly schedule steps across the year boundary correctly."""
    from dbt_dagsterizer.schedules.dbt.factory import build_dbt_schedules
    from dbt_dagsterizer.schedules.dbt.presets import monthly_at

    spec = monthly_at(
        name="monthly_rollover", job_name="stub_job", hour=0, minute=0, offset_months=1,
        lookback_months=2,
    )

    schedules = build_dbt_schedules([spec], {"stub_job": STUB_JOB})

    run_requests = _evaluate_schedule(
        schedules[0], datetime(2026, 1, 15, 0, 0, 0, tzinfo=timezone.utc)
    )
    assert [rr.partition_key for rr in run_requests] == [
        "2025-12-01",
        "2025-11-01",
        "2025-10-01",
    ]


def test_monthly_schedule_zero_offset():
    """Monthly schedule with offset_months=0 anchors at the current month."""
    from dbt_dagsterizer.schedules.dbt.factory import build_dbt_schedules
    from dbt_dagsterizer.schedules.dbt.presets import monthly_at

    spec = monthly_at(name="monthly_zero", job_name="stub_job", hour=0, minute=0, offset_months=0)

    schedules = build_dbt_schedules([spec], {"stub_job": STUB_JOB})

    run_requests = _evaluate_schedule(
        schedules[0], datetime(2026, 9, 15, 0, 0, 0, tzinfo=timezone.utc)
    )
    assert run_requests[0].partition_key == "2026-09-01"


def test_monthly_schedule_run_key_with_dedupe():
    """Monthly schedule with dedupe_across_ticks=True uses plain run key."""
    from dbt_dagsterizer.schedules.dbt.factory import build_dbt_schedules
    from dbt_dagsterizer.schedules.dbt.presets import monthly_at

    spec = monthly_at(
        name="monthly_dedup", job_name="stub_job", hour=0, minute=0, offset_months=1,
        dedupe_across_ticks=True,
    )

    schedules = build_dbt_schedules([spec], {"stub_job": STUB_JOB})

    run_requests = _evaluate_schedule(
        schedules[0], datetime(2026, 9, 1, 0, 0, 0, tzinfo=timezone.utc)
    )
    assert run_requests[0].run_key == "monthly_dedup:2026-08-01"


def test_monthly_schedule_run_key_without_dedupe():
    """Monthly schedule with dedupe_across_ticks=False appends tick timestamp."""
    from dbt_dagsterizer.schedules.dbt.factory import build_dbt_schedules
    from dbt_dagsterizer.schedules.dbt.presets import monthly_at

    spec = monthly_at(
        name="monthly_nodedup", job_name="stub_job", hour=2, minute=30, offset_months=1,
        dedupe_across_ticks=False,
    )

    schedules = build_dbt_schedules([spec], {"stub_job": STUB_JOB})

    run_requests = _evaluate_schedule(
        schedules[0], datetime(2026, 9, 1, 2, 30, 0, tzinfo=timezone.utc)
    )
    assert run_requests[0].run_key.startswith("monthly_nodedup:2026-08-01:")
    assert "20260901T023000" in run_requests[0].run_key


def test_monthly_schedule_cannot_set_daily_offset_fields():
    """Monthly schedule raises if partition_offset_days or partition_lookback_days are non-zero."""
    from dbt_dagsterizer.schedules.dbt.factory import build_dbt_schedules

    spec = {
        "name": "bad_monthly",
        "cron_schedule": "0 0 1 * *",
        "job_name": "stub_job",
        "partition_type": "monthly",
        "partition_offset_days": 1,  # Non-zero daily offset → error
        "partition_lookback_days": 0,
        "partition_offset_hours": 0,
        "partition_lookback_hours": 0,
        "partition_offset_months": 1,
        "partition_lookback_months": 0,
        "enabled": True,
        "dedupe_across_ticks": True,
        "timezone": "UTC",
    }

    with pytest.raises(ValueError, match="cannot set daily offset/lookback"):
        build_dbt_schedules([spec], {"stub_job": STUB_JOB})


def test_monthly_schedule_cannot_set_hourly_offset_fields():
    """Monthly schedule raises if partition_offset_hours or partition_lookback_hours are non-zero."""
    from dbt_dagsterizer.schedules.dbt.factory import build_dbt_schedules

    spec = {
        "name": "bad_monthly_hours",
        "cron_schedule": "0 0 1 * *",
        "job_name": "stub_job",
        "partition_type": "monthly",
        "partition_offset_days": 0,
        "partition_lookback_days": 0,
        "partition_offset_hours": 2,  # Non-zero hourly offset → error
        "partition_lookback_hours": 0,
        "partition_offset_months": 1,
        "partition_lookback_months": 0,
        "enabled": True,
        "dedupe_across_ticks": True,
        "timezone": "UTC",
    }

    with pytest.raises(ValueError, match="cannot set hourly offset/lookback"):
        build_dbt_schedules([spec], {"stub_job": STUB_JOB})


def test_daily_schedule_cannot_set_monthly_offset_fields():
    """Daily schedule raises if partition_offset_months or partition_lookback_months are non-zero."""
    from dbt_dagsterizer.schedules.dbt.factory import build_dbt_schedules

    spec = {
        "name": "bad_daily_months",
        "cron_schedule": "0 0 * * *",
        "job_name": "stub_job",
        "partition_type": "daily",
        "partition_offset_days": 1,
        "partition_lookback_days": 0,
        "partition_offset_hours": 0,
        "partition_lookback_hours": 0,
        "partition_offset_months": 1,  # Non-zero monthly offset → error
        "partition_lookback_months": 0,
        "enabled": True,
        "dedupe_across_ticks": True,
        "timezone": "UTC",
    }

    with pytest.raises(ValueError, match="cannot set monthly offset/lookback"):
        build_dbt_schedules([spec], {"stub_job": STUB_JOB})


def test_hourly_schedule_cannot_set_monthly_offset_fields():
    """Hourly schedule raises if partition_offset_months or partition_lookback_months are non-zero."""
    from dbt_dagsterizer.schedules.dbt.factory import build_dbt_schedules

    spec = {
        "name": "bad_hourly_months",
        "cron_schedule": "0 * * * *",
        "job_name": "stub_job",
        "partition_type": "hourly",
        "partition_offset_days": 0,
        "partition_lookback_days": 0,
        "partition_offset_hours": 1,
        "partition_lookback_hours": 0,
        "partition_offset_months": 1,  # Non-zero monthly offset → error
        "partition_lookback_months": 0,
        "enabled": True,
        "dedupe_across_ticks": True,
        "timezone": "UTC",
    }

    with pytest.raises(ValueError, match="cannot set monthly offset/lookback"):
        build_dbt_schedules([spec], {"stub_job": STUB_JOB})


def test_unsupported_partition_type_still_raises():
    """build_dbt_schedules raises ValueError for unsupported partition_type."""
    from dbt_dagsterizer.schedules.dbt.factory import build_dbt_schedules

    spec = {
        "name": "bad_type",
        "cron_schedule": "0 0 * * 0",
        "job_name": "stub_job",
        "partition_type": "weekly",
        "partition_offset_days": 0,
        "partition_lookback_days": 0,
        "partition_offset_hours": 0,
        "partition_lookback_hours": 0,
        "partition_offset_months": 0,
        "partition_lookback_months": 0,
        "enabled": True,
        "dedupe_across_ticks": True,
        "timezone": "UTC",
    }

    with pytest.raises(ValueError, match="Unsupported partition_type"):
        build_dbt_schedules([spec], {"stub_job": STUB_JOB})


def test_monthly_schedule_cron_and_timezone():
    """Monthly schedule carries the preset's cron and execution timezone."""
    from dbt_dagsterizer.schedules.dbt.factory import build_dbt_schedules
    from dbt_dagsterizer.schedules.dbt.presets import monthly_at

    spec = monthly_at(
        name="monthly_tz",
        job_name="stub_job",
        hour=2,
        minute=30,
        day_of_month=15,
        timezone="Asia/Shanghai",
    )

    schedules = build_dbt_schedules([spec], {"stub_job": STUB_JOB})
    assert schedules[0].cron_schedule == "30 2 15 * *"
    assert schedules[0].execution_timezone == "Asia/Shanghai"


def test_monthly_schedule_disabled():
    """Disabled monthly schedule builds without error."""
    from dbt_dagsterizer.schedules.dbt.factory import build_dbt_schedules
    from dbt_dagsterizer.schedules.dbt.presets import monthly_at

    spec = monthly_at(name="stopped_monthly", job_name="stub_job", hour=0, minute=0, enabled=False)

    schedules = build_dbt_schedules([spec], {"stub_job": STUB_JOB})
    assert len(schedules) == 1
    assert schedules[0] is not None


def test_mixed_daily_hourly_and_monthly_schedules():
    """build_dbt_schedules can handle daily, hourly and monthly specs together."""
    from dbt_dagsterizer.schedules.dbt.factory import build_dbt_schedules
    from dbt_dagsterizer.schedules.dbt.presets import daily_at, hourly_at, monthly_at

    daily_spec = daily_at(name="daily_sched", job_name="stub_job", hour=3, minute=0)
    hourly_spec = hourly_at(name="hourly_sched", job_name="stub_job", minute=0)
    monthly_spec = monthly_at(name="monthly_sched", job_name="stub_job", hour=0, minute=0)

    schedules = build_dbt_schedules(
        [daily_spec, hourly_spec, monthly_spec], {"stub_job": STUB_JOB}
    )
    assert len(schedules) == 3

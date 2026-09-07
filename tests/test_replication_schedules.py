"""Tests for replication schedules."""
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import dagster as dg
import pytest
import yaml

from dbt_dagsterizer.schedules.replication.auto_config import build_auto_replication_schedule_specs

_MANIFEST = {
    "nodes": {
        "model.demo.orders": {
            "resource_type": "model",
            "unique_id": "model.demo.orders",
            "name": "orders",
            "database": "production_db",
            "schema": "dws",
            "identifier": "orders",
        },
    }
}


@dataclass
class _FakeScheduleContext:
    """Minimal context that satisfies the schedule evaluation function."""

    scheduled_execution_time: datetime


def _evaluate_schedule(schedule_def, scheduled_time: datetime):
    """Call the schedule's original function with a fake context."""
    fn = schedule_def._execution_fn.decorated_fn
    result = fn(_FakeScheduleContext(scheduled_execution_time=scheduled_time))
    return list(result) if result else []


def test_schedules_empty_when_replication_disabled(tmp_path: Path):
    """Test that no schedule specs are generated when replication is disabled."""
    orch_path = tmp_path / "dagsterization.yml"
    orch_path.write_text(
        yaml.dump({"version": 1, "replication": {"enabled": False, "entries": []}})
    )

    with patch("dbt_dagsterizer.schedules.replication.auto_config.get_dbt_project_dir", return_value=tmp_path):
        specs = build_auto_replication_schedule_specs()
        assert specs == []


def test_schedules_empty_by_default(tmp_path: Path):
    """Test that schedules are disabled by default (replication uses asset deps)."""
    orch_path = tmp_path / "dagsterization.yml"
    orch_path.write_text(
        yaml.dump({
            "version": 1,
            "replication": {
                "enabled": True,
                "entries": [
                    {
                        "model": "orders",
                        "enabled": True,
                        "destination_table": "orders",
                        "destination_schema": "dbo",
                        "write_disposition": "replace",
                    }
                ],
            },
        })
    )

    with patch("dbt_dagsterizer.schedules.replication.auto_config.get_dbt_project_dir", return_value=tmp_path):
        specs = build_auto_replication_schedule_specs()
        # Schedules should be empty by default - replication uses asset dependencies
        assert specs == []


def test_schedules_created_when_explicitly_enabled(tmp_path: Path):
    """Test that schedule specs are generated only when explicitly enabled."""
    orch_path = tmp_path / "dagsterization.yml"
    orch_path.write_text(
        yaml.dump({
            "version": 1,
            "replication": {
                "enabled": True,
                "schedules": {
                    "enabled": True,  # Explicitly enable schedules
                },
                "entries": [
                    {
                        "model": "orders",
                        "enabled": True,
                        "destination_table": "orders",
                        "destination_schema": "dbo",
                        "write_disposition": "replace",
                    }
                ],
            },
        })
    )

    with patch("dbt_dagsterizer.schedules.replication.auto_config.get_dbt_project_dir", return_value=tmp_path):
        specs = build_auto_replication_schedule_specs()
        assert len(specs) == 1
        spec = specs[0]
        assert spec["name"] == "replicate_orders_schedule"
        assert spec["job_name"] == "replicate_orders_job"  # Jobs have _job suffix
        assert spec["cron_schedule"] == "30 0 * * *"
        assert spec["partition_type"] == "unpartitioned"
        assert spec["enabled"] is True


def test_schedules_skips_disabled_entries(tmp_path: Path):
    """Test that disabled replication entries don't get schedules."""
    orch_path = tmp_path / "dagsterization.yml"
    orch_path.write_text(
        yaml.dump({
            "version": 1,
            "replication": {
                "enabled": True,
                "entries": [
                    {
                        "model": "orders",
                        "enabled": False,
                        "destination_table": "orders",
                        "destination_schema": "dbo",
                        "write_disposition": "replace",
                    }
                ],
            },
        })
    )

    with patch("dbt_dagsterizer.schedules.replication.auto_config.get_dbt_project_dir", return_value=tmp_path):
        specs = build_auto_replication_schedule_specs()
        assert specs == []


def _write_replication_config(tmp_path: Path, partition_type: str) -> Path:
    """Write a dagsterization.yml with one enabled, schedule-enabled replication entry."""
    orch_path = tmp_path / "dagsterization.yml"
    orch_path.write_text(
        yaml.dump({
            "version": 1,
            "partitions": {partition_type: ["orders"]},
            "replication": {
                "enabled": True,
                "schedules": {"enabled": True},
                "entries": [
                    {
                        "model": "orders",
                        "enabled": True,
                        "destination_table": "orders",
                        "destination_schema": "dbo",
                        "write_disposition": "replace",
                    }
                ],
            },
        })
    )
    return orch_path


def test_monthly_model_gets_monthly_cron(tmp_path: Path):
    """A monthly-partitioned model gets a month-scoped cron (00:30 on the 1st)."""
    _write_replication_config(tmp_path, "monthly")

    with patch("dbt_dagsterizer.schedules.replication.auto_config.get_dbt_project_dir", return_value=tmp_path):
        specs = build_auto_replication_schedule_specs()

    assert len(specs) == 1
    assert specs[0]["partition_type"] == "monthly"
    assert specs[0]["cron_schedule"] == "30 0 1 * *"


def test_daily_model_keeps_daily_cron(tmp_path: Path):
    """Regression guard: daily models keep the existing 00:30 daily cron."""
    _write_replication_config(tmp_path, "daily")

    with patch("dbt_dagsterizer.schedules.replication.auto_config.get_dbt_project_dir", return_value=tmp_path):
        specs = build_auto_replication_schedule_specs()

    assert len(specs) == 1
    assert specs[0]["partition_type"] == "daily"
    assert specs[0]["cron_schedule"] == "30 0 * * *"


def test_monthly_replication_schedule_emits_month_partition_keys():
    """The monthly replication schedule emits 'YYYY-MM-01' keys valid for MonthlyPartitionsDefinition."""
    from dbt_dagsterizer.schedules.replication.factory import build_replication_schedules

    partitions_def = dg.MonthlyPartitionsDefinition(start_date="2025-01-01", end_offset=1)

    @dg.job(name="replicate_orders_job", partitions_def=partitions_def)
    def _monthly_replication_job():
        pass

    spec = {
        "name": "replicate_orders_schedule",
        "job_name": "replicate_orders_job",
        "cron_schedule": "30 0 1 * *",
        "partition_type": "monthly",
        "partition_offset_months": 1,
        "partition_lookback_months": 2,
        "enabled": True,
    }

    with patch(
        "dbt_dagsterizer.jobs.replication.get_replication_jobs_by_name",
        return_value={"replicate_orders_job": _monthly_replication_job},
    ):
        schedules = build_replication_schedules([spec])

    assert len(schedules) == 1
    run_requests = _evaluate_schedule(
        schedules[0], datetime(2026, 9, 1, 0, 30, tzinfo=timezone.utc)
    )
    assert [rr.partition_key for rr in run_requests] == [
        "2026-08-01",
        "2026-07-01",
        "2026-06-01",
    ]
    valid_keys = set(partitions_def.get_partition_keys())
    assert all(rr.partition_key in valid_keys for rr in run_requests)


def test_unsupported_replication_partition_type_still_raises():
    """Unknown partition types are still rejected rather than silently skipped."""
    from dbt_dagsterizer.schedules.replication.factory import build_replication_schedules

    spec = {
        "name": "replicate_orders_schedule",
        "job_name": "replicate_orders_job",
        "cron_schedule": "30 0 * * *",
        "partition_type": "weekly",
        "enabled": True,
    }

    with patch(
        "dbt_dagsterizer.jobs.replication.get_replication_jobs_by_name",
        return_value={"replicate_orders_job": None},
    ):
        with pytest.raises(ValueError, match="Unsupported partition_type: weekly"):
            build_replication_schedules([spec])

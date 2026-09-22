"""Tests for arbitrary cron schedules: preset, factory, config, validation, auto_config."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import dagster as dg
import pytest
from ruamel.yaml import YAML


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
    if result is None:
        return []
    if isinstance(result, dg.RunRequest):
        return [result]
    return list(result)


# ---------------------------------------------------------------------------
# cron preset tests
# ---------------------------------------------------------------------------


def test_cron_preset_daily_defaults():
    from dbt_dagsterizer.schedules.dbt.presets import cron

    spec = cron(name="my_cron", job_name="my_job", cron_expression="*/15 * * * *")
    assert spec["partition_type"] == "daily"
    assert spec["cron_schedule"] == "*/15 * * * *"
    assert spec["partition_offset_days"] == 1
    assert spec["partition_lookback_days"] == 0
    assert spec["partition_offset_hours"] == 0
    assert spec["partition_offset_months"] == 0


def test_cron_preset_hourly_defaults():
    from dbt_dagsterizer.schedules.dbt.presets import cron

    spec = cron(
        name="my_cron", job_name="my_job", cron_expression="*/5 * * * *", partition_type="hourly"
    )
    assert spec["partition_type"] == "hourly"
    assert spec["partition_offset_hours"] == 1
    assert spec["partition_lookback_hours"] == 0
    assert spec["partition_offset_days"] == 0


def test_cron_preset_monthly_defaults():
    from dbt_dagsterizer.schedules.dbt.presets import cron

    spec = cron(
        name="my_cron", job_name="my_job", cron_expression="0 0 15 * *", partition_type="monthly"
    )
    assert spec["partition_type"] == "monthly"
    assert spec["partition_offset_months"] == 1
    assert spec["partition_lookback_months"] == 0


def test_cron_preset_unpartitioned_zeroes_all_windows():
    from dbt_dagsterizer.schedules.dbt.presets import cron

    spec = cron(
        name="my_cron", job_name="my_job", cron_expression="@daily", partition_type="unpartitioned"
    )
    assert spec["partition_type"] == "unpartitioned"
    assert spec["partition_offset_days"] == 0
    assert spec["partition_lookback_days"] == 0
    assert spec["partition_offset_hours"] == 0
    assert spec["partition_lookback_hours"] == 0
    assert spec["partition_offset_months"] == 0
    assert spec["partition_lookback_months"] == 0


def test_cron_preset_respects_explicit_offsets():
    from dbt_dagsterizer.schedules.dbt.presets import cron

    spec = cron(
        name="my_cron",
        job_name="my_job",
        cron_expression="0 9 * * 1-5",
        partition_type="daily",
        offset_days=3,
        lookback_days=2,
    )
    assert spec["partition_offset_days"] == 3
    assert spec["partition_lookback_days"] == 2


def test_cron_preset_normalizes_expression():
    from dbt_dagsterizer.schedules.dbt.presets import cron

    spec = cron(name="my_cron", job_name="my_job", cron_expression="  0  9 * * 1-5 ")
    assert spec["cron_schedule"] == "0 9 * * 1-5"


def test_cron_preset_custom_timezone():
    from dbt_dagsterizer.schedules.dbt.presets import cron

    spec = cron(
        name="my_cron", job_name="my_job", cron_expression="0 0 * * *", timezone="Asia/Shanghai"
    )
    assert spec["timezone"] == "Asia/Shanghai"


def test_cron_preset_returns_correct_fields():
    from dbt_dagsterizer.schedules.dbt.presets import cron

    spec = cron(
        name="my_cron",
        job_name="events_job",
        cron_expression="30 2 15 * *",
        partition_type="monthly",
        lookback_months=2,
    )
    assert spec == {
        "name": "my_cron",
        "cron_schedule": "30 2 15 * *",
        "job_name": "events_job",
        "partition_type": "monthly",
        "partition_offset_days": 0,
        "partition_lookback_days": 0,
        "partition_offset_hours": 0,
        "partition_lookback_hours": 0,
        "partition_offset_months": 1,
        "partition_lookback_months": 2,
        "enabled": True,
        "dedupe_across_ticks": True,
        "timezone": "UTC",
    }


def test_cron_preset_empty_name_raises():
    from dbt_dagsterizer.schedules.dbt.presets import cron

    with pytest.raises(ValueError, match="Schedule name must be non-empty"):
        cron(name="", job_name="my_job", cron_expression="0 0 * * *")


def test_cron_preset_empty_job_name_raises():
    from dbt_dagsterizer.schedules.dbt.presets import cron

    with pytest.raises(ValueError, match="job_name must be non-empty"):
        cron(name="my_cron", job_name="", cron_expression="0 0 * * *")


def test_cron_preset_rejects_unknown_partition_type():
    from dbt_dagsterizer.schedules.dbt.presets import cron

    with pytest.raises(ValueError, match="partition_type must be one of"):
        cron(name="my_cron", job_name="my_job", cron_expression="0 0 * * *", partition_type="weekly")


def test_cron_preset_rejects_invalid_expression():
    from dbt_dagsterizer.schedules.dbt.presets import cron

    with pytest.raises(ValueError, match="minute field"):
        cron(name="my_cron", job_name="my_job", cron_expression="60 * * * *")


def test_cron_preset_rejects_negative_offsets():
    from dbt_dagsterizer.schedules.dbt.presets import cron

    with pytest.raises(ValueError, match="offset_days must be >= 0"):
        cron(
            name="my_cron",
            job_name="my_job",
            cron_expression="0 0 * * *",
            partition_type="daily",
            offset_days=-1,
        )


def test_cron_preset_rejects_foreign_granularity_fields():
    from dbt_dagsterizer.schedules.dbt.presets import cron

    with pytest.raises(ValueError, match="cannot set hourly offset/lookback fields"):
        cron(
            name="my_cron",
            job_name="my_job",
            cron_expression="0 0 * * *",
            partition_type="daily",
            offset_hours=1,
        )
    with pytest.raises(ValueError, match="cannot set daily offset/lookback fields"):
        cron(
            name="my_cron",
            job_name="my_job",
            cron_expression="0 0 * * *",
            partition_type="hourly",
            lookback_days=1,
        )
    with pytest.raises(ValueError, match="cannot set monthly offset/lookback fields"):
        cron(
            name="my_cron",
            job_name="my_job",
            cron_expression="0 0 * * *",
            partition_type="unpartitioned",
            offset_months=1,
        )


# ---------------------------------------------------------------------------
# cron factory tests
# ---------------------------------------------------------------------------


def test_cron_daily_partition_key_from_arbitrary_expression():
    """A */15 cron still targets the same daily partition as daily_at would."""
    from dbt_dagsterizer.schedules.dbt.factory import build_dbt_schedules
    from dbt_dagsterizer.schedules.dbt.presets import cron

    spec = cron(name="cron_daily", job_name="stub_job", cron_expression="*/15 * * * *")
    schedules = build_dbt_schedules([spec], {"stub_job": STUB_JOB})
    assert len(schedules) == 1

    run_requests = _evaluate_schedule(
        schedules[0], datetime(2025, 6, 15, 10, 30, 0, tzinfo=timezone.utc)
    )
    assert len(run_requests) == 1
    assert run_requests[0].partition_key == "2025-06-14"
    assert run_requests[0].run_key == "cron_daily:2025-06-14"


def test_cron_daily_with_lookback():
    from dbt_dagsterizer.schedules.dbt.factory import build_dbt_schedules
    from dbt_dagsterizer.schedules.dbt.presets import cron

    spec = cron(
        name="cron_daily_lb",
        job_name="stub_job",
        cron_expression="0 3 * * *",
        lookback_days=2,
    )
    schedules = build_dbt_schedules([spec], {"stub_job": STUB_JOB})

    run_requests = _evaluate_schedule(
        schedules[0], datetime(2025, 6, 15, 3, 0, 0, tzinfo=timezone.utc)
    )
    assert [rr.partition_key for rr in run_requests] == [
        "2025-06-14",
        "2025-06-13",
        "2025-06-12",
    ]


def test_cron_hourly_partition_key():
    from dbt_dagsterizer.schedules.dbt.factory import build_dbt_schedules
    from dbt_dagsterizer.schedules.dbt.presets import cron

    spec = cron(
        name="cron_hourly",
        job_name="stub_job",
        cron_expression="*/5 * * * *",
        partition_type="hourly",
    )
    schedules = build_dbt_schedules([spec], {"stub_job": STUB_JOB})

    run_requests = _evaluate_schedule(
        schedules[0], datetime(2025, 6, 15, 10, 32, 0, tzinfo=timezone.utc)
    )
    assert run_requests[0].partition_key == "2025-06-15-09:00"


def test_cron_monthly_partition_key():
    from dbt_dagsterizer.schedules.dbt.factory import build_dbt_schedules
    from dbt_dagsterizer.schedules.dbt.presets import cron

    spec = cron(
        name="cron_monthly",
        job_name="stub_job",
        cron_expression="0 0 15 * *",
        partition_type="monthly",
    )
    schedules = build_dbt_schedules([spec], {"stub_job": STUB_JOB})

    run_requests = _evaluate_schedule(
        schedules[0], datetime(2025, 6, 15, 0, 0, 0, tzinfo=timezone.utc)
    )
    assert run_requests[0].partition_key == "2025-05-01"


def test_cron_unpartitioned_returns_plain_run_request():
    """Unpartitioned jobs have no partition keys and no run key to dedupe on."""
    from dbt_dagsterizer.schedules.dbt.factory import build_dbt_schedules
    from dbt_dagsterizer.schedules.dbt.presets import cron

    spec = cron(
        name="cron_unpart",
        job_name="stub_job",
        cron_expression="@daily",
        partition_type="unpartitioned",
    )
    schedules = build_dbt_schedules([spec], {"stub_job": STUB_JOB})

    run_requests = _evaluate_schedule(
        schedules[0], datetime(2025, 6, 15, 0, 0, 0, tzinfo=timezone.utc)
    )
    assert len(run_requests) == 1
    assert run_requests[0].partition_key is None
    assert run_requests[0].run_key is None


def test_cron_unpartitioned_rejects_offset_fields():
    from dbt_dagsterizer.schedules.dbt.factory import build_dbt_schedules

    spec = {
        "name": "bad_unpart",
        "cron_schedule": "@daily",
        "job_name": "stub_job",
        "partition_type": "unpartitioned",
        "partition_offset_days": 1,
        "partition_lookback_days": 0,
        "partition_offset_hours": 0,
        "partition_lookback_hours": 0,
        "partition_offset_months": 0,
        "partition_lookback_months": 0,
        "enabled": True,
        "dedupe_across_ticks": True,
        "timezone": "UTC",
    }

    with pytest.raises(ValueError, match="cannot set offset/lookback fields"):
        build_dbt_schedules([spec], {"stub_job": STUB_JOB})


def test_cron_factory_run_key_without_dedupe():
    from dbt_dagsterizer.schedules.dbt.factory import build_dbt_schedules
    from dbt_dagsterizer.schedules.dbt.presets import cron

    spec = cron(
        name="cron_nodedup",
        job_name="stub_job",
        cron_expression="0 0 * * *",
        dedupe_across_ticks=False,
    )
    schedules = build_dbt_schedules([spec], {"stub_job": STUB_JOB})

    run_requests = _evaluate_schedule(
        schedules[0], datetime(2025, 6, 15, 10, 0, 0, tzinfo=timezone.utc)
    )
    assert run_requests[0].run_key.startswith("cron_nodedup:2025-06-14:")
    assert "20250615T100000" in run_requests[0].run_key


def test_cron_factory_sets_execution_timezone():
    from dbt_dagsterizer.schedules.dbt.factory import build_dbt_schedules
    from dbt_dagsterizer.schedules.dbt.presets import cron

    spec = cron(
        name="cron_tz",
        job_name="stub_job",
        cron_expression="0 9 * * *",
        timezone="Asia/Shanghai",
    )
    schedules = build_dbt_schedules([spec], {"stub_job": STUB_JOB})
    assert schedules[0].execution_timezone == "Asia/Shanghai"


def test_cron_factory_disabled_schedule():
    from dbt_dagsterizer.schedules.dbt.factory import build_dbt_schedules
    from dbt_dagsterizer.schedules.dbt.presets import cron

    spec = cron(name="cron_off", job_name="stub_job", cron_expression="0 0 * * *", enabled=False)
    schedules = build_dbt_schedules([spec], {"stub_job": STUB_JOB})
    assert schedules[0].default_status == dg.DefaultScheduleStatus.STOPPED


# ---------------------------------------------------------------------------
# set_schedule cron tests
# ---------------------------------------------------------------------------


def test_set_schedule_cron_daily_writes_window():
    from dbt_dagsterizer.orchestration_config import set_schedule

    data = {"version": 1, "schedules": {}}
    set_schedule(
        data=data,
        name="cron_daily",
        job_name="orders_job",
        schedule_type="cron",
        cron_expression="*/15 * * * *",
        lookback_days=1,
        offset_days=2,
        enabled=True,
    )
    entry = data["schedules"]["cron_daily"]
    assert entry["type"] == "cron"
    assert entry["cron_expression"] == "*/15 * * * *"
    assert entry["partition_type"] == "daily"
    assert entry["lookback_days"] == 1
    assert entry["offset_days"] == 2
    # cron schedules carry no tick time
    assert "hour" not in entry
    assert "minute" not in entry
    assert "lookback_hours" not in entry


def test_set_schedule_cron_normalizes_expression():
    from dbt_dagsterizer.orchestration_config import set_schedule

    data = {"version": 1, "schedules": {}}
    set_schedule(
        data=data,
        name="cron_norm",
        job_name="orders_job",
        schedule_type="cron",
        cron_expression="  0  9  *  *  1-5  ",
        enabled=True,
    )
    assert data["schedules"]["cron_norm"]["cron_expression"] == "0 9 * * 1-5"


def test_set_schedule_cron_hourly_partition_type():
    from dbt_dagsterizer.orchestration_config import set_schedule

    data = {"version": 1, "schedules": {}}
    set_schedule(
        data=data,
        name="cron_hourly",
        job_name="orders_job",
        schedule_type="cron",
        cron_expression="*/5 * * * *",
        partition_type="hourly",
        lookback_hours=2,
        offset_hours=1,
        enabled=True,
    )
    entry = data["schedules"]["cron_hourly"]
    assert entry["partition_type"] == "hourly"
    assert entry["lookback_hours"] == 2
    assert entry["offset_hours"] == 1
    assert "lookback_days" not in entry
    assert "offset_days" not in entry


def test_set_schedule_cron_monthly_partition_type():
    from dbt_dagsterizer.orchestration_config import set_schedule

    data = {"version": 1, "schedules": {}}
    set_schedule(
        data=data,
        name="cron_monthly",
        job_name="orders_job",
        schedule_type="cron",
        cron_expression="0 0 15 * *",
        partition_type="monthly",
        lookback_months=2,
        offset_months=1,
        enabled=True,
    )
    entry = data["schedules"]["cron_monthly"]
    assert entry["partition_type"] == "monthly"
    assert entry["lookback_months"] == 2
    assert entry["offset_months"] == 1
    assert "lookback_days" not in entry


def test_set_schedule_cron_unpartitioned_has_no_window():
    from dbt_dagsterizer.orchestration_config import set_schedule

    data = {"version": 1, "schedules": {}}
    set_schedule(
        data=data,
        name="cron_unpart",
        job_name="orders_job",
        schedule_type="cron",
        cron_expression="@daily",
        partition_type="unpartitioned",
        enabled=True,
    )
    entry = data["schedules"]["cron_unpart"]
    assert entry["partition_type"] == "unpartitioned"
    assert "lookback_days" not in entry
    assert "offset_days" not in entry
    assert "lookback_hours" not in entry
    assert "lookback_months" not in entry


def test_set_schedule_cron_requires_expression():
    from dbt_dagsterizer.orchestration_config import set_schedule

    data = {"version": 1, "schedules": {}}
    with pytest.raises(ValueError, match="requires a non-empty cron_expression"):
        set_schedule(
            data=data,
            name="cron_empty",
            job_name="orders_job",
            schedule_type="cron",
            enabled=True,
        )


def test_set_schedule_cron_rejects_invalid_expression():
    from dbt_dagsterizer.orchestration_config import set_schedule

    data = {"version": 1, "schedules": {}}
    with pytest.raises(ValueError, match="minute field"):
        set_schedule(
            data=data,
            name="cron_bad",
            job_name="orders_job",
            schedule_type="cron",
            cron_expression="60 * * * *",
            enabled=True,
        )


def test_set_schedule_cron_rejects_invalid_partition_type():
    from dbt_dagsterizer.orchestration_config import set_schedule

    data = {"version": 1, "schedules": {}}
    with pytest.raises(ValueError, match="partition_type must be one of"):
        set_schedule(
            data=data,
            name="cron_bad",
            job_name="orders_job",
            schedule_type="cron",
            cron_expression="0 0 * * *",
            partition_type="weekly",
            enabled=True,
        )


# ---------------------------------------------------------------------------
# cron validation tests
# ---------------------------------------------------------------------------

_MANIFEST = {
    "nodes": {
        "model.demo.fact_revenue": {"resource_type": "model", "name": "fact_revenue"},
    },
    "sources": {},
}


def _cron_orchestration(**entry_overrides):
    entry = {
        "type": "cron",
        "job_name": "revenue_job",
        "cron_expression": "*/15 * * * *",
        "partition_type": "daily",
        "lookback_days": 0,
        "offset_days": 1,
    }
    entry.update(entry_overrides)
    return {
        "version": 1,
        "timezone": "UTC",
        "partitions": {"daily": ["fact_revenue"]},
        "jobs": {"revenue_job": {"models": ["fact_revenue"], "partitions": "daily"}},
        "asset_jobs": [],
        "schedules": {"revenue_cron": entry},
        "partition_change": {"detectors": [], "propagators": []},
    }


def _deep_issues(orchestration):
    from dbt_dagsterizer.cli_parts.validation import validate_orchestration

    return validate_orchestration(
        manifest=_MANIFEST,
        orchestration=orchestration,
        require_file_exists=False,
        orchestration_path=Path("/tmp/dagsterization.yml"),
    )


def _deep_errors(orchestration):
    return [i.message for i in _deep_issues(orchestration) if i.level == "error"]


def test_validation_cron_schedule_accepted_end_to_end():
    assert _deep_errors(_cron_orchestration()) == []


def test_validation_cron_rejects_missing_expression():
    errors = _deep_errors(_cron_orchestration(cron_expression=""))
    assert any("cron_expression must be non-empty (required for cron)" in e for e in errors)


def test_validation_cron_rejects_invalid_expression():
    errors = _deep_errors(_cron_orchestration(cron_expression="60 * * * *"))
    assert any("cron_expression is invalid" in e for e in errors)


def test_validation_cron_warns_on_oversized_step():
    issues = _deep_issues(_cron_orchestration(cron_expression="*/90 * * * *"))
    assert [i.message for i in issues if i.level == "error"] == []
    warnings = [i.message for i in issues if i.level == "warn"]
    assert any("step of 90" in w for w in warnings)


def test_validation_cron_rejects_unknown_partition_type():
    errors = _deep_errors(_cron_orchestration(partition_type="weekly"))
    assert any("partition_type must be daily|hourly|monthly|unpartitioned" in e for e in errors)


def test_validation_cron_rejects_foreign_offset_fields():
    errors = _deep_errors(_cron_orchestration(offset_hours=2))
    assert any("cannot set offset_hours" in e for e in errors)


def test_validation_cron_unpartitioned_rejects_window_fields():
    errors = _deep_errors(_cron_orchestration(partition_type="unpartitioned", offset_days=1))
    assert any("cannot set offset_days" in e for e in errors)


def test_validation_cron_rejects_negative_offsets():
    errors = _deep_errors(_cron_orchestration(offset_days=-1))
    assert any("offset_days must be >= 0" in e for e in errors)


def test_validation_structure_accepts_cron_schedule():
    from dbt_dagsterizer.cli_parts.validation import validate_orchestration_structure

    issues = validate_orchestration_structure(orchestration=_cron_orchestration())
    assert [i.message for i in issues if i.level == "error"] == []


def test_validation_structure_rejects_cron_without_expression():
    from dbt_dagsterizer.cli_parts.validation import validate_orchestration_structure

    issues = validate_orchestration_structure(
        orchestration=_cron_orchestration(cron_expression=None)
    )
    errors = [i.message for i in issues if i.level == "error"]
    assert any("cron_expression must be non-empty (required for cron)" in e for e in errors)


def test_validation_structure_rejects_invalid_cron_expression():
    from dbt_dagsterizer.cli_parts.validation import validate_orchestration_structure

    issues = validate_orchestration_structure(
        orchestration=_cron_orchestration(cron_expression="60 * * * *")
    )
    errors = [i.message for i in issues if i.level == "error"]
    assert any("cron_expression is invalid" in e for e in errors)


# ---------------------------------------------------------------------------
# auto_config cron tests
# ---------------------------------------------------------------------------


def _cron_schedule_specs(tmp_path: Path, monkeypatch, cron_entry: dict, timezone: str | None = None):
    """Build cron specs from a scratch dagsterization.yml."""
    from dbt_dagsterizer.schedules.dbt import auto_config

    cfg = {
        "version": 1,
        "partitions": {"daily": ["fact_revenue"]},
        "jobs": {"revenue_job": {"models": ["fact_revenue"], "partitions": "daily"}},
        "schedules": {"revenue_cron": cron_entry},
    }
    if timezone is not None:
        cfg["timezone"] = timezone

    monkeypatch.setattr(auto_config, "load_manifest", lambda: {"nodes": {}})
    monkeypatch.setattr(auto_config, "iter_models", lambda _manifest: [])
    monkeypatch.setattr(auto_config, "get_dbt_project_dir", lambda: tmp_path)

    y = YAML()
    with (tmp_path / "dagsterization.yml").open("w", encoding="utf-8") as f:
        y.dump(cfg, f)

    return auto_config.build_auto_dbt_schedule_specs()


def test_auto_config_builds_cron_spec_with_defaults(tmp_path: Path, monkeypatch):
    specs = _cron_schedule_specs(
        tmp_path,
        monkeypatch,
        {
            "type": "cron",
            "job_name": "revenue_job",
            "cron_expression": "*/15 * * * *",
            "partition_type": "daily",
        },
    )
    assert len(specs) == 1
    assert specs[0]["cron_schedule"] == "*/15 * * * *"
    assert specs[0]["partition_type"] == "daily"
    assert specs[0]["partition_offset_days"] == 1
    assert specs[0]["partition_lookback_days"] == 0


def test_auto_config_cron_inherits_global_timezone(tmp_path: Path, monkeypatch):
    specs = _cron_schedule_specs(
        tmp_path,
        monkeypatch,
        {
            "type": "cron",
            "job_name": "revenue_job",
            "cron_expression": "0 9 * * *",
            "partition_type": "daily",
        },
        timezone="Asia/Shanghai",
    )
    assert specs[0]["timezone"] == "Asia/Shanghai"


def test_auto_config_cron_honours_explicit_hourly_offsets(tmp_path: Path, monkeypatch):
    specs = _cron_schedule_specs(
        tmp_path,
        monkeypatch,
        {
            "type": "cron",
            "job_name": "revenue_job",
            "cron_expression": "*/5 * * * *",
            "partition_type": "hourly",
            "lookback_hours": 2,
            "offset_hours": 1,
        },
    )
    assert specs[0]["partition_type"] == "hourly"
    assert specs[0]["partition_lookback_hours"] == 2
    assert specs[0]["partition_offset_hours"] == 1
    assert specs[0]["partition_offset_days"] == 0


def test_auto_config_cron_unpartitioned(tmp_path: Path, monkeypatch):
    specs = _cron_schedule_specs(
        tmp_path,
        monkeypatch,
        {
            "type": "cron",
            "job_name": "revenue_job",
            "cron_expression": "@daily",
            "partition_type": "unpartitioned",
        },
    )
    assert specs[0]["partition_type"] == "unpartitioned"
    assert specs[0]["partition_offset_days"] == 0
    assert specs[0]["partition_offset_hours"] == 0
    assert specs[0]["partition_offset_months"] == 0


def test_auto_config_cron_requires_expression(tmp_path: Path, monkeypatch):
    with pytest.raises(ValueError, match="requires cron_expression"):
        _cron_schedule_specs(
            tmp_path,
            monkeypatch,
            {"type": "cron", "job_name": "revenue_job"},
        )


def test_auto_config_cron_rejects_unknown_partition_type(tmp_path: Path, monkeypatch):
    with pytest.raises(ValueError, match="partition_type must be daily|hourly|monthly|unpartitioned"):
        _cron_schedule_specs(
            tmp_path,
            monkeypatch,
            {
                "type": "cron",
                "job_name": "revenue_job",
                "cron_expression": "0 0 * * *",
                "partition_type": "weekly",
            },
        )
